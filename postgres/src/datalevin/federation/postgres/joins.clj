(ns ^:no-doc datalevin.federation.postgres.joins
  "Bounded dependent joins and existence checks in one authorized snapshot."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datalevin.query.source.contract :as contract]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.federation.postgres.relational :as relational]
            [datalevin.federation.postgres.statistics :as statistics]))

(defn- vars-in [clauses] (into #{} (mapcat contract/variables) clauses))

(defn- columns [request patterns]
  (reduce (fn [types pattern]
            (reduce (fn [types [term column]]
                      (if (= :var (first term)) (assoc types (second term) (assoc column :variable (second term))) types))
                    types (map vector (:terms pattern)
                               (contract/pattern-columns (contract/pattern-source-info request pattern) pattern))))
          {} patterns))

(defn- estimate [views request groups boundary]
  (let [ordered (loop [pending groups known #{boundary} out []]
                  (if (empty? pending) out
                    (let [group (first (sort-by #(if (seq (set/intersection known (vars-in %))) 0 1) pending))]
                      (recur (remove #(identical? group %) pending) (into known (vars-in group)) (conj out group)))))
        result (reduce (fn [{:keys [known rows observed]} patterns]
                         (let [alias (:source-symbol (first patterns)) {:keys [accepted statistics]} (views alias)
                               local {:clauses patterns :bindings (:bindings request)
                                      :predicates (filterv #(and (= :predicate (:kind %))
                                                                (set/subset? (contract/variables %) (vars-in patterns)))
                                                           (:clauses request))
                                      :requires (mapv #(hash-map :variable %) known)}
                               catalog (statistics/reference-statistics accepted @statistics (vals views))
                               estimate (statistics/estimate accepted local catalog)
                               n (some-> (:rows-per-binding estimate)
                                         (* (statistics/filter-selectivity accepted local @statistics)))]
                           {:known (into known (vars-in patterns))
                            :rows (when (and rows (some? n)) (* rows n))
                            :observed (conj observed (get-in estimate [:provenance :observed-at-ms]))}))
                       {:known #{boundary} :rows 1.0 :observed []} ordered)]
    (cond-> (plan/unknown-estimate true)
      (and (some? (:rows result)) (every? some? (:observed result)))
      (assoc :rows-per-binding (:rows result) :confidence :low
             :provenance {:kind :statistics :id "postgres-joined-cardinality-v2"
                          :observed-at-ms (apply min (:observed result)) :valid-for-ms nil}))))

(defn- candidate [views request groups boundary ordinal maximum-row-bytes]
  (let [patterns (vec (mapcat identity groups)) covered (set (map :id patterns))
        variables (vars-in patterns) all-clauses (:clauses request)
        predicates (filterv #(and (= :predicate (:kind %))
                                  (set/subset? (contract/variables %) variables)) all-clauses)
        outside (vars-in (remove #(covered (:id %)) all-clauses))
        needed (set/union (:required-vars request) outside)
        types (columns request patterns) required (get types boundary)
        outputs (mapv types (sort (conj (set/intersection variables needed) boundary)))
        existence? (= #{boundary} (set (map :variable outputs)))
        ordinal-column {:variable ordinal :type :long :nullable? false}
        inputs [ordinal-column required]
        query-columns (into [ordinal-column] outputs)
        query {:mode :relational-fragment :semantics-version 1 :dependent-order? true
               :clauses (into patterns predicates) :bindings (:bindings request)
               :sources (filterv #(some #{(:symbol %)} (map :source-symbol patterns))
                                 (:datalevin.query.source/region-sources request))
               :inputs inputs :columns query-columns :with []
               :find (mapv #(hash-map :kind :variable :variable (:variable %)) query-columns)}
        accepted (into {} (map (fn [[alias view]] [alias (:accepted view)]))
                       (select-keys views (map :source-symbol patterns)))]
    ;; Compound text predicates need whole-query costing. Retain the existing
    ;; joined alternatives until their correlated fan-out can be estimated.
    (when (and required
               (not-any? (fn [predicate]
                           (or (#{:like :not-like :like-any} (:operator predicate))
                               (and (#{:< :<= :> :>=} (:operator predicate))
                                    (some #(= :string (:type (get types %))) (contract/variables predicate))))) predicates)
               (or existence? (seq predicates)
                            (some #(= :const (first (nth (:terms %) 2))) patterns)))
      (when-let [compiled (relational/compile-fragment accepted query maximum-row-bytes)]
        (merge (select-keys request [:source-symbol :region-id :scope-id])
               {:id (str "join-" (str/join "-" (sort covered)) "-" (name boundary))
                :definition-fingerprint (get-in request [:source-descriptor :definition-fingerprint])
                :kind :join :anchor (first patterns) :clauses patterns :predicates predicates
                :requires [required] :columns outputs :relational? true :join? true :ordinal? true
                :joined-request query :joined-views accepted :query compiled
                :estimate (if existence?
                            (assoc (plan/unknown-estimate true) :rows-per-binding 1.0 :confidence :high
                                   :provenance {:kind :heuristic :id "postgres-existence-upper-bound-v1"
                                                :observed-at-ms (System/currentTimeMillis) :valid-for-ms nil})
                            (estimate views request groups boundary))})))))

(defn alternatives
  "Enumerate connected pairs/triples with filters or no exported inner variables,
  and a boundary produced outside the group. Ordinary accesses remain available."
  [views request maximum-row-bytes]
  (let [patterns (filterv #(= :pattern (:kind %)) (:clauses request))
        groups (vec (sort-by #(:id (first %)) (vals (group-by #(vector (:source-symbol %) (first (:terms %))) patterns))))
        combinations (concat (for [i (range (count groups)) j (range (inc i) (count groups))] [(groups i) (groups j)])
                             (for [i (range (count groups)) j (range (inc i) (count groups))
                                   k (range (inc j) (count groups))] [(groups i) (groups j) (groups k)]))
        all-vars (vars-in (:clauses request))
        ordinal (first (remove all-vars (map #(symbol (str "?dtlv_join_ordinal_" %)) (range))))
        maximum (:max-alternatives request) started (System/nanoTime)]
    (if (> (count patterns) 32) []
    (into [] (comp (take (min 128 (*' 4 maximum)))
                   (take-while (fn [_] (< (- (System/nanoTime) started) 50000000)))
                   (keep (fn [[pair bound]] (candidate views request pair bound ordinal maximum-row-bytes)))
                   (take maximum))
      (for [pair combinations
            :let [selected (vec (mapcat identity pair))
                  sources (set (map :source-symbol selected)) ids (set (map :id selected))
                  edges (count (for [i (range (count pair)) j (range (inc i) (count pair))
                                     :when (seq (set/intersection (vars-in (pair i)) (vars-in (pair j))))] [i j]))
                  boundary (set/intersection (vars-in selected)
                                             (vars-in (filter #(and (= :pattern (:kind %)) (not (ids (:id %)))) patterns)))]
            :when (and (= (count pair) (count sources)) (>= edges (dec (count pair)))
                       (every? (fn [alias] (not (pm/spooled? (:accepted (views alias))))) sources))
            bound (sort boundary)
            :when (not (contains? (:bindings request) bound))]
        [pair bound])))))

(defn batch-query [candidate bindings maximum-row-bytes]
  (or (relational/compile-fragment (:joined-views candidate)
        (assoc (:joined-request candidate) :input-bindings
               (mapv (fn [i binding] (update binding :values #(into [i] %))) (range) bindings)) maximum-row-bytes)
      (throw (ex-info "Joined input requires unsupported conversion" {:error :query-source/invalid :phase :open}))))
