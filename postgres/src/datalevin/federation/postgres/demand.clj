(ns datalevin.federation.postgres.demand
  "Conservative SQL composition for scalar duplicate-insensitive demand."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datalevin.federation.postgres.jdbc :as jdbc]))

(defn table-sql [{:keys [alias accepted]}]
  (str "ONLY " (jdbc/relation (get-in accepted [:requirements :table])) " AS " alias))

(defn- single-row-demand
  "Return a restriction composition only when every observable projection and
  validation depends on one retained row. Decline all unsupported demands."
  [{:keys [tables input grouping window find with optional? absence? used variables
           native-expressions conditions oversized]}]
  (let [value-aliases (into #{} (mapcat #(or (:aliases (native-expressions %)) (:aliases (variables %)))) used)
        validation-aliases
        (into #{} (for [{:keys [alias accepted columns]} tables
                        :when (some #(#{"numeric" "timestamptz"} (get-in accepted [:requirements :columns % :type])) columns)] alias))
        observed (set/union value-aliases validation-aliases)
        reason (cond input :input-bindings
                     (seq grouping) :grouping
                     window :window
                     (seq with) :with-witnesses
                     optional? :optional
                     absence? :absence
                     (not (and (seq find) (every? #(and (= :aggregate (:kind %)) (#{:min :max} (:operator %))) find))) :witness-sensitive
                     (not= oversized "FALSE") :row-size-effect
                     (not= 1 (count observed)) :multiple-observed-rows
                     (not (< 1 (count tables))) :no-inner-relation)
        retained (when-not reason (first (filter #(observed (:alias %)) tables)))
        reason (or reason (when-not retained :unknown-retained-row))]
    (if reason {:applied? false :reason reason :value-aliases (vec (sort value-aliases)) :validation-aliases (vec (sort validation-aliases))}
      (let [inner (remove #(= (:alias retained) (:alias %)) tables)
            condition (if (seq conditions) (str/join " AND " (map #(str "(" % ")") conditions)) "TRUE")]
        {:applied? true :retained-alias (:alias retained)
         :inner-aliases (mapv :alias inner) :value-aliases (vec (sort value-aliases))
         :validation-aliases (vec (sort validation-aliases))
         :from (table-sql retained)
         :conditions [(str "EXISTS (SELECT 1 FROM " (str/join ", " (map table-sql inner)) " WHERE " condition ")")]}))))

(defn unquoted
  "Mask quoted contents in the compiler's SQL-expression grammar.
  Values remain parameters; this helper never rewrites user SQL."
  [^String sql]
  (let [n (.length sql) out (StringBuilder.)]
    (loop [i 0 quote nil]
      (if (= i n)
        (do (when quote (throw (ex-info "Unclosed compiler quotation" {::decline true}))) (str out))
        (let [c (.charAt sql i)]
          (if quote
            (do (.append out \space)
                (if (= c quote)
                  (if (and (< (inc i) n) (= quote (.charAt sql (inc i))))
                    (do (.append out \space) (recur (+ i 2) quote))
                    (recur (inc i) nil))
                  (recur (inc i) quote)))
            (if (#{\' \"} c)
              (do (.append out \space) (recur (inc i) c))
              (do (.append out c) (recur (inc i) nil)))))))))

(defn condition-records [conditions parameters aliases]
  (loop [pending (seq conditions) offset 0 index 0 out []]
    (if-let [sql (first pending)]
      (let [masked (unquoted sql) refs (into #{} (map second) (re-seq #"\b(r\d+)\s*\." masked))
            n (count (filter #{\?} masked)) end (+ offset n)]
        (when-not (and (set/subset? refs aliases) (<= end (count parameters)))
          (throw (ex-info "Unknown compiler reference or parameter" {::decline true})))
        (recur (next pending) end (inc index)
               (conj out {:index index :sql sql :aliases refs :params (subvec parameters offset end)
                          :parameter-indices (vec (range offset end))})))
      (do (when-not (= offset (count parameters))
            (throw (ex-info "Parameters outside supported conditions" {::decline true}))) out))))

(defn graph [aliases records]
  (reduce (fn [g {:keys [aliases]}]
            (reduce (fn [g a] (update g a set/union (disj aliases a))) g aliases))
          (zipmap aliases (repeat #{})) records))

(defn components [g nodes order]
  (loop [pending nodes out []]
    (if (empty? pending) out
      (let [start (first (filter pending order))
            group (loop [seen #{start}]
                    (let [more (set/union seen (set/intersection pending (into #{} (mapcat g) seen)))]
                      (if (= seen more) seen (recur more))))]
        (recur (set/difference pending group) (conj out group))))))

(defn shortest-path [g allowed order start target]
  (loop [queue [[start]] seen #{start}]
    (when-let [path (first queue)]
      (let [last-node (peek path)]
        (if (= last-node target) path
          (let [neighbors (filter #(and (allowed %) ((g last-node) %) (not (seen %))) order)]
            (recur (into (vec (rest queue)) (map #(conj path %) neighbors)) (into seen neighbors))))))))

(defn connected-core [g nodes required order]
  (let [roots (filterv required order)]
    (when (seq roots)
      (reduce (fn [core target]
                (if-let [path (shortest-path g nodes order (first roots) target)] (into core path)
                  (throw (ex-info "Disconnected observed interface" {::decline true}))))
              required (rest roots)))))

(defn conjunction [clauses]
  (if (seq clauses) (str/join " AND " (map #(str "(" % ")") clauses)) "TRUE"))

(defn compose-scope [tables order g nodes records outer required]
  (let [boundary (into #{} (mapcat (fn [{:keys [aliases]}]
                                   (let [outside (set/difference aliases nodes)]
                                     (when-not (set/subset? outside outer)
                                       (throw (ex-info "Cross-sibling qualification" {::decline true})))
                                     (when (seq outside) (set/intersection aliases nodes))))) records)
        core (or (connected-core g nodes (or required boundary) order) nodes)
        visible (set/union outer core)
        local (filterv #(set/subset? (:aliases %) visible) records)
        groups (components g (set/difference nodes core) order)
        children (mapv (fn [group]
                         (compose-scope tables order g group
                                        (filterv #(seq (set/intersection group (:aliases %))) records)
                                        visible nil)) groups)
        local-sql (mapv :sql local)
        exists (mapv #(str "EXISTS (SELECT 1 FROM " (:from %) " WHERE " (conjunction (:conditions %)) ")") children)
        indices (into (mapv :index local) (mapcat :condition-order) children)]
    (when-not (= (sort (map :index records)) (sort indices))
      (throw (ex-info "A qualification predicate was lost or repeated" {::decline true})))
    {:from (str/join ", " (map #(table-sql (tables %)) (filter core order)))
     :conditions (into local-sql exists)
     :params (vec (concat (mapcat :params local) (mapcat :params children)))
     :condition-order indices
     :parameter-order (vec (concat (mapcat :parameter-indices local) (mapcat :parameter-order children)))
     :all-aliases (vec (concat (filter core order) (mapcat :all-aliases children)))
     :scopes (inc (reduce + 0 (map :scopes children)))
     :tree {:aliases (filterv core order) :outer-aliases (filterv outer order)
            :condition-indices (mapv :index local) :children (mapv :tree children)}}))

(defn choose
  "Retain every value/validation alias in a connected root. Recursively
  factor independent qualification components at their connected interfaces."
  [{:keys [tables input grouping window find with optional? absence? used variables
           native-expressions conditions oversized params] :as options}]
  (let [fallback (single-row-demand options)
        value-aliases (into #{} (mapcat #(or (:aliases (native-expressions %)) (:aliases (variables %)))) used)
        validation-aliases (into #{} (for [{:keys [alias accepted columns]} tables
                                          :when (some #(#{"numeric" "timestamptz"}
                                                         (get-in accepted [:requirements :columns % :type])) columns)] alias))
        observed (set/union value-aliases validation-aliases)
        reason (cond input :input-bindings (seq grouping) :grouping window :window
                     (seq with) :with-witnesses optional? :optional absence? :absence
                     (not (and (seq find) (every? #(and (= :aggregate (:kind %)) (#{:min :max} (:operator %))) find))) :witness-sensitive
                     (not= oversized "FALSE") :row-size-effect
                     (empty? observed) :no-observed-row)]
    (if reason (assoc fallback :scoped? false :scope-reason reason)
      (try
        (let [order (mapv :alias tables) nodes (set order)
              _ (when-not (set/subset? observed nodes) (throw (ex-info "Unknown observed alias" {::decline true})))
              records (condition-records conditions (vec params) nodes)
              g (graph nodes records)
              result (compose-scope (zipmap order tables) order g nodes records #{} observed)]
          (when-not (and (= (sort (:all-aliases result)) (sort order))
                         (= (sort (:parameter-order result)) (range (count params))))
            (throw (ex-info "Source occurrence or binding coverage differs" {::decline true})))
          (if (or (= 1 (:scopes result)) (and (= 1 (count observed)) (<= (:scopes result) 2)))
            (assoc fallback :scoped? false :scope-reason :no-additional-scope)
            (assoc result :applied? true :scoped? true :value-aliases (vec (sort value-aliases))
                   :validation-aliases (vec (sort validation-aliases)))))
        (catch clojure.lang.ExceptionInfo e
          (if (::decline (ex-data e)) (assoc fallback :scoped? false :scope-reason (.getMessage e)) (throw e)))))))
