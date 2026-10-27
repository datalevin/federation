(ns datalevin.federation.postgres.plan
  "Pure EAV planning and parameterized SQL generation; no provider I/O."
  (:require [clojure.string :as str]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl QueryKey]
           [java.sql ResultSet]
           [java.time OffsetDateTime]))

(def constraints {:max-binding-rows 256 :max-binding-bytes 262144 :max-request-binding-rows 1})

(defn columns [descriptor clauses]
  (reduce
    (fn [out clause]
      (let [{:keys [entity-type value-type]}
            (get-in descriptor [:pattern-contract :attributes (second (second (:terms clause)))])]
        (reduce (fn [out [term type]]
                  (if (and (= :var (first term)) (not-any? #(= (second term) (:variable %)) out))
                    (conj out {:variable (second term) :type type :nullable? false}) out))
                out (map vector (:terms clause) [entity-type :keyword value-type])))) [] clauses))

(defn choices [descriptor request fusion?]
  (let [patterns (filterv #(= :pattern (:kind %)) (:clauses request))
        groups (when fusion?
                 (->> patterns
                      (group-by #(first (:terms %)))
                      (sort-by (comp pr-str key))
                      (keep (fn [[entity clauses]]
                              (when (and (not= :ignore (first entity)) (> (count clauses) 1)) clauses)))))
        available (into {} (map (juxt :variable identity)) (:available-inputs request))
        candidate
        (fn [clauses anchor kind position]
          (let [cols (columns descriptor clauses)
                [tag x] (when position (nth (:terms anchor) position))
                column (some #(when (= x (:variable %)) %) cols)
                bound? (and (= :var tag) (contains? (:bindings request) x))
                required (if (and position (= :var tag) (not bound?)) [column] [])]
            (when (or (nil? position) (= :const tag) bound?
                      (and column (= column (available x))))
              {:id (str (name kind) "-" (:id anchor) "-" (str/join "-" (map :id clauses)))
               :clauses (vec clauses) :anchor anchor :kind kind :position position
               :columns cols :requires required})))
        for-group (fn [clauses]
                    (concat
                      (keep #(candidate clauses % :entity-lookup 0) clauses)
                      (keep #(candidate clauses % :value-lookup 2) clauses)
                      [(candidate clauses (first clauses) :attribute-enumeration nil)]))]
    (vec (take (get request :max-alternatives 32)
               (concat (mapcat for-group groups) (mapcat #(for-group [%]) patterns))))))

(defn unknown-estimate [dependent?]
  (merge (zipmap (if dependent?
                  [:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
                   :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes]
                  [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes])
                (repeat nil))
         {:confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}))

(defn- operand [bindings [tag x]]
  (cond (= :const tag) [true x]
        (and (= :var tag) (contains? bindings x)) [true (get bindings x)]
        :else [false nil]))

(defn- equality-filter [accepted columns x]
  ;; SQL equality is used only as a superset. The shared query equality is
  ;; always rechecked, including numeric scale, signed zero, and collation.
  (let [xs (if (= 1 (count columns)) [x] x)]
    (when (and (sequential? xs) (= (count columns) (count xs))
                (every? true? (map (fn [column v]
                                    (let [type (pm/types (get-in accepted [:requirements :columns column :type]))]
                                      (and (not= :data type) (value/matches-type? type v)))) columns xs)))
      [(str/join " AND " (map #(str (jdbc/identifier %) " = ?") columns)) (vec xs)])))

(defn- pattern-filters [accepted clauses bindings]
  (mapcat
    (fn [clause]
      (let [[entity _ term] (:terms clause)
            attribute (second (second (:terms clause)))
            columns (pm/attribute-columns accepted attribute)
            [bound? e] (operand bindings entity)
            [vbound? v] (operand bindings term)
            entity-filter
            (when bound?
              (if (and (ref/entity-ref? e)
                        (value/matches-type? (get-in accepted [:identity :type]) e))
                (equality-filter accepted (pm/key-columns accepted)
                                 (let [k (ref/key-tuple e)] (if (= 1 (count k)) (first k) k)))
                ["FALSE" []]))
            v (if (ref/entity-ref? v)
                (let [k (ref/key-tuple v)] (if (= 1 (count k)) (first k) k)) v)]
        (concat (map #(vector (str (jdbc/identifier %) " IS NOT NULL") []) columns)
                (keep identity [entity-filter (when vbound? (equality-filter accepted columns v))])))) clauses))

(defn predicates
  "Only integral comparisons with a matching Long constant are pushed down.
  All other predicates remain core residuals."
  [accepted candidate request]
  (let [vars (into {} (keep (fn [clause]
                              (let [[_ [_ a] [tag v]] (:terms clause)
                                    cs (pm/attribute-columns accepted a)]
                                (when (and (= :var tag) (= 1 (count cs))
                                            (= :long (pm/types (get-in accepted [:requirements :columns (first cs) :type]))))
                                  [v (first cs)]))) (:clauses candidate)))]
    (vec (keep (fn [{:keys [kind operator args] :as predicate}]
                 (when (and (= kind :predicate) (#{:= :not= :< :<= :> :>=} operator))
                   (let [[[tag v] [ctag x]] args]
                     (when (and (= :var tag) (vars v) (= :const ctag) (instance? Long x))
                       (assoc predicate :column (vars v)))))) (:clauses request)))))

(defn sql [accepted plan bindings max-row-bytes]
  (let [columns (vec (distinct (concat (pm/key-columns accepted)
                                      (mapcat #(pm/attribute-columns accepted (second (second (:terms %))))
                                              (:clauses plan)))))
        size (str/join " + " (map #(str "COALESCE(octet_length(" (jdbc/identifier %) "::text)::bigint,0)") columns))
        bounded (str "(" size ") <= " (long max-row-bytes))
        filters (concat (pattern-filters accepted (:clauses plan) bindings)
                        (map (fn [{:keys [column operator args]}]
                               [(str (jdbc/identifier column) " "
                                     ({:= "=" :not= "<>" :< "<" :<= "<=" :> ">" :>= ">="} operator) " ?")
                                [(second (second args))]]) (:predicates plan)))]
    {:columns columns
     :sql (str "SELECT NOT (" bounded "), "
               (str/join ", " (map #(str "CASE WHEN " bounded " THEN " (jdbc/identifier %) " END") columns))
               " FROM ONLY " (jdbc/relation (get-in accepted [:requirements :table]))
               (when (seq filters) (str " WHERE " (str/join " AND " (map first filters)))))
     :params (vec (mapcat second filters))}))

(defn decode [accepted column ^ResultSet rs i]
  (let [x (case (get-in accepted [:requirements :columns column :type])
            ("int2" "int4" "int8") (.getLong rs (int i))
            "bool" (.getBoolean rs (int i))
            ("text" "varchar") (.getString rs (int i))
            "uuid" (.getObject rs (int i) java.util.UUID)
            "bytea" (.getBytes rs (int i))
            "numeric" (.getBigDecimal rs (int i))
            "float4" (.getFloat rs (int i))
            "float8" (.getDouble rs (int i))
            "timestamptz" (let [x (.getObject rs (int i) OffsetDateTime)]
                            (when (or (= x OffsetDateTime/MAX) (= x OffsetDateTime/MIN))
                              (jdbc/fail! :query-source/invalid :values))
                            (some-> x .toInstant)))]
    (when-not (.wasNull rs) x)))

(defn read-row [accepted columns ^ResultSet rs]
  (when (.getBoolean rs 1) (jdbc/fail! :query-source/limit :decode))
  (into {} (map-indexed (fn [i column] [column (decode accepted column rs (+ i 2))]) columns)))

(defn match-row [accepted plan bindings row]
  (let [keys (mapv row (pm/key-columns accepted))
        entity (ref/entity-ref (get-in accepted [:identity :type]) keys)
        matched
        (reduce
          (fn [bound clause]
            (when bound
              (let [attribute (second (second (:terms clause)))
                    raw (mapv row (pm/attribute-columns accepted attribute))
                    x (when (every? some? raw)
                        (if-let [type (get-in accepted [:attributes attribute :reference :type])]
                          (ref/entity-ref type raw)
                          (if (= 1 (count raw)) (first raw) raw)))]
                (when (some? x)
                  (reduce (fn [out [term x]]
                            (when out
                              (case (first term)
                                :ignore out
                                :const (when (QueryKey/equiv (second term) x) out)
                                :var (let [sym (second term)]
                                       (if (contains? out sym)
                                         (when (if (contains? bindings sym)
                                                 (QueryKey/equals (out sym) x)
                                                 (QueryKey/equiv (out sym) x)) out)
                                         (assoc out sym x))))))
                          bound (map vector (:terms clause) [entity attribute x]))))))
          bindings (:clauses plan))]
    (when matched (mapv matched (map :variable (:columns plan))))))
