(ns datalevin.federation.postgres.plan
  "Pure EAV planning and parameterized SQL generation; no provider I/O."
  (:require [clojure.string :as str]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.temporal :as temporal]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl QueryKey]
           [java.sql ResultSet]
           [java.math BigDecimal]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime]
           [java.time.temporal ChronoUnit]))

(def constraints {:max-binding-rows 256 :max-binding-bytes 262144 :max-request-binding-rows 256})

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
        fused-ids (into #{} (mapcat #(map :id %)) groups)
        units (into (vec groups) (comp (remove #(contains? fused-ids (:id %))) (map vector)) patterns)
        maximum (get request :max-alternatives 32)
        _ (when (> (count units) maximum) (jdbc/fail! :query-source/limit :planning))
        available (into {} (map (juxt :variable identity)) (:available-inputs request))
        candidate
        (fn [clauses cols anchor kind position]
          (let [[tag x] (when position (nth (:terms anchor) position))
                column (some #(when (= x (:variable %)) %) cols)
                bound? (and (= :var tag) (contains? (:bindings request) x))
                required (if (and position (= :var tag) (not bound?)) [column] [])]
            (when (or (nil? position) (= :const tag) bound?
                      (and column (= column (available x))))
              {:id (str (name kind) "-" (:id anchor) "-" (str/join "-" (map :id clauses)))
               :clauses (vec clauses) :anchor anchor :kind kind :position position
               :columns cols :requires required})))
        for-group (fn [clauses]
                    (let [cols (columns descriptor clauses)]
                      (concat
                        ;; All clauses in a group have the same entity term.
                        (keep identity [(candidate clauses cols (first clauses) :entity-lookup 0)])
                        (keep #(candidate clauses cols % :value-lookup 2) clauses)
                        [(candidate clauses cols (first clauses) :attribute-enumeration nil)])))
        ;; These independent accesses partition the patterns, so they remain
        ;; schedulable even when available inputs are only potential producers.
        ;; Coverage in dependent lookups alone could leave a dependency cycle.
        required (mapv #(candidate % (columns descriptor %) (first %) :attribute-enumeration nil) units)
        required-ids (set (map :id required))
        candidates (concat (mapcat for-group groups) (mapcat #(for-group [%]) patterns))
        selected (into required
                       (comp (remove #(contains? required-ids (:id %)))
                             (take (- maximum (count required)))) candidates)
        group-order (zipmap (map #(mapv :id %) (concat groups (map vector patterns))) (range))
        anchor-order (zipmap (map :id patterns) (range))]
    ;; Preserve access preference without realizing the discarded candidate tail.
    (vec (sort-by (fn [{:keys [clauses kind anchor]}]
                    [(group-order (mapv :id clauses))
                     (case kind :entity-lookup 0 :value-lookup 1 :attribute-enumeration 2)
                     (anchor-order (:id anchor))]) selected))))

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

(defn- column-sql [column] (str "r." (jdbc/identifier column)))

(def operators {:= "=" :not= "<>" :< "<" :<= "<=" :> ">" :>= ">="})
(def reverse-operator {:= := :not= :not= :< :> :<= :>= :> :< :>= :<=})

(defn comparison-filter
  "Compile a comparison with matching scalar representations. Timestamp
  cutoffs are reduced to exact microsecond comparisons before binding; the
  original predicate remains a core residual. Unproved cases return nil."
  [type column operator x]
  (when (operators operator)
    (cond
      (and (#{"int2" "int4" "int8"} type) (int? x))
      [(str column " " (operators operator) " ?::int8") [x]]

      (and (= type "numeric") (instance? BigDecimal x)
           (<= (- (.precision ^BigDecimal x) (.scale ^BigDecimal x)) 131072)
           (<= (.scale ^BigDecimal x) 16383))
      [(str column " " (operators operator) " ?::numeric") [x]]

      (and (= type "date") (instance? LocalDate x) (<= 1 (.getYear ^LocalDate x) 9999))
      [(str column " " (operators operator) " ?::date") [(str x)]]

      (or (and (= type "timestamp") (instance? LocalDateTime x) (<= 1 (.getYear ^LocalDateTime x) 9999))
          (and (= type "time") (instance? LocalTime x)))
      (let [floor (if (= type "time") (.truncatedTo ^LocalTime x ChronoUnit/MICROS)
                      (.truncatedTo ^LocalDateTime x ChronoUnit/MICROS))
            aligned? (= x floor)
            op (if aligned? operator (case operator :>= :> :< :<= operator))]
        (cond
          (and (not aligned?) (= operator :=)) ["FALSE" []]
          (and (not aligned?) (= operator :not=)) ["TRUE" []]
          :else [(str column " " (operators op) " ?::" type) [(str floor)]]))

      (and (= type "timetz") (= operator :=) (instance? OffsetTime x)
           (<= (Math/abs (.getTotalSeconds (.getOffset ^OffsetTime x))) 57599))
      (if (zero? (mod (.getNano ^OffsetTime x) 1000))
        [(str column " = ?::timetz") [(str x)]] ["FALSE" []])

      (and (= type "timestamptz") (instance? Instant x)
           (not (.isBefore ^Instant x (Instant/parse "0001-01-01T00:00:00Z")))
           (.isBefore ^Instant x (Instant/parse "+10000-01-01T00:00:00Z")))
      (let [floor (.truncatedTo ^Instant x ChronoUnit/MICROS)
            aligned? (= x floor)
            op (if aligned? operator (case operator :>= :> :< :<= operator))]
        (cond
          (and (not aligned?) (= operator :=)) ["FALSE" []]
          (and (not aligned?) (= operator :not=)) ["TRUE" []]
          :else [(str column " " (operators op) " ?::timestamptz") [(str floor)]])))))

(defn- equality-filter [accepted columns x]
  ;; SQL equality is used only as a superset. The shared query equality is
  ;; always rechecked, including numeric scale, signed zero, and collation.
  (let [xs (if (= 1 (count columns)) [x] x)]
    (when (and (sequential? xs) (= (count columns) (count xs))
                (every? true? (map (fn [column v]
                                    (let [type (pm/logical-type (get-in accepted [:requirements :columns column]))]
                                      (or (and (not= :data type) (value/matches-type? type v))
                                          (some? (comparison-filter
                                                   (pm/storage-type (get-in accepted [:requirements :columns column]))
                                                   (column-sql column) := v))))) columns xs)))
      (let [filters (mapv (fn [column v]
                            (or (comparison-filter (pm/storage-type (get-in accepted [:requirements :columns column]))
                                                   (column-sql column) := v)
                                [(str (column-sql column) (when (get-in accepted [:requirements :columns column :enum]) "::text") " = ?") [v]])) columns xs)]
        [(str/join " AND " (map first filters)) (vec (mapcat second filters))]))))

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
        (concat (map #(vector (str (column-sql %) " IS NOT NULL") []) columns)
                (keep identity [entity-filter (when vbound? (equality-filter accepted columns v))])))) clauses))

(defn predicates
  "Push matching integral, decimal, and Instant comparisons as prefilters.
  Unsupported types and comparisons remain core residuals."
  [accepted candidate request]
  (let [vars (into {} (keep (fn [clause]
                              (let [[_ [_ a] [tag v]] (:terms clause)
                                    cs (pm/attribute-columns accepted a)]
                                (when (and (= :var tag) (= 1 (count cs)))
                                  [v (first cs)]))) (:clauses candidate)))]
    (vec (keep (fn [{:keys [kind operator args] :as predicate}]
                 (when (and (= kind :predicate) (= 2 (count args)))
                   (let [[a b] args
                         [a b op] (if (= :const (first a)) [b a (reverse-operator operator)] [a b operator])
                         [tag v] a [ctag x] b column (vars v)]
                     (when (and (= :var tag) column (= :const ctag))
                       (when-let [filter (comparison-filter (pm/storage-type (get-in accepted [:requirements :columns column]))
                                                           (column-sql column) op x)]
                         (assoc predicate :column column :filter filter)))))) (:clauses request)))))

(defn- selected-columns [accepted plan]
  (if (:snapshot? plan)
    (vec (keys (get-in accepted [:requirements :columns])))
    (vec (distinct (concat (pm/key-columns accepted)
                           (mapcat #(pm/attribute-columns accepted (second (second (:terms %))))
                                   (:clauses plan)))))))

(defn fetch-charge
  "Reserve a decoded page and scratch. Fixed-width PostgreSQL scalars need
  at most 64 text bytes each; variable-width values retain the SQL row cap."
  [accepted plan fetch-size max-row-bytes]
  (let [columns (selected-columns accepted plan)
        fixed? (every? #(#{"int2" "int4" "int8" "bool" "uuid" "float4" "float8" "timestamptz"}
                          (get-in accepted [:requirements :columns % :type])) columns)
        row-bytes (if fixed? (min max-row-bytes (* 64 (count columns))) max-row-bytes)]
    (+ 131072 (* fetch-size (+ (* 16 row-bytes) (* 1024 (count columns)) 2048)))))

(defn- query-sql [accepted plan bindings max-row-bytes join-sql ordinal params]
  (let [columns (selected-columns accepted plan)
        size (str/join " + " (map #(str "COALESCE(octet_length(" (column-sql %) "::text)::bigint,0)") columns))
        bounded (str "(" size ") <= " (long max-row-bytes))
        filters (concat (pattern-filters accepted (:clauses plan) bindings)
                        (map :filter (:predicates plan)))]
    {:columns columns
     :sql (str "SELECT " ordinal "NOT (" bounded "), "
               (str/join ", " (map #(str "CASE WHEN " bounded " THEN " (column-sql %) " END") columns))
               " FROM " (when (= "r" (get-in accepted [:requirements :kind])) "ONLY ")
               (jdbc/relation (get-in accepted [:requirements :table])) " AS r" join-sql
               (when (seq filters) (str " WHERE " (str/join " AND " (map first filters)))))
     :params (into (vec params) (mapcat second) filters)}))

(defn sql [accepted plan bindings max-row-bytes]
  (query-sql accepted plan bindings max-row-bytes nil nil []))

(defn snapshot-sql [accepted max-row-bytes]
  (query-sql accepted {:snapshot? true} {} max-row-bytes nil nil []))

(defn batch-sql
  "Join bounded input keys to the table and return an input ordinal per row.
  SQL equality remains a superset; each result is rechecked with its original
  binding. Keys without safe equality pushdown use an ordinal-only join."
  [accepted plan inputs max-row-bytes]
  (jdbc/ensure! (and (<= 1 (count inputs) (:max-request-binding-rows constraints))
                     (= 1 (count (:requires plan)))) :open)
  (let [entity? (= 0 (:position plan))
        columns (if entity? (pm/key-columns accepted)
                    (pm/attribute-columns accepted (second (second (:terms (:anchor plan))))))
        keys (mapv (fn [{:keys [values]}]
                     (let [x (first values)
                           valid? (or (not entity?)
                                      (and (ref/entity-ref? x)
                                           (value/matches-type? (get-in accepted [:identity :type]) x)))
                           x (if (ref/entity-ref? x)
                               (let [key (ref/key-tuple x)] (if (= 1 (count key)) (first key) key)) x)]
                       (when valid?
                         (let [params (second (equality-filter accepted columns x))]
                           (when (= (count columns) (count params)) params))))) inputs)
        pushdown? (every? some? keys)
        ;; Widen integral inputs: casting an out-of-range lookup key to int2
        ;; or int4 would fail instead of correctly finding no matching row.
        types (mapv #(let [type (pm/storage-type (get-in accepted [:requirements :columns %]))]
                       (cond (= type "enum") "text" (#{"int2" "int4"} type) "int8" :else type)) columns)
        names (mapv #(str "k" %) (range (count columns)))
        placeholders (when pushdown? (str ", " (str/join ", " (map #(str "?::" %) types))))
        rows (map-indexed (fn [i _] (str "(" i placeholders ")")) inputs)
        join-sql (str " JOIN (VALUES " (str/join ", " rows) ") AS b(i"
                      (when pushdown? (str ", " (str/join ", " names))) ") ON "
                      (if pushdown?
                        (str/join " AND " (map #(str (column-sql %1) (when (get-in accepted [:requirements :columns %1 :enum]) "::text") " = b." %2) columns names))
                        "TRUE"))]
    (query-sql accepted plan (:bindings plan) max-row-bytes join-sql "b.i, "
               (when pushdown? (mapcat identity keys)))))

(defn decode-value [type ^ResultSet rs i]
  (let [x (case type
            ("int2" "int4" "int8") (.getLong rs (int i))
            "bool" (.getBoolean rs (int i))
            ("text" "varchar" "enum") (.getString rs (int i))
            ("date" "timestamp" "time" "timetz" "interval") (temporal/decode type rs i)
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

(defn decode [accepted column rs i]
  (let [physical (get-in accepted [:requirements :columns column])
        x (decode-value (pm/storage-type physical) rs i)]
    (when (and (:enum physical) (some? x) (not (some #{x} (get-in physical [:enum :labels]))))
      (jdbc/fail! :query-source/stale-definition :decode))
    x))

(defn read-row
  ([accepted columns rs] (read-row accepted columns rs 0))
  ([accepted columns ^ResultSet rs offset]
   (when (.getBoolean rs (inc offset)) (jdbc/fail! :query-source/limit :decode))
   (into {} (map-indexed (fn [i column] [column (decode accepted column rs (+ i offset 2))]) columns))))

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
