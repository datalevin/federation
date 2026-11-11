(ns datalevin.federation.postgres.plan
  "Pure EAV planning and parameterized SQL generation; no provider I/O."
  (:require [clojure.string :as str]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.temporal :as temporal]
            [datalevin.query.source.value :as value])
  (:import [java.nio.charset StandardCharsets]
           [datalevin.utl QueryKey]
           [java.sql ResultSet]
           [java.math BigDecimal]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime]
           [java.time.temporal ChronoUnit]))

(def constraints {:max-binding-rows 256 :max-binding-bytes 2097152 :max-request-binding-rows 256})

(defn dependent-preference
  "Cheap whole-query policy over a typed join graph. Retain selective text
  dimension drivers with membership filters or identity-only existence probes.
  This is a structural heuristic, not a row-count or elapsed-time estimate."
  [views request]
  (let [clauses (:clauses request)
        patterns (filterv #(= :pattern (:kind %)) clauses)
        group-key #(vector (:source-symbol %) (first (:terms %)))
        groups (group-by group-key patterns)
        producers (into {} (keep #(let [[tag v] (nth (:terms %) 2)]
                                    (when (= :var tag) [v (group-key %)]))) patterns)
        outgoing (into #{} (keep #(when (get-in (views (:source-symbol %))
                                               [:attributes (second (second (:terms %))) :reference])
                                   (group-key %))) patterns)
        predicates (filterv #(= :predicate (:kind %)) clauses)
        positive (into #{} (keep #(when (and (= :like (:operator %)) (= :var (ffirst (:args %))))
                                    (second (first (:args %))))) predicates)
        selective (into #{} (keep #(let [[[tag v] [kind pattern]] (:args %)]
                                    (when (and (= :like (:operator %)) (= :var tag) (= :const kind)
                                               (string? pattern) (<= 4 (count (remove #{\% \_} pattern)))) v))) predicates)
        probe-drivers (into selective
                       (keep #(let [[[tag v] [kind pattern]] (:args %)]
                                (when (and (= :like (:operator %)) (= :var tag) (= :const kind)
                                           (string? pattern) (seq pattern)
                                           (not (#{\% \_} (first pattern)))) v))) predicates)
        drivers (into #{} (comp (keep producers) (remove outgoing)) selective)
        projected (into #{} (map :variable) (:find request))
        small? (<= (count groups) 8)]
    (when (and (<= 3 (count groups)) (seq positive)
               (every? #(and (= :aggregate (:kind %)) (#{:min :max} (:operator %))) (:find request))
               (not-any? #(#{:absence :optional} (:kind %)) clauses))
      (cond
        (and (some #(let [[[vtag v] [tag xs]] (:args %)]
                      (and (= :in (:operator %)) (= :const tag)
                           (= :var vtag) (producers v) (not (outgoing (producers v)))
                           (or (sequential? xs) (set? xs)) (< 1 (count xs)))) predicates)
             (or (and small? (some projected selective))
                 (and (<= (count groups) 12) (<= 2 (count drivers)))))
        :text-membership-driver

        (and small? (seq probe-drivers))
        (let [variables (fn [terms] (keep #(when (= :var (first %)) (second %)) terms))
              uses (frequencies (mapcat #(variables (:terms %)) patterns))
              live (into (into projected (:with request)) (mapcat #(variables (:args %))) predicates)
              references (into #{} (keep (fn [{:keys [source-symbol terms]}]
                                          (when (get-in (views source-symbol) [:attributes (second (second terms)) :reference])
                                            (nth terms 2)))) patterns)
              witnesses (filter (fn [[[symbol entity] ps]]
                                  (let [identity (get-in (views symbol) [:identity :attribute])]
                                    (and (contains? references entity)
                                         (not (some live (variables [entity])))
                                         (every? #(and (= identity (second (second (:terms %))))
                                                       (every? (fn [v] (and (= 1 (uses v)) (not (live v))))
                                                               (variables [(nth (:terms %) 2)]))) ps)))) groups)]
          (when (<= 2 (count witnesses)) :existence-probes))))))

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
        ;; Share each group's typed columns across independent and dependent
        ;; alternatives. Delays preserve the candidate quota's lazy tail.
        work (mapv (fn [clauses] {:clauses clauses :columns (delay (columns descriptor clauses))})
                   (concat groups (map vector patterns)))
        units (filterv #(or (> (count (:clauses %)) 1)
                            (not (contains? fused-ids (:id (first (:clauses %)))))) work)
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
        for-group (fn [{:keys [clauses columns]}]
                    (let [cols @columns]
                      (concat
                        ;; All clauses in a group have the same entity term.
                        (keep identity [(candidate clauses cols (first clauses) :entity-lookup 0)])
                        (keep #(candidate clauses cols % :value-lookup 2) clauses)
                        [(candidate clauses cols (first clauses) :attribute-enumeration nil)])))
        ;; These independent accesses partition the patterns, so they remain
        ;; schedulable even when available inputs are only potential producers.
        ;; Coverage in dependent lookups alone could leave a dependency cycle.
        required (mapv (fn [{:keys [clauses columns]}]
                         (candidate clauses @columns (first clauses) :attribute-enumeration nil)) units)
        required-ids (set (map :id required))
        candidates (mapcat for-group work)
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

      ;; ASCII cutoffs agree with Java UTF-16 ordering for every row: any
      ;; non-ASCII first differing character sorts after the cutoff in both.
      ;; Equality keeps its existing native-index prefilter and core recheck.
      (and (#{"text" "varchar"} type) (#{:< :<= :> :>=} operator)
           (string? x) (not (str/includes? x "\u0000"))
           (.canEncode (.newEncoder StandardCharsets/UTF_8) ^String x)
           (every? #(<= (int %) 127) x))
      [(str "(" column ") COLLATE pg_catalog.\"C\" " (operators operator) " ?::text") [x]]

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

(defn- like-pattern
  "Translate default-escape ASCII patterns, preserving the native matcher's
  ignored trailing escape. Invalid escapes and non-ASCII patterns stay local."
  [pattern]
  (when (string? pattern)
    (let [^String pattern pattern n (.length pattern)]
      (loop [i 0]
        (if (= i n) pattern
          (let [c (.charAt pattern i)]
            (cond
              (or (= c \u0000) (> (int c) 127)) nil
              (= c \!) (if (= (inc i) n) (.substring pattern 0 i)
                           (when (#{\! \% \_} (.charAt pattern (inc i)))
                             (recur (+ i 2))))
              :else (recur (inc i)))))))))

(defn like-filter
  "Compile case-sensitive LIKE/NOT LIKE for text/varchar with default ! escape.
  Wildcards still match Unicode code points, including newlines. C collation
  avoids case/punctuation equivalences of provider collations. Return nil for
  a pattern whose transport or matcher semantics have not been established."
  [type column operator pattern]
  (when (and (#{"text" "varchar"} type) (#{:like :not-like} operator))
    (when-let [pattern (like-pattern pattern)]
      [(str column " COLLATE pg_catalog.\"C\" " (if (= :like operator) "LIKE" "NOT LIKE")
            " ?::text ESCAPE '!'") [pattern]])))

(defn like-any-filter
  "Advisory OR of fully admitted LIKE patterns. Decline the entire union when
  any branch is unsupported; pushing only some branches could lose matches."
  [type column patterns]
  (when (and (vector? patterns) (<= 1 (count patterns) 32))
    (let [filters (mapv #(like-filter type column :like %) patterns)]
      (when (every? some? filters)
        [(str "(" (str/join " OR " (map #(str "(" (first %) ")") filters)) ")")
         (vec (mapcat second filters))]))))

(defn predicates
  "Push matching scalar comparisons and supported LIKE calls as prefilters.
  Unsupported types and comparisons remain core residuals."
  [accepted candidate request]
  (let [vars (into {} (keep (fn [clause]
                              (let [[_ [_ a] [tag v]] (:terms clause)
                                    cs (pm/attribute-columns accepted a)]
                                (when (and (= :var tag) (= 1 (count cs)))
                                  [v (first cs)]))) (:clauses candidate)))]
    (vec (keep (fn [{:keys [kind operator args] :as predicate}]
                 (when (and (= kind :predicate) (= 2 (count args)))
                   (let [[a b] args like? (#{:like :not-like :like-any} operator)
                         [a b op] (if (and (not like?) (= :const (first a)))
                                    [b a (reverse-operator operator)] [a b operator])
                         [tag v] a [ctag x] b column (vars v)]
                     (when (and (= :var tag) column (= :const ctag))
                       (when-let [filter (if (= op :like-any)
                                         (like-any-filter (pm/storage-type (get-in accepted [:requirements :columns column]))
                                                          (column-sql column) x)
                                         ((if like? like-filter comparison-filter)
                                          (pm/storage-type (get-in accepted [:requirements :columns column]))
                                          (column-sql column) op x))]
                         (assoc predicate :column column :filter filter)))))) (:clauses request)))))

(defn estimate-sql
  "A single-table, filter-only projection for EXPLAIN, never ANALYZE. Only
  independent accesses with pushed predicates need query-specific statistics."
  [accepted candidate]
  (when (and (empty? (:requires candidate)) (seq (:predicates candidate)))
    (let [tautologies (into #{} (keep (fn [[column info]]
                                       (when (:not-null? info) [(str (column-sql column) " IS NOT NULL") []])))
                           (get-in accepted [:requirements :columns]))
          filters (->> (concat (pattern-filters accepted (:clauses candidate) (:bindings candidate))
                               (map :filter (:predicates candidate)))
                       (remove tautologies) distinct (sort-by first))]
      {:sql (str "SELECT 1 FROM ONLY " (jdbc/relation (get-in accepted [:requirements :table]))
                 " AS r WHERE " (str/join " AND " (map first filters)))
       :params (vec (mapcat second filters))})))

(defn- selected-columns [accepted plan]
  (if (:snapshot? plan)
    (vec (keys (get-in accepted [:requirements :columns])))
    (vec (distinct (concat (pm/key-columns accepted)
                           (mapcat #(pm/attribute-columns accepted (second (second (:terms %))))
                                   (:clauses plan)))))))

(defn- value-columns [accepted plan]
  (set (concat (pm/key-columns accepted)
               (mapcat (fn [clause]
                         (when (not= :ignore (first (nth (:terms clause) 2)))
                           (pm/attribute-columns accepted (second (second (:terms clause))))))
                       (:clauses plan)))))

(defn- invalid-sql
  "Build a guard for conversions that can fail during decoding."
  [type column]
  (case type
    "numeric" (str column "::text IN ('NaN','Infinity','-Infinity')")
    ("date" "timestamp" "timestamptz" "interval") (str "NOT isfinite(" column ")")
    ("time" "timetz") (str column "::text LIKE '24:%'")
    "FALSE"))

(defn- text-size [column]
  (str "COALESCE(octet_length(" (column-sql column) "::text)::bigint,0)"))

(defn- row-size [columns]
  (if (seq columns) (str/join " + " (map text-size columns)) "0"))

(defn fetch-charge
  "Reserve a decoded page and scratch. Presence flags and fixed-width scalars
  use small bounds. Captures can retain one capped value per physical column."
  [accepted plan fetch-size max-row-bytes]
  (let [columns (selected-columns accepted plan)
        values (if (:snapshot? plan) (set columns) (value-columns accepted plan))
        fixed? (every? #(or (not (values %))
                           (#{"int2" "int4" "int8" "bool" "uuid" "float4" "float8" "timestamptz"}
                             (get-in accepted [:requirements :columns % :type]))) columns)
        row-bytes (if fixed? (min max-row-bytes (* 64 (count columns)))
                      (* max-row-bytes (if (:snapshot? plan) (count columns) 1)))]
    (+ 131072 (* fetch-size (+ (* 16 row-bytes) (* 1024 (count columns)) 2048)))))

(defn- query-sql [accepted plan bindings max-row-bytes join-sql ordinal params]
  (let [columns (selected-columns accepted plan)
        values (value-columns accepted plan)
        presence (set (remove values columns))
        bounded (str "(" (row-size (filterv values columns)) ") <= " (long max-row-bytes))
        invalid (str/join " OR " (cons "FALSE" (map #(invalid-sql
                                                       (pm/storage-type (get-in accepted [:requirements :columns %]))
                                                       (column-sql %)) presence)))
        filters (concat (pattern-filters accepted (:clauses plan) bindings)
                        (map :filter (:predicates plan)))]
    {:columns columns :presence-columns presence
     :sql (str "SELECT " ordinal "NOT (" bounded "), (" invalid "), "
               (str/join ", " (map #(if (presence %) (str (column-sql %) " IS NOT NULL")
                                        (str "CASE WHEN " bounded " THEN " (column-sql %) " END")) columns))
               " FROM " (when (= "r" (get-in accepted [:requirements :kind])) "ONLY ")
               (jdbc/relation (get-in accepted [:requirements :table])) " AS r" join-sql
               (when (seq filters) (str " WHERE " (str/join " AND " (map first filters)))))
     :params (into (vec params) (mapcat second) filters)}))

(defn sql [accepted plan bindings max-row-bytes]
  (query-sql accepted plan bindings max-row-bytes nil nil []))

(defn snapshot-sql
  "Capture bounded individual values and exact size/presence markers. Keys must
  fit immediately; oversized payloads stay on the server and fail only if read."
  [accepted max-row-bytes]
  (let [columns (selected-columns accepted {:snapshot? true})
        cap (long max-row-bytes)]
    {:columns columns :snapshot? true :max-row-bytes cap
     :sql (str "SELECT (" (row-size (pm/key-columns accepted)) ") > " cap ", "
               (str/join ", "
                 (mapcat (fn [column]
                           (let [sql (column-sql column)
                                 invalid (invalid-sql (pm/storage-type (get-in accepted [:requirements :columns column])) sql)]
                             [(str "CASE WHEN " sql " IS NULL THEN -1 WHEN " invalid " THEN -2 ELSE " (text-size column) " END")
                              (str "CASE WHEN (" (text-size column) ") <= " cap " AND NOT (" invalid ") THEN " sql " END")])) columns))
               " FROM " (when (= "r" (get-in accepted [:requirements :kind])) "ONLY ")
               (jdbc/relation (get-in accepted [:requirements :table])) " AS r")
     :params []}))

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
  ([accepted projection rs] (read-row accepted projection rs 0))
  ([accepted {:keys [columns presence-columns snapshot? max-row-bytes]} ^ResultSet rs offset]
   (when (.getBoolean rs (inc offset)) (jdbc/fail! :query-source/limit :decode))
   (if snapshot?
     (let [[row sizes]
           (reduce (fn [[row sizes] [i column]]
                     (let [index (+ offset 2 (* 2 i)) size (.getLong rs (int index))]
                       (when (= -2 size) (jdbc/fail! :query-source/invalid :values))
                       [(assoc row column (cond (= -1 size) nil
                                                (> size max-row-bytes) ::oversized
                                                :else (decode accepted column rs (inc index))))
                        (assoc sizes column (max 0 size))])) [{} {}] (map-indexed vector columns))]
       (with-meta row {::sizes sizes ::max-row-bytes max-row-bytes}))
     (do
       (when (.getBoolean rs (+ offset 2)) (jdbc/fail! :query-source/invalid :values))
       (into {} (map-indexed (fn [i column]
                              [column (if (contains? presence-columns column)
                                        (when (.getBoolean rs (int (+ i offset 3))) true)
                                        (decode accepted column rs (+ i offset 3)))]) columns))))))

(defn- match-term [bound term x]
  (when bound
    (case (first term)
      :ignore bound
      :const (when (QueryKey/equiv (second term) x) bound)
      :var (let [sym (second term)]
             ;; Reusing a variable is a join, including within a fused row.
             (if (contains? bound sym)
               (when (QueryKey/equals (bound sym) x) bound)
               (assoc bound sym x))))))

(defn project
  "Export only live variables or variables needed by another access/residual.
  Preserve binding columns for the shared join operator. Physical decoding and
  conversion guards remain unchanged."
  [candidate request]
  (let [covered (set (map :id (:clauses candidate)))
        needed (into (or (:required-vars request) (set (map :variable (:columns candidate))))
                     (mapcat (fn [clause]
                               (keep #(when (= :var (first %)) (second %))
                                     (or (:terms clause) (:args clause)))))
                     (remove #(covered (:id %)) (:clauses request)))
        needed (into needed (map :variable) (:requires candidate))]
    (update candidate :columns #(filterv (comp needed :variable) %))))

(defn- reference-value [type make-reference term bindings values]
  (let [[bound? x] (operand bindings term)]
    (if bound?
      (if (and (ref/entity-ref? x) (value/matches-type? type x)
               (QueryKey/equals (ref/key-tuple x) values)) x ::mismatch)
      (if make-reference (make-reference values) (ref/entity-ref type values)))))

(defn- match-row* [accepted plan bindings row entity-reference references]
  (let [keys (mapv row (pm/key-columns accepted))
        observed (into (set (clojure.core/keys bindings)) (map :variable) (:columns plan))
        observed (into observed (keep (fn [clause]
                                       (let [[tag x] (nth (:terms clause) 2)] (when (= :var tag) x)))) (:clauses plan))
        entity (when (some (fn [clause]
                            (let [[tag x] (first (:terms clause))]
                              (or (= :const tag) (and (= :var tag) (observed x))))) (:clauses plan))
                 (reference-value (get-in accepted [:identity :type]) entity-reference
                                  (first (:terms (first (:clauses plan)))) bindings keys))
        matched
        (when-not (= ::mismatch entity)
        (reduce
          (fn [bound clause]
            (let [[entity-term _ term] (:terms clause)
                  bound (if entity (match-term bound entity-term entity) bound)]
              (when bound
                (let [attribute (second (second (:terms clause)))
                      raw (mapv row (pm/attribute-columns accepted attribute))]
                  (when (every? some? raw)
                    (if (= :ignore (first term)) bound
                      (do
                        (when (some #{::oversized} raw) (jdbc/fail! :query-source/limit :decode))
                        (let [x (if-let [type (get-in accepted [:attributes attribute :reference :type])]
                                  (reference-value type (references attribute) term bound raw)
                                  (if (= 1 (count raw)) (first raw) raw))]
                          (when-not (= ::mismatch x) (match-term bound term x)))))))))) bindings (:clauses plan)))]
    (when matched
      (when-let [sizes (::sizes (meta row))]
        (when (> (reduce + 0 (map sizes (value-columns accepted plan))) (::max-row-bytes (meta row)))
          (jdbc/fail! :query-source/limit :decode)))
      (mapv matched (map :variable (:columns plan))))))

(defn row-matcher
  "Prepare immutable reference metadata once per cursor. Bound nominal keys
  are checked against physical keys and reuse the already owned reference."
  [accepted plan]
  (let [known (into (set (keys (:bindings plan))) (map :variable) (:requires plan))
        observed (into (into known (map :variable) (:columns plan))
                       (keep (fn [clause] (let [[tag v] (nth (:terms clause) 2)] (when (= :var tag) v)))) (:clauses plan))
        [tag e] (first (:terms (first (:clauses plan))))
        entity-reference (when (and (= :var tag) (observed e) (not (known e)))
                           (ref/reference-constructor (get-in accepted [:identity :type])))
        references (into {} (keep (fn [clause]
                                    (let [attribute (second (second (:terms clause)))
                                          [tag v] (nth (:terms clause) 2)]
                                      (when-let [type (and (= :var tag) (not (known v))
                                                          (get-in accepted [:attributes attribute :reference :type]))]
                                        [attribute (ref/reference-constructor type)])))) (:clauses plan))]
    (fn [bindings row] (match-row* accepted plan bindings row entity-reference references))))

(defn match-row [accepted plan bindings row]
  ((row-matcher accepted plan) bindings row))
