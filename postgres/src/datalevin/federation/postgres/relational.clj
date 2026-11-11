(ns datalevin.federation.postgres.relational
  "Exact SQL delegation for flat, single-snapshot joins and aggregates."
  (:require [clojure.string :as str]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.query.source.value :as value])
  (:import [java.math BigDecimal]
           [java.nio.charset StandardCharsets]
           [java.sql ResultSet]))

(defn- unsupported! [] (throw (ex-info "Relational shape requires core execution" {::unsupported true})))
(defn- require! [ok] (when-not ok (unsupported!)))
(defn- conjunction [xs] (if (seq xs) (str/join " AND " (map #(str "(" % ")") xs)) "TRUE"))
(defn- text-column [sql type] (if (#{"text" "varchar"} type) (str sql " COLLATE pg_catalog.\"C\"") sql))
(defn- text-value? [x]
  (and (string? x) (not (str/includes? x "\u0000")) (.canEncode (.newEncoder StandardCharsets/UTF_8) ^String x)))

(defn- expression [accepted alias columns type shape]
  (let [types (mapv #(pm/storage-type (get-in accepted [:requirements :columns %])) columns)]
    (require! (every? #{"int2" "int4" "int8" "bool" "uuid" "text" "varchar" "bytea" "numeric" "timestamptz"} types))
    {:sql (mapv #(text-column (str alias "." (jdbc/identifier %1)) %2) columns types)
     :raw-sql (mapv #(str alias "." (jdbc/identifier %)) columns)
     :types types :type type :shape shape :aliases #{alias}
     :modifier (when (= 1 (count columns)) (get-in accepted [:requirements :columns (first columns) :modifier]))}))

(defn- equivalent [a b strict?]
  (require! (and (= (:type a) (:type b)) (= (:shape a) (:shape b)) (= (count (:sql a)) (count (:sql b)))))
  (conjunction
    (mapcat (fn [x y type other]
              (require! (= (pm/types type) (pm/types other)))
              (cond-> [(str x " = " y)]
                (and strict? (= type "numeric")) (conj (str (or (get (:scales a) (.indexOf ^java.util.List (:sql a) x)) (str "scale(" x ")"))
                                                                   " = " (or (get (:scales b) (.indexOf ^java.util.List (:sql b) y)) (str "scale(" y ")"))))))
            (:sql a) (:sql b) (:types a) (:types b))))

(defn- literal [expr x strict?]
  (let [xs (case (:shape expr)
             :ref (do (require! (and (ref/entity-ref? x) (value/matches-type? (:type expr) x))) (ref/key-tuple x))
             :tuple (do (require! (vector? x)) x)
             :scalar [x])]
    (require! (= (count xs) (count (:sql expr))))
    (let [filters
          (mapv (fn [sql type x]
                  (let [filter (or (plan/comparison-filter type sql := x)
                                   (when (case type
                                           ("text" "varchar") (text-value? x)
                                           "uuid" (uuid? x) "bool" (boolean? x) "bytea" (bytes? x) false)
                                     [(str sql " = ?::" type) [x]]))]
                    (require! filter)
                    (if (and strict? (= type "numeric"))
                      [(str "(" (first filter) ") AND " (or (first (:scales expr)) (str "scale(" sql ")")) " = " (.scale ^BigDecimal x)) (second filter)]
                      filter))) (:sql expr) (:types expr) xs)]
      [(conjunction (map first filters)) (vec (mapcat second filters))])))

(defn- predicate-filter [variables {:keys [operator args]}]
  (cond
    (= :like-any operator)
    (let [[[tag v] [other patterns]] args expr (variables v)]
      (require! (and (= 2 (count args)) (= tag :var) (= other :const)
                     (= :scalar (:shape expr)) (= 1 (count (:sql expr)))))
      (or (plan/like-any-filter (first (:types expr)) (str "(" (first (:sql expr)) ")") patterns)
          (unsupported!)))

    (#{:like :not-like} operator)
    (let [[[tag v] [other pattern]] args expr (variables v)]
      (require! (and (= 2 (count args)) (= tag :var) (= other :const)
                     (= :scalar (:shape expr)) (= 1 (count (:sql expr)))))
      (or (plan/like-filter (first (:types expr)) (str "(" (first (:sql expr)) ")") operator pattern)
          (unsupported!)))

    (#{:in :not-in} operator)
    (let [[[tag v] [other xs]] args expr (variables v)]
      (require! (and (= tag :var) (= other :const) (= :scalar (:shape expr))
                     (#{"text" "varchar"} (first (:types expr)))
                     (or (sequential? xs) (set? xs)) (<= (count xs) 256)
                     (every? #(or (nil? %) (text-value? %)) xs)))
      (let [xs (vec (sort (distinct (remove nil? xs))))]
        [(if (empty? xs) (if (= operator :in) "FALSE" "TRUE")
             (str (first (:sql expr)) (if (= operator :in) " IN (" " NOT IN (")
                  (str/join "," (repeat (count xs) "?::text")) ")")) xs]))
  :else
  (let [[a b] args
        [a b operator] (if (= :const (first a)) [b a (plan/reverse-operator operator)] [a b operator])
        expr (variables (second a))]
    (require! (and (= :var (first a)) expr (plan/operators operator)))
    (if (= :const (first b))
      (if (#{:= :not=} operator)
        (let [[sql params] (literal expr (second b) false)]
          [(if (= operator :not=) (str "NOT (" sql ")") sql) params])
        (do (require! (= 1 (count (:sql expr))))
            (or (plan/comparison-filter (first (:types expr)) (first (:sql expr)) operator (second b))
                (unsupported!))))
      (let [other (variables (second b))]
        (require! (and (= :var (first b)) other))
        (if (#{:= :not=} operator)
          (let [sql (equivalent expr other false)] [(if (= operator :not=) (str "NOT (" sql ")") sql) []])
          (do (require! (and (= (:type expr) (:type other)) (= 1 (count (:sql expr)))
                            (every? #{"int2" "int4" "int8" "numeric" "timestamptz"}
                                    (concat (:types expr) (:types other)))))
              [(str (first (:sql expr)) " " (plan/operators operator) " " (first (:sql other))) []])))))))

(defn- condition-filter [variables clause]
  (let [remaining (volatile! 128)]
    (letfn [(emit [{:keys [kind operator clauses] :as clause} depth]
              (require! (and (<= depth 16) (not (neg? (vswap! remaining dec)))))
              (if (= :boolean kind)
                (do (require! (and (#{:and :or} operator) (vector? clauses)
                                   (<= (count clauses) (if (= :or operator) 32 128))))
                    (let [filters (mapv #(emit % (inc depth)) clauses)]
                      [(if (empty? filters) (if (= :and operator) "TRUE" "FALSE")
                         (str/join (if (= :and operator) " AND " " OR ")
                                   (map #(str "(" (first %) ")") filters)))
                       (vec (mapcat second filters))]))
                (do (require! (= :predicate kind)) (predicate-filter variables clause))))]
      (emit clause 0))))

(defn- nonnegative? [variable predicates]
  (some (fn [{:keys [operator args]}]
          (let [[a b] args
                [a b op] (if (= :const (first a)) [b a (plan/reverse-operator operator)] [a b operator])]
            (and (= [:var variable] a) (= :const (first b)) (int? (second b))
                 (#{:> :>=} op) (not (neg? (second b)))))) predicates))

(defn- aggregate [find expr fields predicates]
  (let [{:keys [operator variable]} find
        type (first (:types expr)) field (first fields)]
    (case operator
      :count {:sql ["count(*)"] :types ["int8"] :shape :scalar :type :long :count? true}
      :count-distinct {:sql [(str "count(DISTINCT " (if (= 1 (count fields)) field
                                                     (str "ROW(" (str/join ", " fields) ")")) ")")]
                       :types ["int8"] :shape :scalar :type :long :count? true}
      :sum (do (require! (and (= 1 (count fields))
                              (or (= type "numeric")
                                  (and (#{"int2" "int4" "int8"} type) (nonnegative? variable predicates)))))
               {:sql [(str "sum(" field ")" (when (not= type "numeric") "::int8"))]
                :types [(if (= type "numeric") "numeric" "int8")] :shape :scalar :type :data})
      (:min :max)
      (do (require! (and (= 1 (count fields))
                        (or (and (:native-extremum? expr) (#{"text" "varchar"} type))
                            (#{"int2" "int4" "int8" "timestamptz"} type)
                            (and (= type "numeric") (integer? (:modifier expr))
                                 (not (neg? (long (:modifier expr))))))))
          {:sql [(str (name operator) "(" field ")")] :types [type] :shape :scalar :type :data})
      (unsupported!))))

(def ^:private input-types {:long "int8" :string "text" :bigdec "numeric" :uuid "uuid" :boolean "bool" :bytes "bytea"})

(defn- input-expression [column i]
  (let [type (:type column) nominal? (and (map? type) (= :custom (:kind type)))
        _ (when nominal? (require! (= ref/type-name (:name type))))
        key-types (if nominal?
                    (try (get-in (ref/validate-type! type) [:parameters :key-types])
                         (catch Exception _ (unsupported!))) [type])
        types (mapv input-types key-types)
        _ (require! (and (every? some? types) (or (not nominal?) (not-any? #{:bigdec} key-types))))
        names (mapv #(str "k" i "_" %) (range (count types)))]
    (cond-> {:sql (mapv text-column (map #(str "b." %) names) types)
             :types types :type type :shape (if nominal? :ref :scalar)
             :input-names names :key-types key-types}
      (= type :bigdec) (assoc :scales [(str "b.s" i)] :scale-name (str "s" i)))))

(defn- default-input [type]
  (case type :long 0 :string "" :bigdec 0M :uuid (java.util.UUID. 0 0) :boolean false :bytes (byte-array 0)))

(defn- valid-input? [column expression x]
  (and (value/matches-type? (:type column) x)
       (every? true?
         (map (fn [type x]
                (case type
                  :string (text-value? x)
                  :bigdec (boolean (plan/comparison-filter "numeric" "x" := x))
                  true))
              (:key-types expression)
              (if (= :ref (:shape expression)) (ref/key-tuple x) [x])))))

(defn- input-relation [request]
  (when-let [columns (:inputs request)]
    (require! (and (= :relational (:mode request)) (vector? columns) (seq columns)
                   (every? #(false? (:nullable? %)) columns)))
    (let [expressions (mapv input-expression columns (range))
          bindings (or (:input-bindings request)
                       [{:binding-id 0 :values
                         (mapv (fn [{:keys [type shape key-types]}]
                                 (if (= shape :ref) (ref/entity-ref type (mapv default-input key-types))
                                     (default-input type))) expressions)}])
          _ (require! (<= 1 (count bindings) 256))
          params (atom [])
          names (mapcat #(cond-> (:input-names %) (:scale-name %) (conj (:scale-name %))) expressions)
          rows (mapv
                 (fn [{:keys [values]}]
                   (require! (= (count values) (count columns)))
                   (let [valid? (every? true? (map valid-input? columns expressions values))
                         fields (mapcat
                                  (fn [{:keys [shape types scale-name key-types]} x]
                                    (let [xs (if valid? (if (= shape :ref) (ref/key-tuple x) [x])
                                                 (mapv (constantly nil) key-types))]
                                      (doseq [x xs] (swap! params conj x))
                                      (cond-> (mapv #(str "?::" %) types)
                                        scale-name (conj (str (if valid? (.scale ^BigDecimal x) 0)))))) expressions values)]
                     (str "(" (if valid? "TRUE" "FALSE") "," (str/join "," fields) ")"))) bindings)]
      {:sql (str "(VALUES " (str/join "," rows) ") AS b(ok," (str/join "," names) ")")
       :params @params
       :variables (into {} (map (fn [c expr] [(:variable c) (assoc (dissoc expr :input-names :key-types :scale-name) :input? true)]) columns expressions))})))

(defn- window-sql [window outputs output-fields max-row-bytes]
  (let [{:keys [order limit offset]} window
        _ (require! (and (= #{:order :limit :offset} (set (keys window))) (vector? order)
                         (integer? limit) (<= 1 limit Long/MAX_VALUE)
                         (integer? offset) (<= 0 offset Long/MAX_VALUE)))
        fields (mapv (fn [[start output]] (subvec output-fields start (+ start (count (:types output)))))
                     (map vector (reductions + 0 (map #(count (:types %)) outputs)) outputs))
        requested (mapcat (fn [{:keys [column direction] :as entry}]
                            (require! (and (= #{:column :direction} (set (keys entry)))
                                           (integer? column) (<= 0 column) (< column (count outputs))
                                           (#{:asc :desc} direction)))
                            (let [output (nth outputs column)]
                              ;; Java String order uses UTF-16, UUID order uses
                              ;; signed halves, and nominal references have an
                              ;; encoded comparator. Leave those orders in core.
                              (require! (and (= :scalar (:shape output))
                                             (#{"int2" "int4" "int8" "numeric" "bool" "timestamptz"}
                                               (first (:types output)))))
                              [(str (first (nth fields column)) (if (= :asc direction) " ASC NULLS FIRST" " DESC NULLS LAST"))])) order)
        _ (require! (every? #(or (not= :ref (:shape %))
                                 (every? #{"int2" "int4" "int8" "uuid" "timestamptz"} (:types %))) outputs))
        scales (vec (keep (fn [[field type]] (when (= type "numeric") (str "scale(" field ")")))
                          (map vector output-fields (mapcat :types outputs))))
        scale-names (mapv #(str "w" %) (range (count scales)))
        projection (concat output-fields (map #(str %1 " AS " %2) scales scale-names))
        size (str/join " + " (map #(str "COALESCE(octet_length(" % "::text)::bigint,0)") output-fields))
        overflow (keep (fn [[field type]] (when (= type "int8")
                                          (str field " < -9223372036854775808::numeric OR " field " > 9223372036854775807::numeric")))
                       (map vector output-fields (mapcat :types outputs)))
        invalid (str/join " OR " (cons "invalid" overflow))
        flags ["g.oversized" "g.invalid"]
        safe "NOT g.oversized AND NOT g.invalid"
        ;; Validate the full result, including conversion/size failures outside
        ;; the selected window. PostgreSQL owns sorting/materialization/spill;
        ;; only the final distinct window crosses the adapter boundary.
        sql (str ", window_guard AS (SELECT COALESCE(bool_or(oversized OR (" size ") > " (long max-row-bytes)
                 "),FALSE) AS oversized, COALESCE(bool_or(" invalid "),FALSE) AS invalid FROM result), "
                 "window_rows AS (SELECT DISTINCT " (str/join ", " projection) " FROM result), "
                 "selected AS (SELECT * FROM window_rows ORDER BY "
                 (str/join ", " (concat requested output-fields scale-names)) " LIMIT ?::bigint OFFSET ?::bigint) SELECT "
                 (str/join ", " (concat flags (map #(str "CASE WHEN " safe " THEN " % " END") output-fields)))
                 " FROM selected CROSS JOIN window_guard AS g UNION ALL SELECT "
                 (str/join ", " (concat flags (repeat (count output-fields) "NULL")))
                 " FROM window_guard AS g WHERE (g.oversized OR g.invalid) AND NOT EXISTS (SELECT 1 FROM selected)")]
    {:sql sql :params [(long limit) (long offset)]}))

(defn- dependent-from
  "Keep each dependent lookup behind the keys produced by its predecessors.
  OFFSET 0 is a PostgreSQL planning fence; all original outer checks remain.
  Edges contain only variable equalities, so moving copies adds no parameters."
  [tables input edges]
  (require! input)
  (loop [pending (vec tables) known #{} out [(:sql input)]]
    (if (empty? pending) (str/join " CROSS JOIN " out)
      (let [ready? (fn [{:keys [alias]}]
                     (some #(and ((:aliases %) alias)
                                 (every? (conj known alias) (:aliases %))
                                 (or (:input? %) (seq (filter known (:aliases %))))) edges))
            table (first (filter ready? pending))]
        (require! table)
        (let [{:keys [alias accepted columns]} table
              available (conj known alias)
              conditions (map :sql (filter #(and ((:aliases %) alias) (every? available (:aliases %))) edges))
              sql (str "LATERAL (SELECT " (str/join ", " (map #(str alias "." (jdbc/identifier %)) (sort columns)))
                       " FROM ONLY " (jdbc/relation (get-in accepted [:requirements :table])) " AS " alias
                       " WHERE " (conjunction conditions) " OFFSET 0) AS " alias)]
          (recur (filterv #(not= alias (:alias %)) pending) available (conj out sql)))))))

(defn compile-query
  "Return a parameterized exact plan, or nil for a shape whose semantics have
  not been established. Views are accepted mappings indexed by source alias."
  [views request max-row-bytes]
  (try
    (require! (not-any? pm/spooled? (vals views)))
    (require! (and (= :relational (:mode request)) (= 1 (:semantics-version request)) (seq (:find request))
                   (= (count views) (count (:sources request)))
                   (= (set (keys views)) (set (map :symbol (:sources request))))))
    (let [input (input-relation request)
          required-patterns (filterv #(= :pattern (:kind %)) (:clauses request))
          optional-patterns (filterv #(= :optional (:kind %)) (:clauses request))
          patterns (into required-patterns optional-patterns)
          _ (require! (<= 1 (count patterns) 32))
          _ (require! (and (every? #(and (integer? (:id %)) (not (neg? (:id %)))) patterns)
                           (= (count patterns) (count (set (map :id patterns))))))
          predicates (filterv #(#{:predicate :boolean} (:kind %)) (:clauses request))
          absences (filterv #(= :absence (:kind %)) (:clauses request))
          _ (require! (= (count (:clauses request)) (+ (count patterns) (count predicates) (count absences))))
          native-specs (:datalevin.query.source/native-extrema request)
          source-order (zipmap (map :symbol (:sources request)) (range))
          state
          (reduce
            (fn [{:keys [tables] :as state} {:keys [id kind source-symbol terms]}]
              (let [optional? (= :optional kind)
                    [entity [_ attribute] val] terms accepted (views source-symbol)
                    _ (require! (and accepted (= :const (first (second terms))) (= 3 (count terms))))
                    key [source-symbol (if (= :ignore (first entity)) [:occurrence id] entity)]
                    alias (or (get-in tables [key :alias]) (str "r" (count tables)))
                    columns (pm/attribute-columns accepted attribute)
                    _ (require! (and (seq columns)
                                     (or (not optional?)
                                         (and (contains? tables key) (= 1 (count columns))
                                              (= :var (first val))
                                              (nil? (get-in accepted [:attributes attribute :reference]))))))
                    attribute-type (if (= attribute (get-in accepted [:identity :attribute]))
                                     (if (= 1 (count columns)) (pm/types (get-in accepted [:requirements :columns (first columns) :type])) :data)
                                     (or (get-in accepted [:attributes attribute :reference :type])
                                         (get-in accepted [:attributes attribute :type])))
                    expressions [(expression accepted alias (pm/key-columns accepted) (get-in accepted [:identity :type]) :ref)
                                 (assoc (expression accepted alias columns attribute-type
                                          (cond (= :custom (:kind attribute-type)) :ref (> (count columns) 1) :tuple :else :scalar))
                                        :nullable? optional?)]
                    table {:alias alias
                           :ordinal (get-in tables [key :ordinal] [(source-order source-symbol) (count tables)])
                           :accepted accepted
                           :columns (into (get-in tables [key :columns] #{}) (concat (pm/key-columns accepted) columns))}
                    native-spec (when (= :var (first val)) (get native-specs (second val)))
                    state (if (and native-spec (= source-symbol (:source native-spec))
                                   (= attribute (:attribute native-spec)))
                            (do (require! (and (= 1 (count columns))
                                                (#{"text" "varchar"} (first (:types (second expressions))))))
                                (assoc-in state [:native-expressions (second val)]
                                  {:sql (:raw-sql (second expressions))
                                   :collation (get-in accepted [:requirements :columns (first columns) :collation])})) state)
                    state (cond-> (assoc-in state [:tables key] table)
                            (not optional?)
                            (update :conditions into (map #(str alias "." (jdbc/identifier %) " IS NOT NULL") columns)))]
                (reduce (fn [state [term expr]]
                          (case (first term)
                            :ignore state
                            :const (let [[sql params] (literal expr (second term) false)]
                                     (-> state (update :conditions conj sql) (update :params into params)))
                            :var (let [v (second term) before (get-in state [:variables v])
                                       state (if before
                                               (let [sql (equivalent before expr true)]
                                                 (-> state (update :conditions conj sql)
                                                     (update :edges conj {:sql sql
                                                                          :aliases (into (:aliases expr #{}) (:aliases before))
                                                                          :input? (boolean (:input? before))}))) state)
                                       state (assoc-in state [:variables v] (or before expr))]
                                   (if (contains? (:bindings request) v)
                                     (let [[sql params] (literal expr (get-in request [:bindings v]) true)]
                                       (-> state (update :conditions conj sql) (update :params into params))) state))
                            (unsupported!))) state (map vector [entity val] expressions))))
            {:tables (array-map) :variables (:variables input {}) :conditions (if input ["b.ok"] []) :params (:params input [])} patterns)
          state
          (reduce (fn [state {:keys [source-symbol terms]}]
                    (let [[entity [tag attribute] val] terms
                          key [source-symbol entity]
                          {:keys [alias accepted]} (get-in state [:tables key])
                          columns (when accepted (pm/attribute-columns accepted attribute))]
                      ;; Scalar NULL is precisely the accepted mapping's absent fact.
                      ;; Composite mappings remain with the complete core lookup.
                      (require! (and alias (= :const tag) (= [:ignore] val)
                                     (= 3 (count terms)) (= 1 (count columns))))
                      (-> state
                          (update-in [:tables key :columns] into columns)
                          (update :conditions conj (str alias "." (jdbc/identifier (first columns)) " IS NULL")))))
                  state absences)
          variables (:variables state)
          _ (doseq [{:keys [kind variable operator]} (:find request)
                    :when (and (= :aggregate kind) (:nullable? (variables variable)))]
              (require! (and (#{:min :max} operator)
                             (contains? (:datalevin.query.source/ignore-nil-extrema request) variable))))
          native-expressions (get state :native-expressions {})
          _ (require! (every? #(contains? native-expressions %) (keys native-specs)))
          filters (mapv #(condition-filter variables %) predicates)
          conditions (into (:conditions state) (map first filters))
          params (into (:params state) (mapcat second) filters)
          _ (require! (<= (count params) 65535))
          ;; Preserve declaration order beyond ten aliases: lexical r10/r2
          ;; ordering changes PostgreSQL's bounded join search and GEQO input.
          tables (sort-by :ordinal (vals (:tables state)))
          columns (mapcat (fn [{:keys [alias accepted columns]}]
                            (map #(vector (str alias "." (jdbc/identifier %))
                                          (get-in accepted [:requirements :columns % :type])) columns)) tables)
          ;; Backend-only intermediate values do not consume the wire row cap.
          oversized "FALSE"
          collation-guards (map (fn [{:keys [collation]}]
                                  (require! (and (integer? collation) (pos? collation)))
                                  (str "NOT COALESCE((SELECT collisdeterministic FROM pg_catalog.pg_collation WHERE oid="
                                       collation "::oid), FALSE)")) (vals native-expressions))
          invalid (str/join " OR " (concat collation-guards (cons "FALSE"
                                    (keep (fn [[sql type]]
                                            (case type
                                              "numeric" (str sql "::text IN ('NaN','Infinity','-Infinity')")
                                              "timestamptz" (str "NOT isfinite(" sql ")") nil)) columns))))
          invalid (str "COALESCE((" invalid "),FALSE)")
          bad (str "(" oversized ") OR (" invalid ")")
          used (vec (distinct (concat (map :variable (:find request)) (:with request))))
          _ (require! (every? variables used))
          fields (into {} (map-indexed (fn [i v] [v (mapv #(str "v" i "_" %) (range (count (:sql (variables v)))))])) used)
          scale-fields (into {} (keep-indexed (fn [i v] (when (= ["numeric"] (:types (variables v))) [v (str "d" i)]))) used)
          distinct-select (concat
                            (mapcat (fn [v]
                                      (map #(str "CASE WHEN " bad " THEN NULL ELSE " %1 " END AS " %2)
                                           (or (:sql (native-expressions v)) (:sql (variables v))) (fields v))) used)
                            (map (fn [[v name]] (str "CASE WHEN " bad " THEN NULL ELSE "
                                                   (or (first (:scales (variables v))) (str "scale(" (first (:sql (variables v))) ")")) " END AS " name)) scale-fields)
                            [(str "(" oversized ") AS oversized") (str "(" invalid ") AS invalid")])
          aggregate? (some #(= :aggregate (:kind %)) (:find request))
          outputs (mapv (fn [{:keys [kind variable] :as find}]
                          (if (= kind :variable) (assoc (variables variable) :sql (fields variable))
                              (aggregate find (cond-> (variables variable) (contains? native-expressions variable)
                                                          (assoc :native-extremum? true)) (fields variable) predicates))) (:find request))
          _ (require! (and (vector? (:columns request)) (= (count outputs) (count (:columns request)))
                           (every? true?
                             (map (fn [find output column]
                                    (and (= (:type column) (if (= :variable (:kind find)) (:type output)
                                                               (if (#{:count :count-distinct} (:operator find)) :long :data)))
                                         (= (:nullable? column) (boolean (or (#{:min :max} (:operator find))
                                                                           (and (= :variable (:kind find)) (:nullable? output)))))))
                                  (:find request) outputs (:columns request)))))
          output-fields (mapv #(str "o" %) (range (reduce + (map #(count (:sql %)) outputs))))
          _ (require! (<= (count output-fields) 128))
          output-expressions (mapcat :sql outputs)
          grouping (vec (distinct (mapcat (fn [{:keys [kind variable]}]
                                          (when (= :variable kind) (cond-> (fields variable)
                                                                    (scale-fields variable) (conj (scale-fields variable))))) (:find request))))
          from (if (:dependent-order? request) (dependent-from tables input (:edges state))
                   ;; Leave the full graph to PostgreSQL's join search. A CROSS
                   ;; JOIN tree can constrain graphs above join_collapse_limit.
                   (str/join ", " (cond-> (mapv #(str "ONLY " (jdbc/relation (get-in % [:accepted :requirements :table])) " AS " (:alias %)) tables)
                                             input (conj (:sql input)))))
          ;; MIN/MAX and the validation flags are insensitive to duplicates.
          ;; Let PostgreSQL aggregate the join directly instead of building a
          ;; distinct intermediate relation. Other reducers keep Datalog's
          ;; distinct find/with witness semantics.
          extrema-only? (every? #(and (= :aggregate (:kind %)) (#{:min :max} (:operator %))) (:find request))
          matched (str "SELECT " (when-not extrema-only? "DISTINCT ")
                       (str/join ", " distinct-select) " FROM " from " WHERE " (conjunction conditions))
          result (str "SELECT " (str/join ", " (map #(str %1 " AS " %2) output-expressions output-fields))
                      ", " (if aggregate? "bool_or(oversized)" "oversized") " AS oversized, "
                      (if aggregate? "bool_or(invalid)" "invalid") " AS invalid FROM matched"
                      (when (and aggregate? (seq grouping)) (str " GROUP BY " (str/join ", " grouping)))
                      (when aggregate? " HAVING count(*) > 0"))
          size (str/join " + " (map #(str "COALESCE(octet_length(" % "::text)::bigint,0)") output-fields))
          too-large (str "oversized OR (" size ") > " (long max-row-bytes))
          safe (str "NOT (" too-large ") AND NOT invalid")
          window (when (:window request) (window-sql (:window request) outputs output-fields max-row-bytes))]
      {:sql (str "WITH matched AS (" matched "), result AS (" result ")"
                 (if window (:sql window)
                   (str " SELECT " too-large ", invalid, "
                        (str/join ", " (map #(str "CASE WHEN " safe " THEN " % " END") output-fields)) " FROM result")))
       :params (into params (:params window)) :layout (mapv #(dissoc % :sql :modifier) outputs) :columns (:columns request)
       :physical-columns (count output-fields)
       :max-rows (when (and aggregate? (empty? grouping) (nil? window)) 1)})
    (catch clojure.lang.ExceptionInfo e (if (::unsupported (ex-data e)) nil (throw e)))))

(defn compile-fragment [views request max-row-bytes]
  (when (= :relational-fragment (:mode request))
    (compile-query views (assoc request :mode :relational) max-row-bytes)))

(defn compile-window [views request max-row-bytes]
  (when (and (= :relational-window (:mode request)) (:window request) (not (:inputs request)))
    (compile-query views (assoc request :mode :relational) max-row-bytes)))

(defn row-reader
  "Prepare cursor-local decoders. Constructors are runtime state and never
  enter the portable SQL plan or its captured metadata."
  [query]
  (let [layout (mapv (fn [column]
                       (cond-> column (= :ref (:shape column))
                         (assoc ::reference (ref/reference-constructor (:type column))))) (:layout query))]
  (fn [^ResultSet rs]
  (when (.getBoolean rs 1) (jdbc/fail! :query-source/limit :decode))
  (when (.getBoolean rs 2) (jdbc/fail! :query-source/invalid :decode))
  (first
    (reduce (fn [[out index] {:keys [shape types count?] :as column}]
              (let [values (mapv #(plan/decode-value %1 rs %2) types (range index (+ index (count types))))
                    x (case shape :ref ((::reference column) values) :tuple values :scalar (first values))
                    ;; clojure.core/count returns a boxed Integer for ordinary
                    ;; collections. Preserve that representation in return maps.
                    x (if (and count? (<= (long x) Integer/MAX_VALUE)) (int x) x)]
                [(conj out x) (+ index (count types))])) [[] 3] layout)))))

(defn read-row [query rs] ((row-reader query) rs))
