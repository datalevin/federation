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
  (let [types (mapv #(get-in accepted [:requirements :columns % :type]) columns)]
    (require! (every? #{"int2" "int4" "int8" "bool" "uuid" "text" "varchar" "bytea" "numeric" "timestamptz"} types))
    {:sql (mapv #(text-column (str alias "." (jdbc/identifier %1)) %2) columns types)
     :types types :type type :shape shape
     :modifier (when (= 1 (count columns)) (get-in accepted [:requirements :columns (first columns) :modifier]))}))

(defn- equivalent [a b strict?]
  (require! (and (= (:type a) (:type b)) (= (:shape a) (:shape b)) (= (count (:sql a)) (count (:sql b)))))
  (conjunction
    (mapcat (fn [x y type other]
              (require! (= (pm/types type) (pm/types other)))
              (cond-> [(str x " = " y)]
                (and strict? (= type "numeric")) (conj (str "scale(" x ") = scale(" y ")"))))
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
                      [(str "(" (first filter) ") AND scale(" sql ") = " (.scale ^BigDecimal x)) (second filter)]
                      filter))) (:sql expr) (:types expr) xs)]
      [(conjunction (map first filters)) (vec (mapcat second filters))])))

(defn- predicate-filter [variables {:keys [operator args]}]
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
              [(str (first (:sql expr)) " " (plan/operators operator) " " (first (:sql other))) []]))))))

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
                        (or (#{"int2" "int4" "int8" "timestamptz"} type)
                            (and (= type "numeric") (integer? (:modifier expr))
                                 (not (neg? (long (:modifier expr))))))))
          {:sql [(str (name operator) "(" field ")")] :types [type] :shape :scalar :type :data})
      (unsupported!))))

(defn compile-query
  "Return a parameterized exact plan, or nil for a shape whose semantics have
  not been established. Views are accepted mappings indexed by source alias."
  [views request max-row-bytes]
  (try
    (require! (and (= :relational (:mode request)) (= 1 (:semantics-version request)) (seq (:find request))
                   (= (count views) (count (:sources request)))
                   (= (set (keys views)) (set (map :symbol (:sources request))))))
    (let [patterns (filterv #(= :pattern (:kind %)) (:clauses request))
          _ (require! (<= 1 (count patterns) 32))
          _ (require! (and (every? #(and (integer? (:id %)) (not (neg? (:id %)))) patterns)
                           (= (count patterns) (count (set (map :id patterns))))))
          predicates (filterv #(= :predicate (:kind %)) (:clauses request))
          _ (require! (= (count (:clauses request)) (+ (count patterns) (count predicates))))
          state
          (reduce
            (fn [{:keys [tables] :as state} {:keys [id source-symbol terms]}]
              (let [[entity [_ attribute] val] terms accepted (views source-symbol)
                    _ (require! (and accepted (= :const (first (second terms))) (= 3 (count terms))))
                    key [source-symbol (if (= :ignore (first entity)) [:occurrence id] entity)]
                    alias (or (get-in tables [key :alias]) (str "r" (count tables)))
                    columns (pm/attribute-columns accepted attribute)
                    _ (require! (seq columns))
                    attribute-type (if (= attribute (get-in accepted [:identity :attribute]))
                                     (if (= 1 (count columns)) (pm/types (get-in accepted [:requirements :columns (first columns) :type])) :data)
                                     (or (get-in accepted [:attributes attribute :reference :type])
                                         (get-in accepted [:attributes attribute :type])))
                    expressions [(expression accepted alias (pm/key-columns accepted) (get-in accepted [:identity :type]) :ref)
                                 (expression accepted alias columns attribute-type
                                   (cond (= :custom (:kind attribute-type)) :ref (> (count columns) 1) :tuple :else :scalar))]
                    table {:alias alias :accepted accepted
                           :columns (into (get-in tables [key :columns] #{}) (concat (pm/key-columns accepted) columns))}
                    state (-> state (assoc-in [:tables key] table)
                              (update :conditions into (map #(str alias "." (jdbc/identifier %) " IS NOT NULL") columns)))]
                (reduce (fn [state [term expr]]
                          (case (first term)
                            :ignore state
                            :const (let [[sql params] (literal expr (second term) false)]
                                     (-> state (update :conditions conj sql) (update :params into params)))
                            :var (let [v (second term) before (get-in state [:variables v])
                                       state (cond-> state before (update :conditions conj (equivalent before expr true)))
                                       state (assoc-in state [:variables v] (or before expr))]
                                   (if (contains? (:bindings request) v)
                                     (let [[sql params] (literal expr (get-in request [:bindings v]) true)]
                                       (-> state (update :conditions conj sql) (update :params into params))) state))
                            (unsupported!))) state (map vector [entity val] expressions))))
            {:tables (array-map) :variables {} :conditions [] :params []} patterns)
          variables (:variables state)
          filters (mapv #(predicate-filter variables %) predicates)
          conditions (into (:conditions state) (map first filters))
          params (into (:params state) (mapcat second) filters)
          _ (require! (<= (count params) 65535))
          tables (sort-by :alias (vals (:tables state)))
          columns (mapcat (fn [{:keys [alias accepted columns]}]
                            (map #(vector (str alias "." (jdbc/identifier %))
                                          (get-in accepted [:requirements :columns % :type])) columns)) tables)
          oversized (str/join " OR "
                      (map (fn [{:keys [alias columns]}]
                             (str "(" (str/join " + " (map #(str "COALESCE(octet_length(" alias "." (jdbc/identifier %)
                                                                 "::text)::bigint,0)") columns)) ") > " (long max-row-bytes))) tables))
          invalid (str/join " OR " (cons "FALSE"
                                    (keep (fn [[sql type]]
                                            (case type
                                              "numeric" (str sql "::text IN ('NaN','Infinity','-Infinity')")
                                              "timestamptz" (str "NOT isfinite(" sql ")") nil)) columns)))
          bad (str "(" oversized ") OR (" invalid ")")
          used (vec (distinct (concat (map :variable (:find request)) (:with request))))
          _ (require! (every? variables used))
          fields (into {} (map-indexed (fn [i v] [v (mapv #(str "v" i "_" %) (range (count (:sql (variables v)))))])) used)
          scale-fields (into {} (keep-indexed (fn [i v] (when (= ["numeric"] (:types (variables v))) [v (str "d" i)]))) used)
          distinct-select (concat
                            (mapcat (fn [v]
                                      (map #(str "CASE WHEN " bad " THEN NULL ELSE " %1 " END AS " %2)
                                           (:sql (variables v)) (fields v))) used)
                            (map (fn [[v name]] (str "CASE WHEN " bad " THEN NULL ELSE scale("
                                                   (first (:sql (variables v))) ") END AS " name)) scale-fields)
                            [(str "(" oversized ") AS oversized") (str "(" invalid ") AS invalid")])
          aggregate? (some #(= :aggregate (:kind %)) (:find request))
          outputs (mapv (fn [{:keys [kind variable] :as find}]
                          (if (= kind :variable) (assoc (variables variable) :sql (fields variable))
                              (aggregate find (variables variable) (fields variable) predicates))) (:find request))
          _ (require! (and (vector? (:columns request)) (= (count outputs) (count (:columns request)))
                           (every? true?
                             (map (fn [find output column]
                                    (and (= (:type column) (if (= :variable (:kind find)) (:type output)
                                                               (if (#{:count :count-distinct} (:operator find)) :long :data)))
                                         (= (:nullable? column) (boolean (#{:min :max} (:operator find))))))
                                  (:find request) outputs (:columns request)))))
          output-fields (mapv #(str "o" %) (range (reduce + (map #(count (:sql %)) outputs))))
          _ (require! (<= (count output-fields) 128))
          output-expressions (mapcat :sql outputs)
          grouping (vec (distinct (mapcat (fn [{:keys [kind variable]}]
                                          (when (= :variable kind) (cond-> (fields variable)
                                                                    (scale-fields variable) (conj (scale-fields variable))))) (:find request))))
          from (str/join " CROSS JOIN " (map #(str "ONLY " (jdbc/relation (get-in % [:accepted :requirements :table])) " AS " (:alias %)) tables))
          matched (str "SELECT DISTINCT " (str/join ", " distinct-select) " FROM " from " WHERE " (conjunction conditions))
          result (str "SELECT " (str/join ", " (map #(str %1 " AS " %2) output-expressions output-fields))
                      ", " (if aggregate? "bool_or(oversized)" "oversized") " AS oversized, "
                      (if aggregate? "bool_or(invalid)" "invalid") " AS invalid FROM matched"
                      (when (and aggregate? (seq grouping)) (str " GROUP BY " (str/join ", " grouping)))
                      (when aggregate? " HAVING count(*) > 0"))
          size (str/join " + " (map #(str "COALESCE(octet_length(" % "::text)::bigint,0)") output-fields))
          too-large (str "oversized OR (" size ") > " (long max-row-bytes))
          safe (str "NOT (" too-large ") AND NOT invalid")]
      {:sql (str "WITH matched AS (" matched "), result AS (" result ") SELECT " too-large ", invalid, "
                 (str/join ", " (map #(str "CASE WHEN " safe " THEN " % " END") output-fields)) " FROM result")
       :params params :layout (mapv #(dissoc % :sql :modifier) outputs) :columns (:columns request)
       :physical-columns (count output-fields)})
    (catch clojure.lang.ExceptionInfo e (if (::unsupported (ex-data e)) nil (throw e)))))

(defn read-row [query ^ResultSet rs]
  (when (.getBoolean rs 1) (jdbc/fail! :query-source/limit :decode))
  (when (.getBoolean rs 2) (jdbc/fail! :query-source/invalid :decode))
  (first
    (reduce (fn [[out index] {:keys [shape type types count?]}]
              (let [values (mapv #(plan/decode-value %1 rs %2) types (range index (+ index (count types))))
                    x (case shape :ref (ref/entity-ref type values) :tuple values :scalar (first values))
                    ;; clojure.core/count returns a boxed Integer for ordinary
                    ;; collections. Preserve that representation in return maps.
                    x (if (and count? (<= (long x) Integer/MAX_VALUE)) (int x) x)]
                [(conj out x) (+ index (count types))])) [[] 3] (:layout query))))
