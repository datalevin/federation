(ns datalevin.federation.postgres-job-sql
  "A deliberately restricted translator for the original flat JOB SQL corpus.
  Rejects SQL whose NULL semantics cannot be represented by mapped EAV facts."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.mapping :as pm])
  (:import [java.util UUID]))

(def references
  {"aka_name" {"person_id" "name"}
   "aka_title" {"movie_id" "title" "kind_id" "kind_type" "episode_of_id" "title"}
   "cast_info" {"person_id" "name" "movie_id" "title" "person_role_id" "char_name" "role_id" "role_type"}
   "complete_cast" {"movie_id" "title" "subject_id" "comp_cast_type" "status_id" "comp_cast_type"}
   "movie_companies" {"movie_id" "title" "company_id" "company_name" "company_type_id" "company_type"}
   "movie_info" {"movie_id" "title" "info_type_id" "info_type"}
   "movie_info_idx" {"movie_id" "title" "info_type_id" "info_type"}
   "movie_keyword" {"movie_id" "title" "keyword_id" "keyword"}
   "movie_link" {"movie_id" "title" "linked_movie_id" "title" "link_type_id" "link_type"}
   "person_info" {"person_id" "name" "info_type_id" "info_type"}
   "title" {"kind_id" "kind_type" "episode_of_id" "title"}})

(defn- unsupported! [reason]
  (throw (ex-info "JOB SQL translation is unsupported" {:error :benchmark/unsupported :reason reason})))

(defn- tokens [s]
  (let [m (re-matcher #"\G\s*('(?:''|[^'])*'|[A-Za-z_][A-Za-z_0-9]*|-?\d+(?:\.\d+)?|!=|<>|<=|>=|[=<>(),.])" s)]
    (loop [end 0 out []]
      (if (.find m)
        (let [t (.group m 1)]
          (recur (.end m)
                 (conj out (cond
                             (str/starts-with? t "'") [:lit (str/replace (subs t 1 (dec (count t))) "''" "'")]
                             (re-matches #"-?\d+(?:\.\d+)?" t) [:lit (if (str/includes? t ".") (Double/parseDouble t) (Long/parseLong t))]
                             :else (str/lower-case t)))))
        (do (when-not (str/blank? (subs s end)) (unsupported! :token)) out)))))

(defn- parse-where [s]
  (let [ts (tokens s) pos (volatile! 0)
        peek! #(get ts @pos)
        pop! #(let [t (peek!)] (vswap! pos inc) t)
        take! #(when (= % (peek!)) (pop!) true)
        expect! #(when-not (take! %) (unsupported! :syntax))]
    (letfn [(operand! []
              (let [t (pop!)]
                (if (vector? t) t
                  (do (when-not (and (string? t) (re-matches #"[a-z_][a-z_0-9]*" t)) (unsupported! :operand))
                      (expect! ".")
                      (let [c (pop!)]
                        (when-not (and (string? c) (re-matches #"[a-z_][a-z_0-9]*" c)) (unsupported! :column))
                        [:col t c])))))
            (predicate! []
              (if (take! "(")
                (let [x (or!)] (expect! ")") x)
                (let [lhs (operand!) op (pop!)]
                  (case op
                    "is" (let [not? (take! "not")] (expect! "null") [(if not? :not-null :null) lhs])
                    "between" (let [lo (operand!)] (expect! "and") [:between lhs lo (operand!)])
                    "in" (do (expect! "(")
                             (let [xs (loop [xs [(operand!)]] (if (take! ",") (recur (conj xs (operand!))) xs))]
                               (expect! ")") [:in lhs xs]))
                    "not" (do (expect! "like") [:not-like lhs (operand!)])
                    (if (#{"=" "!=" "<>" "<" ">" "<=" ">=" "like"} op)
                      [(keyword (if (= op "<>") "!=" op)) lhs (operand!)]
                      (unsupported! :operator))))))
            (and! [] (let [xs (loop [xs [(predicate!)]] (if (take! "and") (recur (conj xs (predicate!))) xs))]
                       (if (= 1 (count xs)) (first xs) (into [:and] xs))))
            (or! [] (let [xs (loop [xs [(and!)]] (if (take! "or") (recur (conj xs (and!))) xs))]
                      (if (= 1 (count xs)) (first xs) (into [:or] xs))))]
      (let [result (or!)]
        (when-not (= @pos (count ts)) (unsupported! :trailing-tokens)) result))))

(defn parse [sql]
  (let [[_ select from where] (re-matches #"(?is)\s*SELECT\s+(.+?)\s+FROM\s+(.+?)\s+WHERE\s+(.+?);?\s*" sql)]
    (when-not where (unsupported! :query-shape))
    (let [outputs (mapv (fn [s]
                          (if-let [[_ a c] (re-matches #"(?i)\s*MIN\((\w+)\.(\w+)\)\s+AS\s+\w+\s*" s)]
                            [:col (str/lower-case a) (str/lower-case c)] (unsupported! :projection)))
                        (str/split select #","))
          from (mapv (fn [s]
                       (if-let [[_ t a] (re-matches #"(?i)\s*(\w+)\s+AS\s+(\w+)\s*" s)]
                         [(str/lower-case a) (str/lower-case t)] (unsupported! :from))) (str/split from #","))]
      (when-not (= (count from) (count (set (map first from)))) (unsupported! :duplicate-alias))
      {:sql sql :outputs outputs :from from :where (parse-where where) :where-sql where})))

(defn sql-query [parsed schema aggregate?]
  (str "SELECT " (when-not aggregate? "DISTINCT ")
       (str/join ", " (map (fn [[_ a c]] (let [col (str (jdbc/identifier a) "." (jdbc/identifier c))]
                                         (if aggregate? (str "MIN(" col ")") col))) (:outputs parsed)))
       " FROM " (str/join ", " (map (fn [[a t]] (str (jdbc/relation [schema t]) " AS " (jdbc/identifier a))) (:from parsed)))
       " WHERE " (:where-sql parsed)))

(defn columns [node]
  (into #{} (filter #(and (vector? %) (= :col (first %)))) (tree-seq sequential? seq node)))

(defn- guaranteed [node]
  (case (first node)
    :and (apply set/union #{} (map guaranteed (rest node)))
    :or (apply set/intersection (map guaranteed (rest node)))
    :null #{}
    (columns node)))

(defn attribute [table column] (keyword (str "job." table) column))
(defn- source-symbol [alias] (symbol (str "$" alias)))
(defn- row-symbol [alias] (symbol (str "?row_" alias)))

(defn compile-query
  "Metadata is the adapter's catalog discovery, never an imported copy of rows.
  Preserve every SQL alias, equality, output and source-table existence check."
  [parsed catalogs]
  (let [tables (into {} (:from parsed)) all-cols (set/union (columns (:where parsed)) (set (:outputs parsed)))
        _ (doseq [[_ a c] all-cols]
            (when-not (get-in catalogs [(tables a) :columns c]) (unsupported! :unknown-column)))
        nonnull (guaranteed (:where parsed))
        nullable-outputs (into #{} (filter (fn [[_ a c :as col]]
                                            (and (not (get-in catalogs [(tables a) :columns c :not-null?]))
                                                 (not (nonnull col))))) (:outputs parsed))
        domain (fn [[_ a c]] (if (= c "id") (tables a) (get-in references [(tables a) c])))
        parent (atom {})
        root (fn root [c] (if-let [p (get @parent c)] (root p) c))
        top (if (= :and (first (:where parsed))) (rest (:where parsed)) [(:where parsed)])
        _ (doseq [[op a b] top :when (and (= := op) (= :col (first a)) (= :col (first b)))]
            (when-not (= (domain a) (domain b)) (unsupported! :incompatible-join-domain))
            (let [a (root a) b (root b)]
              (when-not (= a b) (swap! parent assoc b a))))
        groups (group-by root all-cols)
        vars (into {} (mapcat (fn [[_ cols]]
                               (let [id-col (first (filter #(= "id" (nth % 2)) cols))
                                     [_ a c] (or id-col (first (sort cols)))
                                     v (if id-col (row-symbol a) (symbol (str "?" a "_" c)))]
                                 (map #(vector % v) cols))) groups))
        row (fn [a] (get vars [:col a "id"] (row-symbol a)))
        value (fn [operand] (if (= :col (first operand)) (vars operand) (second operand)))
        fact (fn [[_ a c :as col]] [(source-symbol a) (row a) (attribute (tables a) c) (vars col)])
        global-cols (set/union nonnull (set/difference (set (:outputs parsed)) nullable-outputs))
        global-facts (mapv fact (sort (remove #(= "id" (nth % 2)) global-cols)))
        anchored (set (map #(nth % 1) (remove #(= "id" (nth % 2)) global-cols)))
        anchors (for [[a t] (:from parsed) :when (not (anchored a))]
                  [(source-symbol a) (row a) (attribute t "id") (symbol (str "?identity_" a))])
        bound (into (set (map row (keys tables))) (map vars global-cols))
        optional-facts
        (mapv (fn [[_ a c :as col]]
                (let [e (row a) v (vars col)]
                  (list 'or-join [[e] v]
                        (apply list 'and [(fact col)])
                        (list 'and (list 'not-join [e] [(source-symbol a) e (attribute (tables a) c) '_])
                              [(list 'ground [nil]) [v]])))) (sort nullable-outputs))]
    (letfn [(emit [node]
              (let [[op a b c] node]
                (case op
                  :and (vec (mapcat emit (rest node)))
                  :null (let [[_ a c] a e (row a)]
                          [(list 'not-join [e] [(source-symbol a) e (attribute (tables a) c) '_])])
                  :or (let [inputs (vec (sort (set/intersection bound
                                                              (into (set (map (comp row second) (remove global-cols (columns node))))
                                                                    (map vars (columns node))))))]
                        [(apply list 'or-join [inputs]
                                (map #(apply list 'and (emit %)) (rest node)))])
                  (let [facts (mapv fact (sort (remove #(or (global-cols %) (= "id" (nth % 2))) (columns node))))
                        expr (case op
                               :not-null nil
                               :between (list '<= (value b) (value a) (value c))
                               :in (list 'in (value a) (mapv value b))
                               := (when-not (= (value a) (value b)) (list '= (value a) (value b)))
                               :!= (list 'not= (value a) (value b))
                               (list (symbol (name op)) (value a) (value b)))]
                    (cond-> facts expr (conj [expr]))))))]
      {:query {:find (mapv #(list 'min (vars %)) (:outputs parsed))
               :in (mapv (comp source-symbol first) (:from parsed))
               :where (vec (concat global-facts anchors (emit (:where parsed)) optional-facts))}
       :outputs (mapv vars (:outputs parsed))
       :ignore-nil-extrema (into #{} (map vars) nullable-outputs)
       :columns (reduce (fn [m [_ a c]] (update m (tables a) (fnil conj #{}) c)) {} all-cols)
       :source-tables (mapv second (:from parsed))
       :native-extrema (into {} (keep (fn [[_ a c :as col]]
                                       (when (= :string (pm/logical-type (get-in catalogs [(tables a) :columns c])))
                                         [(vars col) {:source (source-symbol a) :attribute (attribute (tables a) c)}])) (:outputs parsed)))})))

(defn sources! [profile catalogs spec]
  (let [source-id (UUID/randomUUID)
        required (set/union (set (:source-tables spec))
                            (into #{} (mapcat (fn [[t cols]] (keep #(get-in references [t %]) cols))) (:columns spec)))
        ;; Identity domains are independent of attributes, including self references.
        identities (into {} (map (fn [t] [t (pg/accept-mapping (catalogs t)
                                                             {:source-instance-id source-id :relation-id (keyword "job" t)
                                                              :identity-attribute (attribute t "id") :attributes {}})]) required))
        accepted (into {} (map (fn [[t cols]]
                                [t (pg/accept-mapping (catalogs t)
                                      {:source-instance-id source-id :relation-id (keyword "job" t)
                                       :identity-attribute (attribute t "id")
                                       :attributes (into {} (for [c cols :when (not= c "id")]
                                                             [(attribute t c)
                                                              (if-let [target (get-in references [t c])]
                                                                (let [type (get-in identities [target :identity :type])]
                                                                  {:column c :reference {:type type :domain (get-in type [:parameters :identity-domain-id])}})
                                                                c)]))})]) (:columns spec)))]
    (mapv #(pg/source (or (accepted %) (identities %)) profile) (:source-tables spec))))
