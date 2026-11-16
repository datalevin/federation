(ns datalevin.federation.postgres-job
  "Correctness-gated JOB comparisons using the production PostgreSQL adapter."
  (:refer-clojure :exclude [run!])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.query.source.control :as control])
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.sql Connection DriverManager ResultSet]
           [java.util HexFormat Properties UUID]))

(def query-6a
  '{:find [(min ?keyword) (min ?name) (min ?title)]
    :in [$k $n $t $ci $mk]
    :where [[$k ?k :keyword/keyword "marvel-cinematic-universe"]
            [$k ?k :keyword/keyword ?keyword]
            [$n ?n :name/name ?name]
            [(like ?name "%Downey%Robert%")]
            [$t ?t :title/production-year ?year]
            [(> ?year 2010)]
            [$mk ?mk :movie-keyword/keyword ?k]
            [$mk ?mk :movie-keyword/movie ?t]
            [$ci ?ci :cast-info/movie ?t]
            [$ci ?ci :cast-info/person ?n]
            [$t ?t :title/title ?title]]})

(def hero-keywords
  ["superhero" "sequel" "second-part" "marvel-comics" "based-on-comic" "tv-special" "fight" "violence"])

(def query-6d
  (-> query-6a
      (assoc-in [:where 0] '[$k ?k :keyword/keyword ?keyword])
      (assoc-in [:where 1] [(list 'in '?keyword hero-keywords)])
      (assoc-in [:where 5] '[(> ?year 2000)])))

(def query-6b (assoc-in query-6d [:where 5] '[(> ?year 2014)]))

(def query-17c
  '{:find [(min ?name) (min ?name)]
    :in [$k $n $t $ci $mk $cn $mc]
    :where [[$k ?k :keyword/keyword ?keyword]
            [(= ?keyword "character-name-in-title")]
            [$n ?n :name/name ?name]
            [(like ?name "X%")]
            [$ci ?ci :cast-info/person ?n]
            [$ci ?ci :cast-info/movie ?t]
            [$t ?t :title/id ?title-id]
            [$mk ?mk :movie-keyword/movie ?t]
            [$mk ?mk :movie-keyword/keyword ?k]
            [$mc ?mc :movie-company/movie ?t]
            [$mc ?mc :movie-company/company ?cn]
            [$cn ?cn :company-name/id ?company-id]]})

(def query-17d
  (-> query-17c (assoc :find '[(min ?name)])
      (assoc-in [:where 3] '[(like ?name "%Bert%")])))

(def query-20b
  '{:find [(min ?title)]
    :in [$k $n $kt $t $chn $ci $mk $cct1 $cct2 $cc]
    :where [[$cct1 ?subject :comp-cast-type/kind "cast"]
            [$cct2 ?status :comp-cast-type/kind ?status-kind]
            [(like ?status-kind "%complete%")]
            [$chn ?character :char-name/name ?character-name]
            [(not-like ?character-name "%Sherlock%")]
            (or [(like ?character-name "%Tony%Stark%")]
                [(like ?character-name "%Iron%Man%")])
            [$k ?k :keyword/keyword ?keyword]
            [(in ?keyword ["superhero" "sequel" "second-part" "marvel-comics"
                           "based-on-comic" "tv-special" "fight" "violence"])]
            [$kt ?kind :kind-type/kind "movie"]
            [$n ?n :name/name ?name]
            [(like ?name "%Downey%Robert%")]
            [$t ?t :title/production-year ?year]
            [(> ?year 2000)]
            [$t ?t :title/title ?title]
            [$t ?t :title/kind ?kind]
            [$ci ?ci :cast-info/person ?n]
            [$ci ?ci :cast-info/movie ?t]
            [$ci ?ci :cast-info/character ?character]
            [$mk ?mk :movie-keyword/keyword ?k]
            [$mk ?mk :movie-keyword/movie ?t]
            [$cc ?cc :complete-cast/movie ?t]
            [$cc ?cc :complete-cast/subject ?subject]
            [$cc ?cc :complete-cast/status ?status]]})

(def table-specs
  ;; Reference targets precede their users. No imported/native copy of JOB data.
  [{:table "keyword" :identity :keyword/id :attributes {:keyword/keyword "keyword"}}
   {:table "name" :identity :name/id :attributes {:name/name "name"}}
   {:table "title" :identity :title/id
    :attributes {:title/title "title" :title/production-year "production_year"}}
   {:table "cast_info" :identity :cast-info/id
    :attributes {:cast-info/movie ["movie_id" "title"] :cast-info/person ["person_id" "name"]}}
   {:table "movie_keyword" :identity :movie-keyword/id
    :attributes {:movie-keyword/movie ["movie_id" "title"] :movie-keyword/keyword ["keyword_id" "keyword"]}}])

(def company-specs
  [{:table "company_name" :identity :company-name/id :attributes {}}
   {:table "movie_companies" :identity :movie-company/id
    :attributes {:movie-company/movie ["movie_id" "title"]
                 :movie-company/company ["company_id" "company_name"]}}])

(def hero-cast-specs
  (let [[keyword name title cast movie-keyword] table-specs]
    [keyword name
     {:table "kind_type" :identity :kind-type/id :attributes {:kind-type/kind "kind"}}
     (assoc-in title [:attributes :title/kind] ["kind_id" "kind_type"])
     {:table "char_name" :identity :char-name/id :attributes {:char-name/name "name"}}
     (assoc-in cast [:attributes :cast-info/character] ["person_role_id" "char_name"])
     movie-keyword
     {:table "comp_cast_type" :identity :comp-cast-type/id :attributes {:comp-cast-type/kind "kind"}}
     {:table "complete_cast" :identity :complete-cast/id
      :attributes {:complete-cast/movie ["movie_id" "title"]
                   :complete-cast/subject ["subject_id" "comp_cast_type"]
                   :complete-cast/status ["status_id" "comp_cast_type"]}}]))

(def query-specs
  {:6a {:query query-6a :tables table-specs :outputs '[?keyword ?name ?title]}
   :6b {:query query-6b :tables table-specs :outputs '[?keyword ?name ?title]}
   :6d {:query query-6d :tables table-specs :outputs '[?keyword ?name ?title]}
   :17c {:query query-17c :tables (into table-specs company-specs) :outputs '[?name ?name]}
   :17d {:query query-17d :tables (into table-specs company-specs) :outputs '[?name]}
   :20b {:query query-20b :tables hero-cast-specs :outputs '[?title]
         :source-tables ["keyword" "name" "kind_type" "title" "char_name" "cast_info"
                         "movie_keyword" "comp_cast_type" "comp_cast_type" "complete_cast"]}})

(def defaults {:query :6a :schema "public" :warmups 2 :samples 7 :timeout-ms 300000
               :fusion? true :relational? true :statistics? true :filtered-statistics? true
               :native-extrema? true :max-row-bytes 262144
               :limits {:retained-bytes 8589934592 :external-requests 16384}})

(defn options! [options]
  (when-not (and (map? options) (every? (set (keys defaults)) (keys options)))
    (throw (ex-info "Unknown JOB benchmark options" {})))
  (let [options (merge defaults options)
        options (cond-> options (map? (:limits options))
                  (update :limits #(merge (:limits defaults) %)))]
    (when-not (and (contains? query-specs (:query options))
                   (nat-int? (:warmups options)) (<= (:warmups options) 20)
                   (pos-int? (:samples options)) (<= (:samples options) 100)
                   (pos-int? (:timeout-ms options)) (<= (:timeout-ms options) 1048576)
                   (boolean? (:fusion? options)) (boolean? (:relational? options))
                   (boolean? (:statistics? options)) (boolean? (:filtered-statistics? options))
                   (boolean? (:native-extrema? options))
                   (pos-int? (:max-row-bytes options)) (<= (:max-row-bytes options) 1048576)
                   (map? (:limits options))
                   (every? #{:external-requests :retained-bytes :spill-bytes :batch-rows
                              :batch-bytes :binding-rows :binding-bytes} (keys (:limits options)))
                   (every? pos-int? (vals (:limits options))))
      (throw (ex-info "Invalid JOB benchmark options" {})))
    (jdbc/identifier (:schema options))
    options))

(defn sql-6a
  "Original JOB 6a (default), 6b or 6d, retaining all outputs and default collation."
  ([schema aggregate?] (sql-6a schema aggregate? :6a))
  ([schema aggregate? variant]
   (str (if aggregate?
         "SELECT MIN(k.keyword), MIN(n.name), MIN(t.title)"
         "SELECT DISTINCT k.keyword, n.name, t.title")
       " FROM " (jdbc/relation [schema "cast_info"]) " AS ci, "
       (jdbc/relation [schema "keyword"]) " AS k, "
       (jdbc/relation [schema "movie_keyword"]) " AS mk, "
       (jdbc/relation [schema "name"]) " AS n, "
       (jdbc/relation [schema "title"]) " AS t"
       (if (#{:6b :6d} variant)
         " WHERE k.keyword IN ('superhero','sequel','second-part','marvel-comics','based-on-comic','tv-special','fight','violence')"
         " WHERE k.keyword = 'marvel-cinematic-universe'")
       " AND n.name LIKE '%Downey%Robert%' AND t.production_year > " (case variant :6b 2014 :6d 2000 2010)
       " AND k.id = mk.keyword_id AND t.id = mk.movie_id"
       " AND t.id = ci.movie_id AND ci.movie_id = mk.movie_id AND n.id = ci.person_id")))

(defn sql-query [schema aggregate? variant]
  (if (#{:6a :6b :6d} variant) (sql-6a schema aggregate? variant)
    (let [family-17? (#{:17c :17d} variant)
          outputs (case variant :17c ["n.name" "n.name"] :17d ["n.name"] :20b ["t.title"])
          tables (if family-17?
                   [["cast_info" "ci"] ["company_name" "cn"] ["keyword" "k"]
                    ["movie_companies" "mc"] ["movie_keyword" "mk"] ["name" "n"] ["title" "t"]]
                   [["complete_cast" "cc"] ["comp_cast_type" "cct1"] ["comp_cast_type" "cct2"]
                    ["char_name" "chn"] ["cast_info" "ci"] ["keyword" "k"] ["kind_type" "kt"]
                    ["movie_keyword" "mk"] ["name" "n"] ["title" "t"]])]
      (str "SELECT " (when-not aggregate? "DISTINCT ")
           (str/join ", " (map #(if aggregate? (str "MIN(" % ")") %) outputs))
           " FROM " (str/join ", " (map (fn [[table alias]] (str (jdbc/relation [schema table]) " AS " alias)) tables))
           " WHERE "
           (if family-17?
             (str "k.keyword = 'character-name-in-title' AND n.name LIKE "
                  (if (= :17c variant) "'X%'" "'%Bert%'")
                  " AND n.id = ci.person_id AND ci.movie_id = t.id AND t.id = mk.movie_id"
                  " AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_id = cn.id"
                  " AND ci.movie_id = mc.movie_id AND ci.movie_id = mk.movie_id AND mc.movie_id = mk.movie_id")
             (str "cct1.kind = 'cast' AND cct2.kind LIKE '%complete%' AND chn.name NOT LIKE '%Sherlock%'"
                  " AND (chn.name LIKE '%Tony%Stark%' OR chn.name LIKE '%Iron%Man%')"
                  " AND k.keyword IN ('superhero','sequel','second-part','marvel-comics','based-on-comic','tv-special','fight','violence')"
                  " AND kt.kind = 'movie' AND n.name LIKE '%Downey%Robert%' AND t.production_year > 2000"
                  " AND kt.id = t.kind_id AND t.id = mk.movie_id AND t.id = ci.movie_id AND t.id = cc.movie_id"
                  " AND mk.movie_id = ci.movie_id AND mk.movie_id = cc.movie_id AND ci.movie_id = cc.movie_id"
                  " AND chn.id = ci.person_role_id AND n.id = ci.person_id AND k.id = mk.keyword_id"
                  " AND cct1.id = cc.subject_id AND cct2.id = cc.status_id"))))))

(defn sources!
  "Discover and accept only mapped columns and relations needed by this query."
  ([profile schema] (sources! profile schema :6a))
  ([profile schema variant]
  (let [{:keys [tables source-tables]} (query-specs variant)
        source-id (UUID/randomUUID)
        accepted
        (reduce
          (fn [accepted {:keys [table identity attributes]}]
            (assoc accepted table
                   (pg/accept-mapping (pg/discover profile [schema table])
                     {:source-instance-id source-id :relation-id (keyword "job" table)
                      :identity-attribute identity
                      :attributes
                      (into {} (map (fn [[attribute spec]]
                                      [attribute
                                       (if (string? spec) spec
                                         (let [[column target] spec
                                               type (get-in accepted [target :identity :type])]
                                           {:column column :reference
                                            {:type type :domain (get-in type [:parameters :identity-domain-id])}}))]))
                            attributes)}))) {} tables)]
    (mapv #(pg/source (accepted %) profile) (or source-tables (map :table tables))))))

(defn- connect ^Connection [profile]
  (let [properties (Properties.)]
    (doseq [[k v] (:properties profile)] (.setProperty properties k v))
    (DriverManager/getConnection (:jdbc-url profile) properties)))

(defn- native!
  "Each invocation owns a fresh read-only snapshot, including connect/close time."
  ([profile sql] (native! profile sql []))
  ([profile sql params]
   (with-open [conn (connect profile)]
    (.setReadOnly conn true)
    (.setTransactionIsolation conn Connection/TRANSACTION_REPEATABLE_READ)
    (.setAutoCommit conn false)
    (try
      (with-open [stmt (.prepareStatement conn sql)]
        (doseq [[i x] (map-indexed vector params)] (.setObject stmt (inc i) x))
        (.setQueryTimeout stmt (int (Math/ceil (/ (:statement-timeout-ms profile) 1000.0))))
        (with-open [rs (.executeQuery stmt)]
          (let [n (.getColumnCount (.getMetaData rs))]
            (loop [rows #{}]
              (if (.next rs)
                (recur (conj rows (mapv #(.getObject ^ResultSet rs (int %)) (range 1 (inc n)))))
                rows)))))
      (finally (.rollback conn))))))

(defn- sha256 [text]
  (.formatHex (HexFormat/of)
              (.digest (MessageDigest/getInstance "SHA-256")
                       (.getBytes ^String text StandardCharsets/UTF_8))))

(defn digest [rows] (sha256 (pr-str (vec (sort-by pr-str rows)))))

(defn- implementation []
  (into (sorted-map)
        (keep (fn [path] (when-let [resource (io/resource path)] [path (sha256 (slurp resource))])))
        ["datalevin/federation/postgres_job.clj" "datalevin/federation/postgres.clj"
         "datalevin/federation/postgres/plan.clj" "datalevin/federation/postgres/jdbc.clj"
         "datalevin/federation/postgres/statistics.clj" "datalevin/query/source/access.clj"
         "datalevin/federation/postgres/joins.clj" "datalevin/query/source/joins.clj"
         "datalevin/query/source/join_cost.clj"
         "datalevin/query/source/contract.clj" "datalevin/query/source/cursor.clj"
         "datalevin/query/source/extrema.clj"
         "datalevin/query/source/region.clj" "datalevin/query/source/envelope.clj"
         "datalevin/query/source.clj" "datalevin/query/source/estimation.clj"
         "datalevin/query/source/session.clj" "datalevin/query/source/control.clj"
         "datalevin/federation/postgres/relational.clj" "datalevin/query/source/planning.clj"
         "datalevin/query/source/program.clj"]))

(defn summarize [samples]
  (let [xs (vec (sort samples)) n (count xs)]
    (when (pos? n)
      {:samples n :min-ns (first xs) :max-ns (peek xs)
       :median-ns (if (odd? n) (nth xs (quot n 2))
                    (/ (+ (nth xs (dec (quot n 2))) (nth xs (quot n 2))) 2.0))
       :p95-ns (nth xs (dec (int (Math/ceil (* 0.95 n)))))})))

(defn comparison [samples nonempty?]
  (let [native (summarize (map #(get-in % [:native :elapsed-ns]) samples))
        adapter (summarize (map #(get-in % [:adapter :elapsed-ns]) samples))
        ratio (when (seq samples) (/ (double (:median-ns native)) (:median-ns adapter)))
        faster (count (filter #(< (get-in % [:adapter :elapsed-ns]) (get-in % [:native :elapsed-ns])) samples))]
    {:native native :adapter adapter :speedup ratio :adapter-faster-pairs faster
     ;; A useful milestone needs a margin and repeated wins on nonempty input.
     :win? (boolean (and nonempty? (>= (count samples) 5) (>= ratio 1.1)
                          (>= faster (Math/ceil (* 0.8 (count samples))))))}))

(defn- check-result! [expected actual]
  (when-not (= expected actual)
    (throw (ex-info "JOB result mismatch" {:error :result-mismatch
                                           :expected-sha256 (digest expected)
                                           :actual-sha256 (digest actual)})))
  actual)

(defn- timed [f]
  (let [start (System/nanoTime) result (f)]
    {:elapsed-ns (- (System/nanoTime) start) :result result}))

(defn- clean! [sources]
  (when-not (every? #(zero? (:live-sessions (pg/statistics %))) sources)
    (throw (ex-info "JOB query retained a source session" {:error :live-sessions}))))

(defn run!
  "Run against a static, already loaded JOB database. Never loads or changes data.
  Connection properties stay in memory and are excluded from the returned report."
  [profile options]
  (let [started (System/nanoTime)
        options (options! options)
        {:keys [schema timeout-ms limits]} options
        variant (:query options) {:keys [tables outputs] :as spec} (query-specs variant)
        profile (merge profile (select-keys options [:fusion? :relational? :statistics? :filtered-statistics? :max-row-bytes])
                       {:compute-context-id :benchmark/job :statement-timeout-ms timeout-ms})
        native-extrema (when (:native-extrema? options)
                         (select-keys '{?keyword {:source $k :attribute :keyword/keyword}
                                        ?name {:source $n :attribute :name/name}
                                        ?title {:source $t :attribute :title/title}} outputs))
        query (assoc (:query spec) :timeout timeout-ms
                     :query-source (cond-> {:limits limits} (seq native-extrema) (assoc :native-extrema native-extrema)))
        sql (sql-query schema true variant)
        report (atom {:format-version 1 :status :running :options options
                      :query query :sql sql :started-at (str (java.time.Instant/now))
                      :implementation-sha256 (implementation)
                      :effective-limits (merge control/default-limits limits)
                      :effective-source-profile (select-keys (jdbc/profile! profile) [:max-row-bytes :fetch-size])
                      :timing-scope :client-wall-including-connect-snapshot-planning-execute-close
                      :win-rule {:minimum-samples 5 :minimum-speedup 1.1 :minimum-faster-pair-fraction 0.8
                                 :requires-nonempty-input true}
                      :environment {:java (System/getProperty "java.version") :clojure (clojure-version)
                                    :os (System/getProperty "os.name") :arch (System/getProperty "os.arch")
                                    :max-heap-bytes (.maxMemory (Runtime/getRuntime))}
                      :samples []})
        providers (atom nil)
        stage (atom :discovery) phase-started (atom (System/nanoTime))
        begin! (fn [name] (reset! stage name) (reset! phase-started (System/nanoTime)))]
    (try
      (let [setup (timed #(sources! profile schema variant)) sources (:result setup)
            invoke #(try (set (apply d/q % sources)) (finally (clean! sources)))
            native #(native! profile sql)
            adapter #(invoke query)]
        (reset! providers sources)
        (swap! report assoc :discovery-ns (:elapsed-ns setup))
        (begin! :native-oracle)
        (let [tuples (native! profile (sql-query schema false variant))
              first-native (timed native) expected (:result first-native)
              table-names (mapv :table tables)
              metadata-params (into [schema] table-names)
              metadata-filter (str " IN (" (str/join "," (repeat (count table-names) "?")) ")")
              metadata (native! profile "SELECT version(), current_database(), datcollate FROM pg_database WHERE datname = current_database()")]
          (swap! report assoc :database (first metadata) :expected expected
                 :native-first-query (dissoc first-native :result)
                 :verification {:matched-distinct-rows (count tuples) :sha256 (digest tuples)})
          (begin! :native-diagnostics)
          (swap! report assoc :native-plan
                 (str (ffirst (native! profile (str "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) " sql)))))
          (swap! report assoc :relations
                 (vec (sort-by first
                        (native! profile
                          (str "SELECT c.relname, c.reltuples::float8, c.relpages FROM pg_class c "
                               "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=? "
                               "AND c.relname" metadata-filter) metadata-params))))
          (swap! report assoc :indexes
                 (vec (sort-by pr-str
                        (native! profile
                          (str "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname=? "
                               "AND tablename" metadata-filter) metadata-params))))
          (begin! :adapter-plan)
          (swap! report assoc :adapter-plan (pr-str (apply d/explain {:run? false} query sources)))
          (clean! sources)
          (begin! :adapter-oracle)
          (check-result! tuples (invoke (-> query (assoc :find outputs)
                                            (update :query-source dissoc :native-extrema))))
          (let [check-adapter #(let [actual (adapter)
                                    ;; SQL returns one all-null row for empty aggregate input.
                                    actual (if (and (empty? tuples) (empty? actual)) #{(vec (repeat (count outputs) nil))} actual)]
                                (check-result! expected actual))
                check-native #(check-result! expected (native))]
            (begin! :first-aggregate-query)
            (swap! report assoc :first-aggregate-query (dissoc (timed check-adapter) :result))
            (begin! :warmup)
            (dotimes [i (:warmups options)]
              (doseq [f (if (even? i) [check-native check-adapter] [check-adapter check-native])] (f)))
            (begin! :measurement)
            (dotimes [i (:samples options)]
              (let [order (if (even? i) [:native :adapter] [:adapter :native])
                    sample (reduce (fn [sample mode]
                                     (assoc sample mode
                                            (dissoc (timed (case mode :native check-native :adapter check-adapter)) :result)))
                                   {:index i :order order} order)]
                (swap! report update :samples conj sample)))
            (begin! :adapter-diagnostics)
            (swap! report assoc :executed-plan (pr-str (apply d/explain {:run? true} query sources)))
            (clean! sources)
            (swap! report assoc :status :ok :comparison (comparison (:samples @report) (seq tuples))))))
      (catch Exception e
        ;; JDBC messages may contain connection details. Persist only safe classifications.
        (swap! report assoc :status :failed
               :failure (merge {:stage @stage :class (.getName (class e))
                                :elapsed-ns (- (System/nanoTime) @phase-started)}
                               (select-keys (ex-data e) [:error :phase :category :maximum :attempted
                                                        :expected-sha256 :actual-sha256]))))
      (finally
        (when @providers
          (swap! report assoc :source-statistics (mapv pg/statistics @providers)))
        (swap! report assoc :total-ns (- (System/nanoTime) started))))
    @report))

(defn -main [options output]
  (let [url (System/getenv "POSTGRES_JOB_URL")]
    (when-not (and url options output)
      (throw (ex-info "Set POSTGRES_JOB_URL; pass an EDN options map and an output path" {})))
    (let [properties (cond-> {}
                       (System/getenv "POSTGRES_JOB_USER") (assoc "user" (System/getenv "POSTGRES_JOB_USER"))
                       (System/getenv "POSTGRES_JOB_PASSWORD") (assoc "password" (System/getenv "POSTGRES_JOB_PASSWORD")))
          result (run! {:jdbc-url url :properties properties} (edn/read-string options))]
      (with-open [writer (io/writer output)]
        (binding [*out* writer] (pprint/pprint result)))
      (prn (select-keys result [:status :failure :comparison]))
      (shutdown-agents)
      (System/exit (if (= :ok (:status result)) 0 1)))))
