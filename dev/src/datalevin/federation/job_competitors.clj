(ns datalevin.federation.job-competitors
  "Reproducible JOB federation comparison against read-only PostgreSQL.
  Setup and plans are untimed. Each query retains raw samples and call order."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres-job :as job]
            [datalevin.federation.postgres-job-sql :as sql]
            [datalevin.federation.postgres-job-suite :as suite]
            [jsonista.core :as json])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Duration Instant]))

(def defaults
  {:queries nil :corpus (:corpus suite/defaults) :schema "public"
   :samples 1 :warmups 0 :timeout-ms 300000 :include-adapter? true
   :capture-plans? true :drill-ansi-not-equal? true
   :trino-catalog "job" :drill-schema "job.postgres.public"
   :environment-file nil})

(defn options! [options]
  (when-not (and (map? options)
                 (every? (conj (set (keys defaults)) :engine :endpoint) (keys options)))
    (throw (ex-info "Unknown comparison options" {})))
  (let [{:keys [engine endpoint samples warmups timeout-ms schema
                include-adapter? capture-plans? drill-ansi-not-equal?
                trino-catalog drill-schema environment-file] :as opts}
        (merge defaults options)]
    (suite/options! (select-keys opts [:queries :corpus :schema :samples :warmups :timeout-ms]))
    (when-not (and (#{:trino :drill :duckdb :accio} engine) (string? endpoint)
                   (re-matches #"https?://[^\s/]+(?::\d+)?" endpoint)
                   (<= 1 samples 10) (<= 0 warmups 3) (<= 1000 timeout-ms 300000)
                   (every? boolean? [include-adapter? capture-plans? drill-ansi-not-equal?])
                   (every? #(and (string? %) (not (str/blank? %))) [schema trino-catalog drill-schema])
                   (or (nil? environment-file) (string? environment-file)))
      (throw (ex-info "Invalid comparison options" {})))
    opts))

(defn reserve-output! [output]
  (io/make-parents output)
  (Files/createFile (.toPath (io/file output)) (make-array FileAttribute 0)))

(def implementation-resources
  ["datalevin/federation/job_competitors.clj"
   "datalevin/federation/postgres_job_suite.clj" "datalevin/federation/postgres_job_sql.clj"
   "datalevin/federation/postgres_job.clj" "datalevin/federation/postgres.clj"
   "datalevin/query/source/program.clj" "datalevin/query/source/join_cost.clj"
   "datalevin/federation/postgres/joins.clj" "datalevin/query/source/estimation.clj"
   "datalevin/query/source/region.clj" "datalevin/query/source/envelope.clj"
   "datalevin/query/source/relational.clj" "datalevin/query/source/scope.clj"
   "datalevin/query/source/planning.clj" "datalevin/query/source/session.clj"
   "datalevin/query/source/extrema.clj" "datalevin/query/source/runtime.clj"
   "datalevin/query/source/value.clj" "datalevin/query/source/schema.clj"
   "datalevin/query/source/descriptor.clj" "datalevin/query/source/access.clj"
   "datalevin/query/source/joins.clj" "datalevin/query/source/control.clj"
   "datalevin/query/source/cursor.clj" "datalevin/query/source/binding.clj"
   "datalevin/federation/entity_ref.clj" "datalevin/federation/postgres/statistics.clj"
   "datalevin/federation/fingerprint.clj" "datalevin/federation/mapping.clj"
   "datalevin/federation/postgres/mapping.clj" "datalevin/federation/postgres/relational.clj"
   "datalevin/federation/postgres/plan.clj" "datalevin/federation/postgres/jdbc.clj"])

(defn request! [^HttpClient client endpoint method body headers timeout-ms]
  (let [builder (doto (HttpRequest/newBuilder (URI/create endpoint))
                  (.timeout (Duration/ofMillis timeout-ms)))
        _ (doseq [[k v] headers] (.header builder k v))
        publisher (if body (HttpRequest$BodyPublishers/ofString body)
                      (HttpRequest$BodyPublishers/noBody))
        request (-> builder (.method method publisher) .build)
        response (.send client request (HttpResponse$BodyHandlers/ofString))
        text (.body response)]
    (when-not (<= 200 (.statusCode response) 299)
      (throw (ex-info "Engine HTTP failure"
                      {:http-status (.statusCode response)
                       :response (subs text 0 (min 4000 (count text)))})))
    (when (seq text) (json/read-value text))))

(defn trino! [client endpoint statement timeout-ms options]
  (let [headers {"X-Trino-User" "job-benchmark" "X-Trino-Catalog" (:trino-catalog options)
                 "X-Trino-Schema" (:schema options) "Content-Type" "text/plain"}
        active (atom nil)
        deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (try
      (loop [response (request! client (str endpoint "/v1/statement") "POST"
                                statement headers timeout-ms)
             rows [] requests 1]
        (reset! active (get response "nextUri"))
        (when-let [error (get response "error")]
          (throw (ex-info "Trino query failed"
                          {:engine-error error :query-id (get response "id")})))
        (let [rows (into rows (get response "data"))]
          (if-let [uri @active]
            (let [remaining (long (/ (- deadline (System/nanoTime)) 1000000))]
              (when-not (pos? remaining)
                (throw (ex-info "Trino client deadline" {:timeout-ms timeout-ms})))
              (recur (request! client uri "GET" nil headers remaining)
                     rows (inc requests)))
            {:rows (set rows) :query-id (get response "id")
             :http-requests requests :server-stats (get response "stats")})))
      (catch Exception e
        (when @active
          (try (request! client @active "DELETE" nil headers 10000)
               (catch Exception _ nil)))
        (throw e)))))

(defn cancel-drill! [client endpoint statement]
  (let [profiles (request! client (str endpoint "/profiles.json") "GET" nil {} 10000)]
    (doseq [p (get profiles "runningQueries")
            :when (= statement (get p "query"))]
      (request! client (str endpoint "/profiles/cancel/" (get p "queryId"))
                "GET" nil {} 10000))))

(defn ansi-not-equal
  "Normalize JOB inequality tokens, preserving quoted text and comments."
  [statement]
  (str/replace statement
               #"(?s)'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\r\n]*|/\*.*?\*/|!="
               #(if (= % "!=") "<>" %)))

(defn drill! [client endpoint statement timeout-ms options]
  (try
    (let [response (request! client (str endpoint "/query.json") "POST"
                             (json/write-value-as-string
                               {:queryType "SQL" :query statement
                                :defaultSchema (:drill-schema options)
                                :options {"drill.exec.http.rest.errors.verbose" "true"}})
                             {"Content-Type" "application/json"} timeout-ms)
          columns (get response "columns")]
      (when (or (get response "errorMessage")
                (contains? #{"FAILED" "CANCELED" "CANCELLATION_REQUESTED"}
                           (get response "queryState"))
                (not (contains? response "rows")))
        (throw (ex-info "Drill query failed" {:engine-error response})))
      {:rows (into #{} (map (fn [row] (mapv #(get row %) columns))) (get response "rows"))
       :query-id (get response "queryId") :query-state (get response "queryState")
       :columns columns :http-requests 1})
    (catch Exception e
      (try (cancel-drill! client endpoint statement)
           (catch Exception cleanup (.addSuppressed e cleanup)))
      (throw e))))

(defn attempt [f]
  (let [start (System/nanoTime)]
    (try (assoc (f) :status :ok :elapsed-ns (- (System/nanoTime) start))
         (catch Exception e
           {:status :failed :elapsed-ns (- (System/nanoTime) start)
            :failure {:class (.getName (class e)) :message (.getMessage e)
                      :data (ex-data e)}}))))

(defn bridge! [client endpoint operation statement timeout-ms]
  (let [response (request! client (str endpoint "/" operation) "POST"
                           (json/write-value-as-string {:sql statement :timeout_ms timeout-ms})
                           {"Content-Type" "application/json"} (+ timeout-ms 15000))]
    (when-let [error (get response "error")]
      (throw (ex-info "Python engine failed" {:engine-error error})))
    (if (= operation "query")
      (do (when-not (contains? response "rows")
            (throw (ex-info "Python engine returned no result" {:response response})))
          {:rows (set (mapv vec (get response "rows"))) :http-requests 1
           :engine-details (dissoc response "rows")})
      {:plan response})))

(defn adapter! [query providers]
  (try
    {:rows (let [rows (set (apply d/q query providers))]
             (if (empty? rows) #{(vec (repeat (count (:find query)) nil))} rows))}
    (finally
      (when-not (every? #(zero? (:live-sessions (pg/statistics %))) providers)
        (throw (ex-info "Adapter source leaked" {}))))))

(defn result-rows [rows]
  ;; Drill's REST scalar output is textual. JOB aggregates contain strings or
  ;; integral scalars; preserve nil separately and compare values by position.
  (set (map #(mapv (fn [v] (when (some? v) (str v))) %) rows)))

(defn checked [sample expected]
  (if (= :ok (:status sample))
    (assoc sample :answer-matches? (= (result-rows expected)
                                     (result-rows (:rows sample))))
    sample))

(defn adapter-setup [profile parsed catalogs options]
  (let [spec (sql/compile-query parsed catalogs)
        query (assoc (:query spec) :timeout (:timeout-ms options)
                     :query-source (cond-> {:limits (:limits suite/defaults)
                                           :native-extrema (:native-extrema spec)}
                                     (seq (:ignore-nil-extrema spec))
                                     (assoc :ignore-nil-extrema (:ignore-nil-extrema spec))))]
    {:query query :providers (sql/sources! profile catalogs spec)}))

(defn diagnostic! [client engine endpoint statement sample timeout-ms options]
  (case engine
    :trino (trino! client endpoint (str "EXPLAIN " statement) timeout-ms options)
    (:duckdb :accio) (bridge! client endpoint "plan" statement timeout-ms)
    :drill (let [id (:query-id sample)]
             (if id
               {:profile (request! client (str endpoint "/profiles/" id ".json")
                                   "GET" nil {} timeout-ms)}
               (drill! client endpoint (str "EXPLAIN PLAN FOR " statement) timeout-ms options)))))

(defn source-metadata! [profile schema tables]
  (let [literal #(str "'" (str/replace % "'" "''") "'")
        names (str/join "," (map literal (sort tables)))
        schema (literal schema)]
    {:row-count-kind :catalog-estimate
     :tables (suite/native! profile
               (str "SELECT c.relname,c.reltuples::bigint,c.relpages,pg_relation_size(c.oid) "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=" schema " AND c.relname IN (" names ")"))
     :indexes (suite/native! profile
                (str "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname="
                     schema " AND tablename IN (" names ")"))
     :statistics (suite/native! profile
                   (str "SELECT tablename,attname,null_frac,n_distinct,avg_width,correlation "
                        "FROM pg_stats WHERE schemaname=" schema " AND tablename IN (" names ")"))}))

(defn run-screen! [options output]
  (let [opts (options! options)
        {:keys [engine endpoint samples warmups timeout-ms include-adapter?]} opts
        url (System/getenv "POSTGRES_JOB_URL")
        _ (assert (seq url) "POSTGRES_JOB_URL is required")
        _ (reserve-output! output)
        profile {:jdbc-url url :compute-context-id :benchmark/job-competitors
                 :statement-timeout-ms timeout-ms :max-row-bytes 262144
                 :properties (cond-> {}
                               (System/getenv "POSTGRES_JOB_USER")
                               (assoc "user" (System/getenv "POSTGRES_JOB_USER"))
                               (System/getenv "POSTGRES_JOB_PASSWORD")
                               (assoc "password" (System/getenv "POSTGRES_JOB_PASSWORD")))}
        client (-> (HttpClient/newBuilder) (.connectTimeout (Duration/ofSeconds 10)) .build)
        entries (suite/corpus! (suite/options! (select-keys opts [:queries :corpus :schema])))
        tables (set (mapcat (fn [e] (map second (:from (sql/parse (:sql e))))) entries))
        catalogs (when include-adapter?
                   (into {} (for [t (sort tables)] [t (pg/discover profile [(:schema opts) t])])))
        _ (jdbc/identifier (:schema opts))
        report (atom {:format-version 2 :status :running :started-at (str (Instant/now))
                      :options opts :timing-scope :client-wall-including-planning-execute-decode-cleanup
                      :sampling :rotating-order-per-query :additional-tuple-oracles? false
                      :query-ids (mapv :id entries)
                      :environment (when-let [p (:environment-file opts)] (json/read-value (slurp p)))
                      :source-metadata (source-metadata! profile (:schema opts) tables)
                      :adapter-limits (:limits suite/defaults)
                      :client {:java (System/getProperty "java.version")
                               :max-heap-bytes (.maxMemory (Runtime/getRuntime))
                               :processors (.availableProcessors (Runtime/getRuntime))}
                      :implementation-sha256
                      (into (sorted-map)
                            (for [p implementation-resources]
                              [p (job/digest [(slurp (io/resource p))])]))
                      :database (suite/native! profile "SELECT version(), datcollate FROM pg_database WHERE datname=current_database()")
                      :native-settings
                      (suite/native! profile "SELECT current_setting('max_parallel_workers_per_gather'), current_setting('work_mem'), current_setting('shared_buffers'), current_setting('jit'), current_setting('geqo_threshold')")
                      :results []})]
    (try
      (suite/write-report! output @report)
      (doseq [[index entry] (map-indexed vector entries)]
        (let [statement (str/replace (str/trim (:sql entry)) #";\s*$" "")
              remote-statement (if (and (#{:drill :accio} engine) (:drill-ansi-not-equal? opts))
                                 (ansi-not-equal statement) statement)
              parsed (sql/parse (:sql entry))
              setup (when include-adapter?
                      (attempt #(adapter-setup profile parsed catalogs opts)))
              _ (when (and include-adapter? (not= :ok (:status setup)))
                  (throw (ex-info "Adapter benchmark setup failed" {:query (:id entry) :setup setup})))
              native #(hash-map :rows (suite/native! profile statement))
              remote #(case engine
                        :trino (trino! client endpoint statement timeout-ms opts)
                        (:duckdb :accio) (bridge! client endpoint "query" remote-statement timeout-ms)
                        :drill (drill! client endpoint remote-statement timeout-ms opts))
              modes (cond-> [:native engine]
                      (= :ok (:status setup)) (conj :adapter))
              invoke (fn [mode]
                       (let [ready (when (#{:duckdb :accio} mode)
                                     (request! client (str endpoint "/prepare") "GET" nil {} 150000))]
                         (cond-> (attempt (case mode
                                            :native native
                                            :adapter #(adapter! (:query setup) (:providers setup))
                                            remote))
                           ready (assoc :worker-restarts (get ready "restarts")))))
              oracle (invoke :native)
              _ (assert (= :ok (:status oracle)) (str "Native oracle failed: " (:id entry)))
              expected (:rows oracle)
              warm (mapv (fn [_] (into {} (for [m modes] [m (checked (invoke m) expected)])))
                         (range warmups))
              measurements
              (mapv (fn [i]
                      (let [offset (mod (+ i index) (count modes))
                            order (vec (take (count modes) (drop offset (cycle modes))))
                            result (reduce (fn [r m] (assoc r m (checked (invoke m) expected)))
                                           {:index i :order order} order)]
                        (prn {:query (:id entry) :sample i
                              :results (into {} (for [m modes]
                                                  [m (-> (get result m)
                                                         (select-keys [:status :answer-matches? :elapsed-ns :failure]))]))})
                        (flush) result)) (range samples))
              diagnostic (when (:capture-plans? opts)
                           (attempt #(diagnostic! client engine endpoint remote-statement
                                                  (get (last measurements) engine) timeout-ms opts)))
              result (cond-> (assoc entry :expected expected :warmups warm :samples measurements
                                    :setup (when setup (dissoc setup :query :providers))
                                    :diagnostic diagnostic)
                       (not= statement remote-statement)
                       (assoc :submitted-sql remote-statement
                              :sql-normalization :bang-equal-to-angle))]
          (swap! report update :results conj result)
          (suite/write-report! output @report)))
      (swap! report assoc :status :complete :finished-at (str (Instant/now)))
      (suite/write-report! output @report)
      @report
      (catch Exception e
        (swap! report assoc :status :failed :finished-at (str (Instant/now))
               :failure {:class (.getName (class e)) :message (.getMessage e)})
        (suite/write-report! output @report)
        (throw e))
      (finally (.close client)))))

(defn -main [options output]
  (when-not (and options output)
    (throw (ex-info "Usage: clojure -M:job-baselines '<EDN options>' <new-output.edn>" {})))
  (run-screen! (edn/read-string options) output)
  (shutdown-agents))
