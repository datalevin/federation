(ns datalevin.federation.postgres-job-suite
  "Checkpointed full JOB client-wall baseline. Never loads or mutates JOB data."
  (:refer-clojure :exclude [run!])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [datalevin.core :as d]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres-job :as job]
            [datalevin.federation.postgres-job-sql :as sql]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.query.source.control :as control])
  (:import [java.nio.file Files StandardCopyOption]
           [java.sql Connection DriverManager ResultSet SQLException]
           [java.util Properties]))

(def defaults
  {:corpus "../datalevin/benchmarks/JOB-bench/queries" :schema "public"
   :timeout-ms (:timeout-ms job/defaults) :samples 1 :warmups 0 :queries nil :verify-tuples? true
   :oracle-max-rows 1000000 :max-row-bytes (:max-row-bytes job/defaults)
   :limits (:limits job/defaults)})

(defn options! [opts]
  (when-not (and (map? opts) (every? (set (keys defaults)) (keys opts)))
    (throw (ex-info "Unknown JOB suite options" {})))
  (let [opts (merge defaults opts)
        {:keys [corpus schema timeout-ms samples warmups queries verify-tuples? oracle-max-rows max-row-bytes limits] :as opts}
        (cond-> opts (map? (:limits opts)) (update :limits #(merge (:limits defaults) %)))]
    (when-not (and (string? corpus) (string? schema)
                   (pos-int? timeout-ms) (<= timeout-ms 1048576)
                   (pos-int? samples) (<= samples 20) (nat-int? warmups) (<= warmups 10)
                   (or (nil? queries) (and (vector? queries) (seq queries) (every? keyword? queries)
                                           (= (count queries) (count (set queries)))))
                   (boolean? verify-tuples?) (pos-int? oracle-max-rows)
                   (pos-int? max-row-bytes) (<= max-row-bytes 1048576)
                   (map? limits) (every? (set (keys control/default-limits)) (keys limits))
                   (every? pos-int? (vals limits)))
      (throw (ex-info "Invalid JOB suite options" {})))
    (jdbc/identifier schema) opts))

(defn query-order [id]
  (let [[_ n suffix] (re-matches #"(\d+)([a-z])" (name id))]
    [(Long/parseLong n) suffix]))

(defn corpus! [options]
  (let [files (->> (.listFiles (io/file (:corpus options)))
                   (filter #(re-matches #"\d+[a-z]\.sql" (.getName ^java.io.File %)))
                   (map (fn [f] [(keyword (str/replace (.getName ^java.io.File f) #"\.sql$" "")) (slurp f)]))
                   (into {}))]
    (when-not (= 113 (count files))
      (throw (ex-info "The original JOB corpus must contain 113 SQL queries" {:count (count files)})))
    (when (some #(not (contains? files %)) (:queries options))
      (throw (ex-info "Unknown JOB query" {})))
    (mapv (fn [id] {:id id :sql (files id) :sha256 (job/digest [(files id)])})
          (sort-by query-order (or (:queries options) (keys files))))))

(defn native!
  "Fresh read-only repeatable-read connection; includes decoding and close.
  A result-row bound applies to the untimed DISTINCT correctness oracle."
  ([profile statement] (native! profile statement Long/MAX_VALUE))
  ([profile statement max-rows]
   (let [props (Properties.)]
     (doseq [[k v] (:properties profile)] (.setProperty props k v))
     (with-open [conn (DriverManager/getConnection (:jdbc-url profile) props)]
       (.setReadOnly conn true)
       (.setTransactionIsolation conn Connection/TRANSACTION_REPEATABLE_READ)
       (.setAutoCommit conn false)
       (try
         (with-open [stmt (.prepareStatement conn statement)]
           (.setQueryTimeout stmt (int (Math/ceil (/ (:statement-timeout-ms profile) 1000.0))))
           ;; A nonzero Execute fetch count suppresses native parallel workers.
           ;; Timed aggregates return one row; bound only the untimed tuple oracle.
           (.setFetchSize stmt (if (= max-rows Long/MAX_VALUE) 0 4096))
           (with-open [rs (.executeQuery stmt)]
             (let [n (.getColumnCount (.getMetaData rs))]
               (loop [rows #{}]
                 (if (.next rs)
                   (do (when (>= (count rows) max-rows)
                         (throw (ex-info "JOB oracle exceeded row bound" {:error :benchmark/oracle-limit :maximum max-rows})))
                       (recur (conj rows (mapv #(.getObject ^ResultSet rs (int %)) (range 1 (inc n))))))
                   rows)))))
         (finally (.rollback conn)))))))

(defn attempt [f]
  (let [start (System/nanoTime)]
    (try (let [result (f)] {:status :ok :elapsed-ns (- (System/nanoTime) start) :result result})
         (catch Exception e
           {:status :failed :elapsed-ns (- (System/nanoTime) start)
            :failure (merge {:class (.getName (class e))}
                            (when (instance? SQLException e) {:sql-state (.getSQLState ^SQLException e)})
                            (select-keys (ex-data e) [:error :reason :phase :category :maximum :attempted
                                                     :expected-sha256 :actual-sha256]))}))))

(defn- measured [f] (attempt f))

(defn- check! [expected actual]
  (when-not (= expected actual)
    (throw (ex-info "JOB result mismatch"
                    {:error :benchmark/result-mismatch :expected-sha256 (job/digest expected)
                     :actual-sha256 (job/digest actual)})))
  {:status :ok :rows (count expected) :sha256 (job/digest expected)})

(defn- invoke! [providers query]
  (try (set (apply d/q query providers))
       (finally
         (when-not (every? #(zero? (:live-sessions (pg/statistics %))) providers)
           (throw (ex-info "JOB source session leaked" {:error :benchmark/live-sessions}))))))

(defn run-query! [profile options catalogs index {:keys [id] :as entry}]
  (let [profile (assoc profile :max-row-bytes (:max-row-bytes options))
        parsed (attempt #(sql/parse (:sql entry)))
        ;; Original SQL is valid independently of adapter translation support.
        native-sql (if (= :ok (:status parsed)) (sql/sql-query (:result parsed) (:schema options) true)
                       (throw (ex-info "Original JOB SQL could not be parsed" {:query id})))
        setup (measured #(let [spec (sql/compile-query (:result parsed) catalogs)]
                           {:spec spec :providers (sql/sources! profile catalogs spec)}))
        {:keys [spec providers]} (:result setup)
        query (when spec (assoc (:query spec) :timeout (:timeout-ms options)
                                :query-source (cond-> {:limits (:limits options) :native-extrema (:native-extrema spec)}
                                                (seq (:ignore-nil-extrema spec))
                                                (assoc :ignore-nil-extrema (:ignore-nil-extrema spec)))))
        native #(native! profile native-sql)
        adapter #(invoke! providers query)
        warmups (when (= :ok (:status setup))
                  (mapv (fn [_] {:native (dissoc (measured native) :result)
                                 :adapter (dissoc (measured adapter) :result)}) (range (:warmups options))))
        samples (mapv (fn [i]
                        (let [order (if (even? (+ index i)) [:native :adapter] [:adapter :native])]
                          (reduce (fn [m mode]
                                    (assoc m mode (if (and (= mode :adapter) (not= :ok (:status setup)))
                                                    {:status (if (= :benchmark/unsupported (get-in setup [:failure :error])) :unsupported :failed)
                                                     :failure (:failure setup)}
                                                    (measured (if (= mode :native) native adapter)))))
                                  {:order order :index i} order))) (range (:samples options)))
        expected (get-in samples [0 :native :result])
        aggregate-checks (mapv (fn [sample]
                                 (if (and (= :ok (get-in sample [:native :status]))
                                          (= :ok (get-in sample [:adapter :status])))
                                   (attempt #(check! (get-in sample [:native :result])
                                                     (let [rows (get-in sample [:adapter :result])]
                                                       (if (empty? rows) #{(vec (repeat (count (:outputs spec)) nil))} rows))))
                                   {:status :unavailable})) samples)
        verified-aggregates? (every? #(= :ok (:status %)) aggregate-checks)
        tuples (when (and verified-aggregates? (:verify-tuples? options))
                 (let [native-tuples (measured #(native! profile (sql/sql-query (:result parsed) (:schema options) false) (:oracle-max-rows options)))
                       adapter-tuples (when (= :ok (:status native-tuples))
                                        (measured #(invoke! providers (-> query (assoc :find (:outputs spec))
                                                                        (update :query-source dissoc :native-extrema :ignore-nil-extrema)))))
                       check (when (= :ok (:status adapter-tuples))
                               (attempt #(check! (:result native-tuples) (:result adapter-tuples))))]
                   {:native (dissoc native-tuples :result) :adapter (some-> adapter-tuples (dissoc :result)) :check check}))
        valid? (and verified-aggregates? (or (not (:verify-tuples? options)) (= :ok (get-in tuples [:check :status]))))]
    (merge (dissoc entry :sql)
           {:sql native-sql :query query :status (if valid? :ok :incomplete)
            :setup (dissoc setup :result) :warmups warmups
            :samples (mapv #(-> % (update :native dissoc :result) (update :adapter dissoc :result)) samples)
            :expected expected :aggregate-checks aggregate-checks :tuple-verification tuples
            :source-statistics (mapv pg/statistics providers)})))

(defn summary [entries expected-count sample-count]
  (let [valid (filter #(= :ok (:status %)) entries)
        total (fn [xs mode i] (reduce + 0 (keep #(let [r (get-in % [:samples i mode])]
                                                                  (when (= :ok (:status r)) (:elapsed-ns r))) xs)))
        totals (mapv (fn [i]
                       (let [native-complete? (= expected-count (count (filter #(= :ok (get-in % [:samples i :native :status])) entries)))
                             full? (and (= 113 expected-count) (= expected-count (count valid)) native-complete?)]
                         {:sample i :native-complete? native-complete? :full-suite-comparable? full?
                          :native-total-ns (when native-complete? (total entries :native i))
                          :adapter-verified-subtotal-ns (total valid :adapter i)
                          :native-matched-subtotal-ns (total valid :native i)
                          :adapter-attempted-ns (reduce + 0 (keep #(get-in % [:samples i :adapter :elapsed-ns]) entries))
                          :adapter-full-suite-ns (when full? (total entries :adapter i))
                          :speedup (when full? (/ (double (total entries :native i)) (total entries :adapter i)))})) (range sample-count))]
    {:expected-queries expected-count :attempted-queries (count entries) :verified-queries (count valid)
     :incomplete-queries (mapv :id (remove #(= :ok (:status %)) entries))
     :adapter-outcomes (frequencies (for [entry entries sample (:samples entry)]
                                      (let [a (:adapter sample)]
                                        (if (= :ok (:status a)) (if (= :ok (:status entry)) :verified :verification-failed)
                                          (or (get-in a [:failure :reason]) (get-in a [:failure :category])
                                              (get-in a [:failure :error]) (get-in a [:failure :sql-state]) (:status a))))))
     :totals totals}))

(defn write-report!
  "Atomically checkpoint an EDN benchmark report beside its destination."
  [path report]
  (let [file (io/file path) tmp (io/file (str path ".tmp"))]
    (io/make-parents file)
    (with-open [w (io/writer tmp)] (binding [*out* w] (pprint/pprint report)))
    (Files/move (.toPath tmp) (.toPath file)
                (into-array StandardCopyOption [StandardCopyOption/ATOMIC_MOVE StandardCopyOption/REPLACE_EXISTING]))))

(defn run! [profile opts output]
  (let [started (System/nanoTime) options (options! opts) corpus (corpus! options)
        profile (merge profile {:compute-context-id :benchmark/job-suite :statement-timeout-ms (:timeout-ms options)
                                :max-row-bytes (:max-row-bytes options)})
        parsed (mapv #(sql/parse (:sql %)) corpus)
        tables (sort (set (mapcat #(map second (:from %)) parsed)))
        discovery (measured #(into {} (map (fn [t] [t (pg/discover profile [(:schema options) t])]) tables)))
        catalogs (:result discovery)
        report (atom {:format-version 1 :status :running :started-at (str (java.time.Instant/now)) :options options
                      :timing-scope :client-wall-including-connect-snapshot-planning-estimation-execute-decode-close
                      :excluded-from-query-time [:jvm-startup :catalog-discovery :mapping-acceptance :correctness-oracles :warmups]
                      :sample-policy :alternating-mode-order-by-query-and-sample
                      :native-aggregate-fetch-size 0
                      :effective-limits (merge control/default-limits (:limits options))
                      :effective-source-profile (select-keys (jdbc/profile! profile) [:max-row-bytes :fetch-size :whole-query-mode])
                      :implementation-sha256 (into (sorted-map)
                                                   (for [p ["datalevin/federation/postgres_job_suite.clj" "datalevin/federation/postgres_job_sql.clj"
                                                            "datalevin/federation/postgres_job.clj"
                                                            "datalevin/federation/postgres.clj" "datalevin/query/source/program.clj"
                                                            "datalevin/query/source/join_cost.clj" "datalevin/federation/postgres/joins.clj"
                                                            "datalevin/query/source/estimation.clj"
                                                            "datalevin/query/source/region.clj" "datalevin/query/source/envelope.clj"
                                                            "datalevin/query/source/relational.clj" "datalevin/query/source/scope.clj" "datalevin/query/source/planning.clj"
                                                            "datalevin/query/source/session.clj" "datalevin/query/source/extrema.clj"
                                                            "datalevin/query/source/runtime.clj" "datalevin/query/source/value.clj"
                                                            "datalevin/query/source/schema.clj" "datalevin/query/source/descriptor.clj"
                                                            "datalevin/query/source/access.clj" "datalevin/query/source/joins.clj"
                                                            "datalevin/query/source/control.clj"
                                                            "datalevin/query/source/cursor.clj" "datalevin/query/source/binding.clj"
                                                            "datalevin/federation/entity_ref.clj"
                                                            "datalevin/federation/postgres/statistics.clj"
                                                            "datalevin/federation/fingerprint.clj" "datalevin/federation/mapping.clj"
                                                            "datalevin/federation/postgres/mapping.clj"
                                                            "datalevin/federation/postgres/relational.clj"
                                                            "datalevin/federation/postgres/plan.clj" "datalevin/federation/postgres/jdbc.clj"]]
                                                     [p (job/digest [(slurp (io/resource p))])]))
                      :environment {:java (System/getProperty "java.version") :clojure (clojure-version)
                                    :os (System/getProperty "os.name") :arch (System/getProperty "os.arch")
                                    :max-heap-bytes (.maxMemory (Runtime/getRuntime))}
                      :database (native! profile "SELECT version(), current_database(), datcollate FROM pg_database WHERE datname=current_database()")
                      :discovery (dissoc discovery :result) :results []})]
    (when-not (= :ok (:status discovery)) (throw (ex-info "JOB catalog discovery failed" (:failure discovery))))
    (write-report! output @report)
    (doseq [[i entry] (map-indexed vector corpus)]
      (let [result (run-query! profile options catalogs i entry)]
        (swap! report update :results conj result)
        (swap! report assoc :summary (summary (:results @report) (count corpus) (:samples options))
               :harness-elapsed-ns (- (System/nanoTime) started))
        (write-report! output @report)
        (prn {:query (:id entry) :status (:status result)
              :native-ms (some-> (get-in result [:samples 0 :native :elapsed-ns]) (/ 1e6))
              :adapter-ms (some-> (get-in result [:samples 0 :adapter :elapsed-ns]) (/ 1e6))
              :failure (or (get-in result [:setup :failure]) (get-in result [:samples 0 :adapter :failure])
                           (some :failure (:aggregate-checks result)) (get-in result [:tuple-verification :adapter :failure]))})
        (flush)))
    (swap! report assoc :status :complete :finished-at (str (java.time.Instant/now))
           :harness-elapsed-ns (- (System/nanoTime) started))
    (write-report! output @report)
    @report))

(defn -main [opts output]
  (let [url (System/getenv "POSTGRES_JOB_URL")]
    (when-not (and url opts output) (throw (ex-info "Set POSTGRES_JOB_URL; pass EDN options and output path" {})))
    (let [properties (cond-> {} (System/getenv "POSTGRES_JOB_USER") (assoc "user" (System/getenv "POSTGRES_JOB_USER"))
                                (System/getenv "POSTGRES_JOB_PASSWORD") (assoc "password" (System/getenv "POSTGRES_JOB_PASSWORD")))
          report (run! {:jdbc-url url :properties properties} (edn/read-string opts) output)]
      (prn (:summary report)) (shutdown-agents))))
