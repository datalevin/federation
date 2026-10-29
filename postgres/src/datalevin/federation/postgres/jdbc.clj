(ns datalevin.federation.postgres.jdbc
  "Private PostgreSQL transport. SQL and driver errors never enter diagnostics."
  (:require [clojure.string :as str]
            [datalevin.query.source :as spi])
  (:import [java.lang AutoCloseable]
           [java.sql Connection DriverManager PreparedStatement ResultSet SQLException]
           [java.util Properties]
           [java.util.concurrent ArrayBlockingQueue Callable Executor ExecutorService
            Future ThreadFactory ThreadPoolExecutor TimeUnit TimeoutException]))

(defn fail! [error phase]
  (throw (ex-info "PostgreSQL query source failed" {:error error :phase phase})))

(defn ensure! [condition phase]
  (when-not condition (fail! :query-source/invalid phase)))

(defn identifier [s]
  (ensure! (and (string? s) (<= 1 (count s) 63) (not (str/includes? s "\u0000"))) :mapping)
  (str "\"" (str/replace s "\"" "\"\"") "\""))

(defn relation [table]
  (ensure! (and (vector? table) (= 2 (count table))) :mapping)
  (str/join "." (map identifier table)))

(def ^:private threads
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. runnable "federation-postgres-connect") (.setDaemon true)))))

;; Bound both abandoned pool checkouts and queued work. A late connection is
;; closed by its checkout task, even if the driver ignores interruption.
(defonce ^:private ^ExecutorService connector
  (doto (ThreadPoolExecutor. 8 8 30 TimeUnit/SECONDS (ArrayBlockingQueue. 16) threads)
    (.allowCoreThreadTimeOut true)))

(def ^:private abort-executor
  (reify Executor
    (execute [_ runnable]
      (.start (doto (Thread. runnable "federation-postgres-abort") (.setDaemon true))))))

(defn abort! [^Connection conn]
  (when conn (try (.abort conn abort-executor) (catch Exception _ nil))))

(defn close! [resource]
  (when resource (try (.close ^AutoCloseable resource) (catch Exception _ nil))))

(defn profile! [profile]
  (let [profile (merge {:connect-timeout-ms 10000 :statement-timeout-ms 10000
                        :fetch-size 128 :max-row-bytes 16384 :fusion? true} profile)]
    (ensure! (= 1 (count (filter some? ((juxt :jdbc-url :data-source) profile)))) :profile)
    (when-let [url (:jdbc-url profile)]
      (ensure! (and (string? url) (str/starts-with? url "jdbc:postgresql:")) :profile))
    (when-let [pool (:data-source profile)]
      (ensure! (instance? javax.sql.DataSource pool) :profile))
    (doseq [k [:connect-timeout-ms :statement-timeout-ms :max-row-bytes]]
      (ensure! (and (integer? (k profile)) (<= 1 (k profile) 1048576)) :profile))
    (ensure! (and (integer? (:fetch-size profile)) (<= 1 (:fetch-size profile) 256)
                  (boolean? (:fusion? profile))
                  (every? (fn [[k v]] (and (string? k) (string? v))) (:properties profile))) :profile)
    profile))

(defn check! [{:keys [control cancelled]}]
  (when control (spi/check-active! control))
  (when @cancelled (fail! :query-source/cancelled :execution)))

(defn connect! [{:keys [profile] :as context}]
  (check! context)
  (let [result (atom nil)
        task (try
               (.submit connector
                 ^Callable
                 (reify Callable
                   (call [_]
                     (let [conn (if-let [pool (:data-source profile)]
                                  (.getConnection ^javax.sql.DataSource pool)
                                  (let [props (Properties.)]
                                    (doseq [[k v] (:properties profile)] (.setProperty props k v))
                                    (.setProperty props "connectTimeout"
                                                  (str (max 1 (quot (+ 999 (:connect-timeout-ms profile)) 1000))))
                                    (DriverManager/getConnection (:jdbc-url profile) props)))]
                       (if (compare-and-set! result nil conn) conn
                           (do (close! conn) nil))))))
               (catch java.util.concurrent.RejectedExecutionException _
                 (fail! :query-source/limit :acquisition)))
        deadline (+ (System/nanoTime) (* 1000000 (long (:connect-timeout-ms profile))))]
    (try
      (loop []
        (check! context)
        (when (>= (System/nanoTime) deadline) (fail! :query/timeout :acquisition))
        (let [r (try [:done (.get ^Future task 20 TimeUnit/MILLISECONDS)]
                     (catch TimeoutException _ [:waiting]))]
          (if (= :waiting (first r)) (recur)
              (do (check! context) (second r)))))
      (catch Throwable e
        (let [old (first (swap-vals! result (constantly ::abandoned)))]
          (when (instance? Connection old) (close! old)))
        (.cancel ^Future task true)
        (if (instance? clojure.lang.ExceptionInfo e) (throw e)
            (let [cause (or (.getCause e) e)
                  state (when (instance? SQLException cause) (.getSQLState ^SQLException cause))]
              (fail! (if (and state (str/starts-with? state "28"))
                       :query-source/unauthorized :query-source/provider) :acquisition)))))))

(defn translate! [context phase ^SQLException error]
  (check! context)
  (let [state (.getSQLState error)]
    (fail! (cond
             (= "57014" state) :query/timeout
             (or (= "42501" state) (and state (str/starts-with? state "28"))) :query-source/unauthorized
             (and state (str/starts-with? state "22")) :query-source/invalid
             (or (#{"57P01" "57P02" "57P03"} state)
                 (and state (str/starts-with? state "08"))) :query-source/snapshot-lost
             (and state (str/starts-with? state "42")) :query-source/stale-definition
             :else :query-source/provider) phase)))

(defn request!
  "Admit a real round trip. The callback fully consumes its bounded result."
  [{:keys [limiter metrics] :as context} phase f]
  (check! context)
  (let [permit (when limiter (spi/acquire-request! limiter))]
    (when metrics (swap! metrics update :external-requests inc))
    (try (let [result (f)] (check! context) result)
         (catch SQLException e (translate! context phase e))
         (finally (close! permit)))))

(defn statement ^PreparedStatement [{:keys [connection profile control]} sql params]
  (let [stmt (.prepareStatement ^Connection connection sql)]
    (try
      (.setQueryTimeout stmt (int (max 1 (quot (+ 999 (min (:statement-timeout-ms profile)
                                                          (or (when control (spi/remaining-time-ms control))
                                                              Long/MAX_VALUE))) 1000))))
      (doseq [[i x] (map-indexed vector params)] (.setObject stmt (inc (int i)) x))
      stmt
      (catch Throwable e (close! stmt) (throw e)))))

(defn execute! [context phase sql params]
  (request! context phase
    #(with-open [stmt (statement context sql params)] (.execute stmt))))

(defn query! [context phase sql params read-row]
  (request! context phase
    #(with-open [stmt (statement context sql params) rs (.executeQuery stmt)]
       (loop [out []]
         (check! context)
         (if (.next rs)
           (do (when (>= (count out) 2048) (fail! :query-source/limit phase))
               (recur (conj out (read-row rs))))
           out)))))

(defn catalog!
  "Inspect one ordinary table. Unsupported physical types remain visible."
  [context table]
  (relation table)
  (let [[schema name] table
        tables (query! context :discovery
                 (str "SELECT c.oid::bigint, c.relkind::text, c.relrowsecurity, current_database() "
                      "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                      "WHERE n.nspname=? AND c.relname=?") [schema name]
                 (fn [^ResultSet r] {:oid (.getLong r 1) :kind (.getString r 2)
                                    :row-security? (.getBoolean r 3) :database (.getString r 4)}))
        _ (ensure! (= 1 (count tables)) :discovery)
        info (first tables)
        columns (query! context :discovery
                  (str "SELECT a.attname, t.typname, a.atttypid::bigint, a.atttypmod, a.attnotnull, "
                       "a.attcollation::bigint FROM pg_catalog.pg_attribute a "
                       "JOIN pg_catalog.pg_type t ON t.oid=a.atttypid "
                       "WHERE a.attrelid=?::oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum")
                  [(:oid info)]
                  (fn [^ResultSet r] [(.getString r 1)
                                     {:type (.getString r 2) :type-id (.getLong r 3) :modifier (.getInt r 4)
                                      :not-null? (.getBoolean r 5) :collation (.getLong r 6)}]))
        keys (query! context :discovery
               (str "SELECT a.attname, c.condeferrable, c.convalidated FROM pg_catalog.pg_constraint c "
                    "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY k(attnum, ord) "
                    "JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum "
                    "WHERE c.conrelid=?::oid AND c.contype='p' ORDER BY k.ord") [(:oid info)]
               (fn [^ResultSet r] {:column (.getString r 1) :deferred? (.getBoolean r 2)
                                  :validated? (.getBoolean r 3)}))]
    (assoc info :table table :columns (into (sorted-map) columns) :primary-key keys)))
