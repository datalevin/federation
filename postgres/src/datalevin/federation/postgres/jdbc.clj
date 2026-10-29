(ns datalevin.federation.postgres.jdbc
  "Private PostgreSQL transport. SQL and driver errors never enter diagnostics."
  (:require [clojure.string :as str]
            [datalevin.query.source :as spi]
            [datalevin.federation.fingerprint :as fp])
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
                        :fetch-size 128 :max-row-bytes 16384 :fusion? true :relational? true} profile)]
    (ensure! (= 1 (count (filter some? ((juxt :jdbc-url :data-source) profile)))) :profile)
    (when-let [url (:jdbc-url profile)]
      (ensure! (and (string? url) (str/starts-with? url "jdbc:postgresql:")) :profile))
    (when-let [pool (:data-source profile)]
      (ensure! (instance? javax.sql.DataSource pool) :profile))
    (doseq [k [:connect-timeout-ms :statement-timeout-ms :max-row-bytes]]
      (ensure! (and (integer? (k profile)) (<= 1 (k profile) 1048576)) :profile))
    (ensure! (and (integer? (:fetch-size profile)) (<= 1 (:fetch-size profile) 256)
                  (boolean? (:fusion? profile)) (boolean? (:relational? profile))
                  (every? (fn [[k v]] (and (string? k) (string? v))) (:properties profile))) :profile)
    (when-let [role (:role profile)] (identifier role))
    (ensure! (and (or (nil? (:session-settings profile)) (map? (:session-settings profile)))
                  (<= (count (:session-settings profile)) 32)
                  (every? (fn [[k v]] (and (string? k) (<= (count k) 63) (re-matches #"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*" k)
                                           (string? v) (<= (count v) 4096)
                                           (not (str/includes? v "\u0000")))) (:session-settings profile))) :profile)
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

(defn begin!
  "Establish one transaction-local authorization and decoding context."
  [{:keys [connection profile] :as context}]
  (ensure! (.getAutoCommit ^Connection connection) :acquisition)
  (.setReadOnly ^Connection connection true)
  (.setAutoCommit ^Connection connection false)
  (execute! context :acquisition "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY" [])
  (execute! context :acquisition "SET LOCAL search_path = pg_catalog" [])
  (execute! context :acquisition (str "SET LOCAL statement_timeout = " (:statement-timeout-ms profile)) [])
  (execute! context :acquisition "SET LOCAL row_security = on" [])
  (execute! context :acquisition "SET LOCAL IntervalStyle = iso_8601" [])
  (when-let [role (:role profile)]
    (execute! context :acquisition (str "SET LOCAL ROLE " (identifier role)) []))
  (execute! context :acquisition "SET LOCAL standard_conforming_strings = on" [])
  (doseq [[k v] (sort (:session-settings profile))]
    (execute! context :acquisition
              (str "SET LOCAL " (identifier k) " = '" (str/replace v "'" "''") "'") [])))

(declare relation-dependencies!)

(defn- security! [context oid]
  (let [names (vec (sort (keys (get-in context [:profile :session-settings]))))
        auth (first (query! context :discovery
                      (str "SELECT current_user, session_user, row_security_active(?::oid), "
                           "(SELECT jsonb_agg(jsonb_build_array(oid,rolsuper,rolinherit,rolbypassrls,rolconfig) ORDER BY oid)::text FROM pg_catalog.pg_roles "
                           "WHERE pg_has_role(current_user, oid, 'USAGE'))") [oid]
                      (fn [^ResultSet r] {:role (.getString r 1) :session-user (.getString r 2)
                                         :active? (.getBoolean r 3) :memberships (.getString r 4)})))
        policies (query! context :discovery
                   (str "SELECT polname, polcmd::text, polpermissive, polroles::text, "
                        "pg_get_expr(polqual,polrelid),pg_get_expr(polwithcheck,polrelid) "
                        "FROM pg_catalog.pg_policy WHERE polrelid=?::oid ORDER BY polname") [oid]
                   (fn [^ResultSet r] (mapv #(.getString r (int %)) (range 1 7))))]
    {:authorization {:role (:role auth) :session-user (:session-user auth)
                     :memberships (fp/fingerprint :definition (:memberships auth))
                     :context-id (get-in context [:profile :compute-context-id])
                     :setting-names names
                     :settings (fp/fingerprint :definition (select-keys (get-in context [:profile :session-settings]) names))}
     :active? (:active? auth) :policies (fp/fingerprint :definition policies)
     :dependencies (relation-dependencies! context oid)}))

(defn- relation-dependencies! [context oid]
  ;; Follow nested views, fingerprinting catalog-visible relations, policies,
  ;; and directly referenced functions. SQL text stays inside PostgreSQL.
  (first
    (query! context :discovery
      (str "WITH RECURSIVE rels(oid) AS (SELECT ?::oid UNION "
           "SELECT d.refobjid FROM rels r JOIN pg_catalog.pg_rewrite w ON w.ev_class=r.oid "
           "JOIN pg_catalog.pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=w.oid "
           "AND d.refclassid='pg_class'::regclass), "
           "objects AS (SELECT 'r' AS kind,c.oid::bigint AS oid, "
           "jsonb_build_array(c.relkind,c.relowner,c.reloptions,c.relrowsecurity,c.relforcerowsecurity, "
           "CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid,true) END, "
           "(SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attisdropped) ORDER BY a.attnum) "
           "FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0), "
           "(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.polname) FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid)) AS definition "
           "FROM rels JOIN pg_catalog.pg_class c USING(oid) UNION "
           "SELECT 'f',p.oid::bigint,to_jsonb(p) FROM pg_catalog.pg_proc p "
           "WHERE p.oid IN (SELECT d.refobjid FROM pg_catalog.pg_depend d WHERE d.refclassid='pg_proc'::regclass "
           "AND ((d.classid='pg_rewrite'::regclass AND d.objid IN (SELECT w.oid FROM pg_catalog.pg_rewrite w JOIN rels r ON r.oid=w.ev_class)) "
           "OR (d.classid='pg_policy'::regclass AND d.objid IN (SELECT q.oid FROM pg_catalog.pg_policy q JOIN rels r ON r.oid=q.polrelid))))) "
           "SELECT encode(sha256(convert_to(jsonb_agg(jsonb_build_array(kind,oid,definition) ORDER BY kind,oid)::text,'UTF8')),'hex') FROM objects")
      [oid] (fn [^ResultSet r] (.getString r 1)))))

(defn catalog!
  "Inspect a named relation and its key, enum, view, and authorization contracts."
  [context table]
  (relation table)
  (let [[schema name] table
        tables (query! context :discovery
                 (str "SELECT c.oid::bigint, c.relkind::text, c.relrowsecurity, current_database(), ?::regclass::oid::bigint "
                      "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                      "WHERE n.nspname=? AND c.relname=?") [(relation table) schema name]
                 (fn [^ResultSet r]
                   (when-not (= (.getLong r 1) (.getLong r 5))
                     (fail! :query-source/stale-definition :acquisition))
                   {:oid (.getLong r 1) :kind (.getString r 2)
                    :row-security? (.getBoolean r 3) :database (.getString r 4)}))
        _ (ensure! (= 1 (count tables)) :discovery)
        info (first tables)
        columns (query! context :discovery
                  (str "SELECT a.attname, t.typname, a.atttypid::bigint, a.atttypmod, a.attnotnull, "
                       "a.attcollation::bigint, t.typtype::text, n.nspname, "
                       "(SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder) FROM pg_catalog.pg_enum e WHERE e.enumtypid=t.oid) "
                       "FROM pg_catalog.pg_attribute a "
                       "JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace "
                       "WHERE a.attrelid=?::oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum")
                  [(:oid info)]
                  (fn [^ResultSet r] [(.getString r 1)
                                     (cond-> {:type (.getString r 2) :type-id (.getLong r 3) :modifier (.getInt r 4)
                                              :not-null? (.getBoolean r 5) :collation (.getLong r 6)}
                                       (= "e" (.getString r 7))
                                       (assoc :enum {:schema (.getString r 8) :name (.getString r 2)
                                                     :labels (let [a (.getArray r 9)]
                                                               (try (vec (.getArray a)) (finally (.free a))))}))]))
        keys (query! context :discovery
               (str "SELECT a.attname, c.condeferrable, c.convalidated FROM pg_catalog.pg_constraint c "
                    "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY k(attnum, ord) "
                    "JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum "
                    "WHERE c.conrelid=?::oid AND c.contype='p' ORDER BY k.ord") [(:oid info)]
               (fn [^ResultSet r] {:column (.getString r 1) :deferred? (.getBoolean r 2)
                                  :validated? (.getBoolean r 3)}))
        security (when (or (:row-security? info) (#{"v" "m"} (:kind info))) (security! context (:oid info)))]
    (cond-> (assoc info :table table :columns (into (sorted-map) columns) :primary-key keys)
      (or (:row-security? info) (#{"v" "m"} (:kind info)))
      (assoc :security security)
      (#{"v" "m"} (:kind info))
      (assoc :view (assoc (first (query! context :discovery
                            (str "SELECT encode(sha256(convert_to(pg_get_viewdef(oid,true),'UTF8')),'hex'), relowner::bigint, "
                                 "reloptions::text, relispopulated FROM pg_catalog.pg_class WHERE oid=?::oid") [(:oid info)]
                            (fn [^ResultSet r] {:definition (.getString r 1) :owner (.getLong r 2)
                                               :options (.getString r 3) :populated? (.getBoolean r 4)})))
                          :dependencies (:dependencies security))))))
