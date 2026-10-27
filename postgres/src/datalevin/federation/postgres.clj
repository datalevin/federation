(ns datalevin.federation.postgres
  "Read-only PostgreSQL tables with accepted primary keys and snapshot sessions."
  (:require [datalevin.federation.mapping :as mapping]
            [datalevin.federation.postgres.jdbc :as jdbc :refer [ensure! fail!]]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [java.sql Connection SQLException]
           [java.util UUID]))

(def accept-mapping
  "Accept discovery metadata, a source UUID, relation/identity attribute IDs,
  and an attribute map. Values are column names or {:columns [...] :reference
  {:domain ... :type ...}}. Only a validated nondeferrable primary key establishes
  durable identity. This function performs no I/O."
  pm/accept)

(defn discover
  "Discover one [schema table] using :jdbc-url/:properties or :data-source.
  Returns portable catalog metadata, including unsupported physical types.
  Owns and closes its connection; never closes a caller-owned pool."
  [profile table]
  (let [context {:profile (jdbc/profile! profile) :cancelled (atom false)}]
    (with-open [conn (jdbc/connect! context)]
      (jdbc/catalog! (assoc context :connection conn) table))))

(def ^:private common [:control :query-id :spi-version :features :authorization :consistency :limits])
(def ^:private request-constraints {:max-concurrent-requests 1 :rate-limit nil})
(def ^:private empty-charge (value/value-charge []))

(defn- active! [session]
  (jdbc/check! (:context session))
  (when (:closed? @(:state session)) (fail! :query-source/invalid :execution)))

(defn- close-portal! [context state]
  (when-let [portal (:portal @state)]
    (swap! state dissoc :portal)
    ;; CLOSE and transaction rollback release backend resources even after the
    ;; read budget/deadline is exhausted. They do not retrieve provider data.
    (try
      (with-open [stmt (jdbc/statement context (str "CLOSE " portal) [])] (.execute stmt))
      (catch Exception _ nil))))

(defn- next-row! [{:keys [accepted candidate inputs context state metrics]}]
  (loop []
    (jdbc/check! context)
    (when (:closed? @state) (fail! :query-source/invalid :fetch))
    (let [inputs @inputs
          {:keys [pending remaining portal more? input-index current-bindings binding-id columns]} @state]
      (cond
        pending pending
        (seq remaining)
        (let [row (first remaining)]
          (swap! state assoc :remaining (next remaining))
          (if-let [result (plan/match-row accepted candidate current-bindings row)]
            (let [pending {:row result :id binding-id}]
              (swap! state assoc :pending pending) pending)
            (recur)))
        (and portal more?)
        (let [n (get-in context [:profile :fetch-size])
              rows (jdbc/query! context :fetch (str "FETCH FORWARD " n " FROM " portal) []
                                #(plan/read-row accepted columns %))]
          (swap! metrics update :read-rows + (count rows))
          (swap! state assoc :remaining (seq rows) :more? (= n (count rows)))
          (recur))
        portal (do (close-portal! context state) (recur))
        (< input-index (if inputs (count inputs) 1))
        (let [input (when inputs (nth inputs input-index))
              bindings (merge (:bindings candidate)
                              (zipmap (map :variable (:requires candidate)) (:values input)))
              query (plan/sql accepted candidate bindings (get-in context [:profile :max-row-bytes]))
              portal (str "dtlv_" (.replace (str (UUID/randomUUID)) "-" ""))]
          (jdbc/execute! context :open (str "DECLARE " portal " NO SCROLL CURSOR FOR " (:sql query)) (:params query))
          (swap! state assoc :portal portal :columns (:columns query) :more? true
                 :input-index (inc input-index) :current-bindings bindings :binding-id (:binding-id input))
          (recur))
        :else nil))))

(defrecord Cursor [accepted candidate inputs context state metrics reservation session-state]
  spi/IQuerySourceCursor
  (-next-batch! [this {:keys [control max-rows max-bytes]}]
    (ensure! (and (identical? control (:control context)) (pos-int? max-rows) (pos-int? max-bytes)) :fetch)
    (let [started (System/nanoTime) columns (:columns candidate)
          dependent? (boolean (seq (:requires candidate)))
          charge (fn [rows ids] (if dependent? (spi/batch-charge-bytes columns rows ids)
                                   (spi/batch-charge-bytes columns rows)))]
      (locking state
        (try
          (jdbc/check! context)
          (let [first-row (next-row! this)
                first-bytes (if first-row (charge [(:row first-row)] [(:id first-row)]) (charge [] []))
                _ (when (> first-bytes max-bytes) (fail! :query-source/limit :batch))
                window (max first-bytes (min 65536 max-bytes))
                token (spi/reserve! control {:retained-bytes window :in-flight-batches 1})]
            (try
              (loop [rows [] ids [] bytes (charge [] [])]
                (let [row (when (< (count rows) max-rows) (next-row! this))
                      attempted (when row
                                  (if (empty? rows)
                                    (charge [(:row row)] [(:id row)])
                                    (+ bytes (- (value/value-charge [(:row row)]) empty-charge)
                                       (if dependent? (- (value/value-charge [(:id row)]) empty-charge) 0))))]
                  (if (and row (<= attempted window))
                    (do (swap! state dissoc :pending)
                        (recur (conj rows (value/owned-copy (:row row)))
                               (if dependent? (conj ids (:id row)) ids) attempted))
                    (let [exhausted? (and (< (count rows) max-rows) (nil? row))]
                      (jdbc/check! context)
                      (swap! metrics #(-> % (update :batches inc) (update :rows + (count rows))
                                          (update :charged-bytes + bytes)))
                      (cond-> {:columns columns :rows rows :row-count (count rows) :byte-count bytes
                               :reservation token :exhausted? (boolean exhausted?)}
                        dependent? (assoc :binding-ids ids))))))
              (catch Throwable e (spi/release! token) (throw e))))
          (finally (swap! metrics update :elapsed-ns + (- (System/nanoTime) started)))))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [this]
    (locking state
      (when-not (:closed? @state)
        (close-portal! context state)
        (reset! inputs nil)
        (reset! state {:closed? true})
        (spi/release! reservation)
        (swap! session-state update :cursors disj this)))
    nil))

(defrecord Session [views request context info state reservations registration stats]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [this open]
    (locking state
      (active! this)
      (let [candidate (get-in @state [:plans (:plan open)])
            view (get views (:definition-fingerprint open))
            dependent? (boolean (seq (:requires candidate)))
            ctl (:control request)]
        (when-not view (fail! :query-source/stale-definition :open))
        (ensure! (and candidate (= (:definition-fingerprint candidate) (:definition-fingerprint open))
                      (= (:id candidate) (:alternative-id open))
                      (= (select-keys candidate [:source-symbol :region-id :scope-id])
                         (select-keys open [:source-symbol :region-id :scope-id]))
                      (= (select-keys request common) (select-keys open common))
                      (empty? (:cursors @state))) :open)
        (if dependent?
          (do (ensure! (and (vector? (:bindings open))
                            (<= 1 (count (:bindings open))
                                (min 256 (get-in request [:limits :binding-rows])))) :open)
              (when (> (spi/binding-charge-bytes (:requires candidate) (:bindings open))
                       (min 262144 (get-in request [:limits :binding-bytes])))
                (fail! :query-source/limit :open)))
          (ensure! (not (contains? open :bindings)) :open))
        (let [profile (:profile context)
              columns (count (get-in view [:accepted :requirements :columns]))
              bytes (+ 131072 (* (:fetch-size profile) (+ (* 16 (:max-row-bytes profile))
                                                          (* 1024 columns) 2048))
                       (* 4 (value/value-charge [candidate (:bindings open)])))
              token (spi/reserve! ctl {:retained-bytes bytes})]
          (try
            (let [metrics (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0 :read-bytes nil
                                 :external-requests 0 :elapsed-ns 0})
                  cursor (->Cursor (:accepted view) candidate
                                   (atom (when dependent? (value/owned-copy (:bindings open))))
                                   (assoc context :metrics metrics)
                                   (atom {:input-index 0}) metrics token state)]
              (swap! state update :cursors conj cursor)
              cursor)
            (catch Throwable e (spi/release! token) (throw e)))))))
  (-cancel-session! [_ _]
    (reset! (:cancelled context) true)
    (jdbc/abort! (:connection context))
    nil)
  (-close-session! [_]
    (let [[before _] (swap-vals! state assoc :closed? true)]
      (when-not (:closed? before)
        (run! spi/-close-cursor! (:cursors before))
        (spi/unregister-cancel! registration)
        (try (.rollback ^Connection (:connection context)) (catch Exception _ nil))
        (jdbc/close! (:connection context))
        (jdbc/close! (:limiter context))
        (run! spi/release! @reservations)
        (reset! reservations [])
        (reset! state {:closed? true})
        (swap! stats update :live-sessions dec)))
    nil))

(defn- descriptor [accepted profile]
  (assoc (mapping/source-info accepted (:compute-context-id profile))
         :read-capabilities #{:snapshot}
         ::accepted accepted))

(defn- acquire [source request]
  (let [{:keys [profile stats]} source
        ctl (:control request)
        cancelled (atom false) connection (atom nil)
        registration (spi/register-cancel! ctl (fn [_] (reset! cancelled true) (jdbc/abort! @connection)))
        reservations (atom []) limiter (atom nil)]
    (try
      (spi/check-active! ctl)
      (ensure! (and (= 2 (:spi-version request)) (= #{} (:features request))
                    (seq (:source-descriptors request))) :acquisition)
      (swap! reservations conj (spi/reserve! ctl {:retained-bytes (+ 8388608
                                                (* 4 (value/value-charge (:source-descriptors request))))}))
      (reset! limiter (spi/request-limiter ctl request-constraints))
      (let [views (into {}
                    (map (fn [d]
                           (let [accepted (::accepted d)]
                             (pm/compiled! accepted)
                             (ensure! (and (= d (descriptor accepted profile))
                                           (= (:source-instance-id d) (get-in source [:descriptor :source-instance-id])))
                                      :acquisition)
                             [(:definition-fingerprint d) {:descriptor d :accepted accepted}])))
                    (:source-descriptors request))
            context {:profile profile :control ctl :cancelled cancelled :limiter @limiter :metrics stats}
            conn (jdbc/request! context :acquisition #(let [conn (jdbc/connect! context)]
                                                       (reset! connection conn) conn))
            context (assoc context :connection conn)]
        (jdbc/check! context)
        (ensure! (.getAutoCommit ^Connection conn) :acquisition)
        (.setReadOnly ^Connection conn true)
        (.setAutoCommit ^Connection conn false)
        (jdbc/execute! context :acquisition "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY" [])
        ;; SET/LOCK utility statements do not pin the data snapshot. Lock every
        ;; accepted relation before the first catalog SELECT, so a concurrent
        ;; DDL commit cannot hide behind an earlier repeatable-read snapshot.
        (jdbc/execute! context :acquisition "SET LOCAL search_path = pg_catalog" [])
        (jdbc/execute! context :acquisition
                       (str "SET LOCAL statement_timeout = " (:statement-timeout-ms profile)) [])
        (doseq [view (sort-by #(pr-str (get-in % [:accepted :requirements :table])) (vals views))]
          (jdbc/execute! context :acquisition
                         (str "LOCK TABLE ONLY " (jdbc/relation (get-in view [:accepted :requirements :table]))
                              " IN ACCESS SHARE MODE") []))
        (doseq [view (vals views)]
          (pm/verify! (:accepted view) (jdbc/catalog! context (get-in view [:accepted :requirements :table]))))
        (let [session (->Session views request context
                        (merge (select-keys (:descriptor source) [:source-instance-id :compute-context-id])
                               {:spi-version 2 :features #{} :consistency :snapshot :session-id (UUID/randomUUID)
                                :authorization-token :postgres-profile :acquired-at-ms (System/currentTimeMillis)
                                :data-version nil :consumption {:opens :many :max-open-cursors 1}
                                :request-constraints request-constraints})
                        (atom {:plans {} :cursors #{}}) reservations registration stats)]
          (swap! stats update :live-sessions inc)
          session))
      (catch Throwable e
        (spi/unregister-cancel! registration)
        (jdbc/close! @connection)
        (jdbc/close! @limiter)
        (run! spi/release! @reservations)
        (if (instance? SQLException e)
          (jdbc/translate! {:control ctl :cancelled cancelled} :acquisition e)
          (throw e))))))

(defrecord PostgresSource [descriptor accepted profile stats]
  spi/IQuerySource
  (-source-info [_] descriptor)
  (-open-session [this request] (acquire this request))
  (-plan-source [_ session request]
    (let [state (:state session) ctl (:control request)]
      (locking state
        (active! session)
        (ensure! (and (= descriptor (:source-descriptor request))
                      (= descriptor (get-in session [:views (:definition-fingerprint descriptor) :descriptor]))
                      (= (select-keys request common) (select-keys (:request session) common))
                      (= (:scope-id request) (get-in descriptor [:scope :id]))) :planning)
        {:alternatives
         (mapv
           (fn [{:keys [id anchor columns requires kind] :as candidate}]
             (let [predicates (plan/predicates accepted candidate request)
                   candidate (merge candidate (select-keys request [:source-symbol :region-id :scope-id :bindings])
                                    {:predicates predicates :definition-fingerprint (:definition-fingerprint descriptor)})
                   token (spi/reserve! ctl {:retained-bytes (+ 4096 (* 4 (value/value-charge candidate)))})]
               (try
                 (let [handle (Object.)]
                   (swap! state assoc-in [:plans handle] (value/owned-copy candidate))
                   (swap! (:reservations session) conj token)
                   {:id id :plan handle :covers (set (map :id (:clauses candidate)))
                    :prefilters (set (map :id predicates)) :requires requires :columns columns
                    :scope-id (:scope-id request) :completion :all-bindings
                    :access {:kind kind :anchor-clause (:id anchor)}
                    :properties {:replay :identical :ordering [] :blocking-state :none}
                    :constraints plan/constraints :estimate (plan/unknown-estimate (boolean (seq requires)))
                    :explain {:label "PostgreSQL table access"}})
                 (catch Throwable e (spi/release! token) (throw e)))))
           (plan/choices descriptor request (:fusion? profile)))}))))

(defn source
  "Reopen accepted metadata without I/O. Runtime profiles require a public
  :compute-context-id and either :jdbc-url (with optional string :properties)
  or :data-source. Tables sharing a database/profile share one read transaction.
  Optional :fusion? false offers separate EAV accesses for conformance checks."
  [accepted profile]
  (pm/compiled! accepted)
  (ensure! (some? (:compute-context-id profile)) :profile)
  (let [profile (jdbc/profile! profile) d (descriptor accepted profile)]
    (spi/validate-source-info! d)
    (->PostgresSource d accepted profile (atom {:external-requests 0 :live-sessions 0}))))

(defn statistics
  "Cumulative acquisition requests and live sessions; no connection details."
  [source] @(:stats source))
