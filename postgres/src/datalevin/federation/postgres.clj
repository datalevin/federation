(ns datalevin.federation.postgres
  "Read-only PostgreSQL relations with validated keys and replayable snapshot sessions."
  (:require [datalevin.federation.mapping :as mapping]
            [datalevin.federation.postgres.captured :as captured]
            [datalevin.federation.postgres.jdbc :as jdbc :refer [ensure! fail!]]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.federation.postgres.relational :as relational]
            [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl QueryKey$KeySet]
           [java.sql Connection ResultSet SQLException]
           [java.util UUID]))

(def accept-mapping
  "Accept discovery metadata, a source UUID, relation/identity attribute IDs,
  and an attribute map. Values are column names or {:columns [...] :reference
  {:domain ... :type ...}}. Tables require a validated nondeferrable primary key;
  views require declared :key-columns, checked against each captured snapshot.
  This function performs no I/O."
  pm/accept)

(defn discover
  "Discover one [schema table] using :jdbc-url/:properties or :data-source.
  Returns portable catalog metadata, including unsupported physical types.
  Owns and closes its connection; never closes a caller-owned pool."
  [profile table]
  (let [context {:profile (jdbc/profile! profile) :cancelled (atom false)}]
    (with-open [conn (jdbc/connect! context)]
      (let [context (assoc context :connection conn)]
        (try (jdbc/begin! context) (jdbc/catalog! context table)
             (finally (.rollback ^Connection conn)))))))

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

(defn- reserve-cursor! [ctl accepted candidate bindings profile]
  (let [fixed (* 4 (value/value-charge [candidate bindings]))]
    (loop [n (:fetch-size profile)]
      (let [bytes (+ fixed (if (:relational? candidate)
                            (+ 131072 (* n (+ (* 16 (:max-row-bytes profile))
                                              (* 1024 (get-in candidate [:query :physical-columns])) 2048)))
                            (plan/fetch-charge accepted candidate n (:max-row-bytes profile))))
            result (try
                     (spi/reserve! ctl {:retained-bytes bytes})
                     (catch clojure.lang.ExceptionInfo e
                       (if (and (> n 1) (= :query-source/limit (:error (ex-data e)))
                                (= :retained-bytes (:category (ex-data e))))
                         nil (throw e))))]
        ;; Failed reservations charge nothing. Reduce the page before any SQL
        ;; runs, retaining the same row cap and shared query memory limit.
        (if result [result n] (recur (max 1 (quot n 2))))))))

(defn- next-row! [{:keys [accepted candidate inputs context state metrics]}]
  (loop []
    (jdbc/check! context)
    (when (:closed? @state) (fail! :query-source/invalid :fetch))
    (let [inputs @inputs candidate @candidate
          {:keys [pending remaining portal more? input-index input-start projection snapshot?]} @state]
      (cond
        pending pending
        (seq remaining)
        (let [[ordinal row] (first remaining)
              input (when inputs (nth inputs (+ input-start ordinal)))
              bindings (merge (:bindings candidate)
                              (zipmap (map :variable (:requires candidate)) (:values input)))]
          (when snapshot? (swap! metrics update :read-rows inc))
          (swap! state assoc :remaining (next remaining))
          (if-let [result (if (:relational? candidate) row (plan/match-row accepted candidate bindings row))]
            (let [pending {:row result :id (:binding-id input)}]
              (swap! state assoc :pending pending) pending)
            (recur)))
        (and portal more?)
        (let [n (get-in context [:profile :fetch-size])
              rows (jdbc/query! context :fetch (str "FETCH FORWARD " n " FROM " portal) []
                                (fn [^ResultSet rs]
                                  [(if inputs (.getInt rs 1) 0)
                                   (if (:relational? candidate) (relational/read-row (:query candidate) rs)
                                       (plan/read-row accepted projection rs (if inputs 1 0)))]))]
          (swap! metrics update :read-rows + (count rows))
          (swap! state assoc :remaining (seq rows) :more? (= n (count rows)))
          (recur))
        portal (do (close-portal! context state) (recur))
        (< input-index (if inputs (count inputs) 1))
        (let [end (if inputs (min (count inputs) (+ input-index (:max-request-binding-rows plan/constraints))) 1)
              max-row-bytes (get-in context [:profile :max-row-bytes])
              query (cond (:relational? candidate) (:query candidate)
                          inputs (plan/batch-sql accepted candidate (subvec inputs input-index end) max-row-bytes)
                          :else (plan/sql accepted candidate (:bindings candidate) max-row-bytes))
              portal (str "dtlv_" (.replace (str (UUID/randomUUID)) "-" ""))]
          (jdbc/execute! context :open (str "DECLARE " portal " NO SCROLL CURSOR FOR " (:sql query)) (:params query))
          (swap! state assoc :portal portal :projection (select-keys query [:columns :presence-columns]) :more? true
                 :input-index end :input-start input-index)
          (recur))
        :else nil))))

(defrecord Cursor [accepted candidate inputs context state metrics reservation session-state]
  spi/IQuerySourceCursor
  (-next-batch! [this {:keys [control max-rows max-bytes]}]
    (ensure! (and (identical? control (:control context)) (pos-int? max-rows) (pos-int? max-bytes)) :fetch)
    (let [started (System/nanoTime) columns (:columns @candidate)
          dependent? (boolean (seq (:requires @candidate)))
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
        (reset! candidate nil)
        (reset! state {:closed? true})
        (spi/release! reservation)
        (swap! session-state update :cursors disj this)))
    nil))

(defn- cursor! [session accepted candidate bindings capture]
  (let [{:keys [context state request]} session
        [token fetch-size] (reserve-cursor! (:control request) accepted candidate bindings
                                            (cond-> (:profile context) capture (assoc :fetch-size 1)))]
    (try
      (let [metrics (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0 :read-bytes nil
                           :external-requests 0 :elapsed-ns 0})
            owned-inputs (when bindings (value/owned-copy bindings))
            owned-candidate (value/owned-copy candidate)
            context (-> context (assoc :metrics metrics) (assoc-in [:profile :fetch-size] fetch-size))
            row-source (when capture (captured/row-source! context accepted capture owned-candidate))
            cursor (->Cursor accepted (atom owned-candidate) (atom owned-inputs) context
                             (atom (if capture
                                     {:snapshot? true :input-start 0 :input-index (if owned-inputs (count owned-inputs) 1)
                                      :remaining (mapcat (fn [i]
                                                           (let [bindings (merge (:bindings owned-candidate)
                                                                            (zipmap (map :variable (:requires owned-candidate))
                                                                                    (:values (when owned-inputs (nth owned-inputs i)))))]
                                                             (map #(vector i %) (row-source bindings))))
                                                         (range (if owned-inputs (count owned-inputs) 1)))}
                                     {:input-index 0})) metrics token state)]
        (swap! state update :cursors conj cursor)
        cursor)
      (catch Throwable e (spi/release! token) (throw e)))))

(defn- compile-relational! [ctl views request max-row-bytes decline-on-limit?]
  (let [n (count (filter #(= :pattern (:kind %)) (:clauses request)))]
    (when (<= 1 n 32)
      (let [token (try (spi/reserve! ctl {:retained-bytes (+ 1048576 (* 32768 n n))})
                       (catch clojure.lang.ExceptionInfo e
                         (when-not (and decline-on-limit? (= :query-source/limit (:error (ex-data e)))
                                        (= :retained-bytes (:category (ex-data e)))) (throw e))))]
        (when token
          (try ((if (= :relational-fragment (:mode request)) relational/compile-fragment relational/compile-query)
                 views request max-row-bytes)
               (finally (spi/release! token))))))))

(defn- open-fragment! [session open]
  (let [{:keys [state request views context]} session]
    (locking state
      (active! session)
      (let [plan (:plan open) sources (:sources plan)
            query (assoc (:query plan) :sources sources)
            inputs (:inputs query)
            accepted (into {} (map (fn [{:keys [symbol definition-fingerprint]}]
                                     (let [view (views definition-fingerprint)]
                                       (ensure! view :open) [symbol (:accepted view)]))) sources)
            descriptor (get-in views [(:definition-fingerprint open) :descriptor])]
        (ensure! (and (= (select-keys request common) (select-keys open common))
                      (= "relational-fragment" (:alternative-id open)) (empty? (:cursors @state))
                      (= :relational-fragment (:mode query)) descriptor
                      (= (:source-symbol open) (:symbol (first sources)))
                      (= (:definition-fingerprint open) (:definition-fingerprint (first sources)))
                      (= (:scope-id open) (get-in descriptor [:scope :id])) (= 0 (:region-id open))
                      (vector? inputs) (seq inputs) (vector? (:bindings open))
                      (<= 1 (count (:bindings open)) (min 256 (get-in request [:limits :binding-rows])))) :open)
        (when (> (spi/binding-charge-bytes inputs (:bindings open))
                 (min 262144 (get-in request [:limits :binding-bytes])))
          (fail! :query-source/limit :open))
        (let [compiled (compile-relational! (:control request) accepted (assoc query :input-bindings (:bindings open))
                                           (get-in context [:profile :max-row-bytes]) false)]
          (ensure! (and compiled (= (:columns open) (:columns compiled))) :open)
          (cursor! session (accepted (:symbol (first sources)))
                   {:relational? true :query compiled :columns (:columns compiled) :requires [] :bindings {}} nil nil))))))

(defrecord Session [views request context info state reservations registration stats]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [this open]
    (locking state
      (active! this)
      (let [candidate (get-in @state [:plans (:plan open)])
            view (get views (:definition-fingerprint open))
            dependent? (boolean (seq (:requires candidate)))]
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
        (cursor! this (:accepted view) candidate (when dependent? (:bindings open))
                 (when (:snapshot view) view)))))
  (-cancel-session! [_ _]
    (reset! (:cancelled context) true)
    (jdbc/abort! (:connection context))
    nil)
  (-close-session! [_]
    (locking state
      (let [[before _] (swap-vals! state assoc :closed? true)]
        (when-not (:closed? before)
          (run! spi/-close-cursor! (:cursors before))
          (spi/unregister-cancel! registration)
          (try (.rollback ^Connection (:connection context)) (catch Exception _ nil))
          (jdbc/close! (:connection context))
          (jdbc/close! (:limiter context))
          (doseq [view (vals views)]
            (when-let [snapshot (:snapshot view)]
              (captured/close! (:indexes view))
              (reset! snapshot nil)))
          (run! spi/release! @reservations)
          (reset! reservations [])
          (reset! state {:closed? true})
          (swap! stats update :live-sessions dec))))
    nil)
  spi/IRelationalQuerySession
  (-open-relational-cursor [this open]
    (locking state
      (active! this)
      (ensure! (and (= (select-keys request common) (select-keys open common))
                    (empty? (:cursors @state)) (= "relational" (:alternative-id open))
                    (not (contains? open :bindings))) :open)
      (let [plan (:plan open)
            sources (:sources plan)
            accepted (into {} (map (fn [{:keys [symbol definition-fingerprint]}]
                                     (let [view (views definition-fingerprint)]
                                       (ensure! view :open)
                                       [symbol (:accepted view)]))) sources)
            descriptor (get-in views [(:definition-fingerprint open) :descriptor])
            query (assoc (:query plan) :sources sources)
            compiled (compile-relational! (:control request) accepted query (get-in context [:profile :max-row-bytes]) false)]
        (ensure! (and descriptor (= (:source-symbol open) (:symbol (first sources)))
                      (= (:definition-fingerprint open) (:definition-fingerprint (first sources)))
                      (= (:scope-id open) (get-in descriptor [:scope :id])) (= 0 (:region-id open))
                      compiled (= (:columns open) (:columns compiled))) :open)
        (cursor! this (accepted (:symbol (first sources)))
                 {:relational? true :query compiled :columns (:columns compiled) :requires [] :bindings {}} nil nil))))
  spi/IRelationalFragmentSession
  (-open-relational-fragment-cursor [this open] (open-fragment! this open)))

(defn- descriptor [accepted profile]
  (assoc (mapping/source-info accepted (:compute-context-id profile))
         :read-capabilities #{:snapshot}
         ::accepted accepted))

(defn- capture!
  "Capture one bounded owned relation before publishing a replayable session."
  [context accepted reservations]
  (let [query (plan/snapshot-sql accepted (get-in context [:profile :max-row-bytes]))
        [workspace n] (reserve-cursor! (:control context) accepted {:snapshot? true} nil (:profile context))
        portal (str "dtlv_" (.replace (str (UUID/randomUUID)) "-" ""))
        state (atom {}) seen (QueryKey$KeySet.)]
    (try
      (jdbc/execute! context :acquisition (str "DECLARE " portal " NO SCROLL CURSOR FOR " (:sql query)) [])
      (reset! state {:portal portal})
      (loop [out []]
        (let [rows (jdbc/query! context :acquisition (str "FETCH FORWARD " n " FROM " portal) []
                                #(plan/read-row accepted query %))
              out (reduce (fn [out row]
                            (jdbc/check! context)
                            (let [key (mapv row (pm/key-columns accepted))]
                              (when-not (every? some? key) (fail! :query-source/invalid :identity))
                              ;; Reserve both retained rows and duplicate-key tracking before ownership.
                              (let [token (spi/reserve! (:control context)
                                            {:retained-bytes (+ 512 (* 4 (value/value-charge [row key])))})]
                                (swap! reservations conj token)
                                (when-not (.add seen key) (fail! :query-source/invalid :identity))
                                (conj out (value/owned-copy row))))) out rows)]
          (if (= n (count rows)) (recur out) out)))
      (finally (close-portal! context state) (spi/release! workspace)))))

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
                             [(:definition-fingerprint d) (cond-> {:descriptor d :accepted accepted}
                                                         (pm/spooled? accepted) (assoc :snapshot (atom nil) :indexes (atom {})))])))
                    (:source-descriptors request))
            context {:profile profile :control ctl :cancelled cancelled :limiter @limiter :metrics stats}
            conn (jdbc/request! context :acquisition #(let [conn (jdbc/connect! context)]
                                                       (reset! connection conn) conn))
            context (assoc context :connection conn)]
        (jdbc/check! context)
        (jdbc/begin! context)
        ;; Lock every relation before the first catalog SELECT pins the snapshot.
        (doseq [view (sort-by #(pr-str (get-in % [:accepted :requirements :table])) (vals views))]
          (let [kind (get-in view [:accepted :requirements :kind])
                table (jdbc/relation (get-in view [:accepted :requirements :table]))]
            (if (= "m" kind)
              ;; PostgreSQL rejects LOCK TABLE for materialized views. A zero-row
              ;; SELECT holds ACCESS SHARE without transferring relation data.
              (jdbc/query! context :acquisition (str "SELECT 1 FROM " table " LIMIT 0") [] (constantly nil))
              (jdbc/execute! context :acquisition
                             (str "LOCK TABLE " (when (= "r" kind) "ONLY ") table " IN ACCESS SHARE MODE") []))))
        (doseq [view (vals views)]
          (pm/verify! (:accepted view) (jdbc/catalog! context (get-in view [:accepted :requirements :table]))))
        (doseq [view (vals views)]
          (when-let [snapshot (:snapshot view)]
            (reset! snapshot (capture! context (:accepted view) reservations))))
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
  spi/IRelationalQuerySource
  (-plan-relational [_ request]
    (when (:relational? profile)
      (let [sources (:sources request)
            views (into {} (map (fn [{:keys [symbol descriptor]}] [symbol (::accepted descriptor)])) sources)]
        (when (and (= descriptor (:descriptor (first sources))) (every? some? (vals views))
                   (compile-relational! (:control request) views request (:max-row-bytes profile) true))
          {:columns (:columns request)
           :plan {:sources (mapv (fn [{:keys [symbol descriptor]}]
                                  {:symbol symbol :definition-fingerprint (:definition-fingerprint descriptor)}) sources)
                  :query (select-keys request [:mode :semantics-version :clauses :bindings :find :with :columns])}}))))
  spi/IRelationalFragmentSource
  (-plan-relational-fragment [_ request]
    (when (and (:relational? profile) (= :relational-fragment (:mode request)) (seq (:inputs request)))
      (let [sources (:sources request)
            views (into {} (map (fn [{:keys [symbol descriptor]}] [symbol (::accepted descriptor)])) sources)]
        (when (and (= descriptor (:descriptor (first sources))) (every? some? (vals views))
                   (compile-relational! (:control request) views request (:max-row-bytes profile) true))
          {:columns (:columns request) :constraints {:max-binding-rows 256 :max-binding-bytes 262144}
           :plan {:sources (mapv (fn [{:keys [symbol descriptor]}]
                                  {:symbol symbol :definition-fingerprint (:definition-fingerprint descriptor)}) sources)
                  :query (select-keys request [:mode :semantics-version :clauses :bindings :find :with :columns :inputs])}}))))
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
             (let [predicates (if (pm/spooled? accepted) [] (plan/predicates accepted candidate request))
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
  Optional :fusion? false offers separate EAV accesses for conformance checks.
  Set :relational? false to keep joins and aggregation in the core engine."
  [accepted profile]
  (pm/compiled! accepted)
  (ensure! (some? (:compute-context-id profile)) :profile)
  (let [profile (jdbc/profile! profile) d (descriptor accepted profile)]
    (spi/validate-source-info! d)
    (->PostgresSource d accepted profile (atom {:external-requests 0 :live-sessions 0}))))

(defn statistics
  "Cumulative acquisition requests and live sessions; no connection details."
  [source] @(:stats source))
