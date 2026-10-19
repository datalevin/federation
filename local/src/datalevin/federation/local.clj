(ns datalevin.federation.local
  "Declared-schema local relations. Every invocation pins one immutable version."
  (:require [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [java.util UUID]
           [datalevin.utl QueryKey]))

(defn- fail! [error phase]
  (throw (ex-info "Local query source failed" {:error error :phase phase})))

(defn- pattern-columns [descriptor clause]
  (let [types (get-in descriptor [:pattern-contract :attributes (second (second (:terms clause)))])]
    (when-not types (fail! :query-source/unsupported :clauses))
    [{:type (:entity-type types) :nullable? false}
     {:type :keyword :nullable? false}
     {:type (:value-type types) :nullable? false}]))

(defn- accepted-rows [descriptor facts]
  (when-not (vector? facts) (fail! :query-source/invalid :local-input))
  (doseq [fact facts]
    (when-not (and (vector? fact) (= 3 (count fact))) (fail! :query-source/invalid :local-input))
    (let [columns (pattern-columns descriptor {:terms [[:ignore] [:const (nth fact 1)] [:ignore]]})]
      (when (and (some? (first fact)) (some? (nth fact 2))) (value/rows! columns [fact]))))
  (value/owned-copy facts))

(defn- active! [state control]
  (when (:closed? @state)
    (throw (ex-info "Local source is closed" {:error :query-source/invalid :state :closed})))
  (when (:cancelled? @state) (fail! :query-source/cancelled :execution))
  (spi/check-active! control))

(defn- limit! [category maximum attempted]
  (throw (ex-info "Local value exceeds resource limit"
                  {:error :query-source/limit
                   :phase (if (= category :batch-bytes) :batch :reserve)
                   :category category :maximum maximum :attempted attempted})))

(defn- bounded-charge
  "Stop sizing at the first excess charge or cancellation, before copying.
  The final atomic reservation still checks concurrent/shared ledger use."
  [check! initial scale maximum overflow! measure!]
  (let [bytes (volatile! initial)
        observe! (fn [increment]
                   (check!)
                   (let [total (+' @bytes (*' scale increment))]
                     (vreset! bytes total)
                     (when-not (value/at-most? total (long maximum))
                       (overflow! total))))]
    (observe! 0)
    (measure! observe!)
    @bytes))

(defn- retained-charge [request check! overhead scale x]
  (let [maximum (get-in request [:limits :retained-bytes] Long/MAX_VALUE)]
    (bounded-charge check! overhead scale maximum
                    #(limit! :retained-bytes maximum %)
                    #(value/value-charge x %))))

(defn- match-row [bindings terms row]
  (reduce (fn [bound [term x]]
            (when bound
              (case (first term)
                :ignore bound
                :const (when (QueryKey/equiv (second term) x) bound)
                :var (let [sym (second term)]
                       (if (contains? bound sym)
                         (when (if (contains? bindings sym)
                                 (QueryKey/equals (bound sym) x)
                                 (QueryKey/equiv (bound sym) x)) bound)
                         (assoc bound sym x)))))) bindings (map vector terms row)))

(defn- unknown-estimate [dependent?]
  (merge (zipmap (if dependent?
                  [:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
                   :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes]
                  [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes])
                (repeat nil))
         {:confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}))

(defn- columns [info clause]
  (reduce (fn [out [term column]]
            (if (and (= :var (first term)) (not-any? #(= (second term) (:variable %)) out))
              (conj out (assoc column :variable (second term))) out))
          [] (map vector (:terms clause) (pattern-columns info clause))))

(defn- choices [info request config]
  (mapcat
    (fn [clause]
      (let [available (into {} (map (juxt :variable identity)) (:available-inputs request))
            cols (columns info clause)
            make (fn [kind position]
                   (let [[tag x] (when position (nth (:terms clause) position))
                         column (some #(when (= x (:variable %)) %) cols)
                         requires (if (and position (= :var tag) (not (contains? (:bindings request) x)))
                                    [column] [])]
                     (when (or (nil? position) (= :const tag) (contains? (:bindings request) x)
                               (and column (= column (available x))))
                       {:id (keyword (name kind) (str (:id clause))) :clause clause :columns cols
                        :requires requires :kind kind})))
            modes (case (get config :mode :both)
                    :scan [[:attribute-enumeration nil]]
                    :lookup [[:entity-lookup 0]]
                    :value [[:value-lookup 2]]
                    :both [[:attribute-enumeration nil] [:value-lookup 2] [:entity-lookup 0]])]
        (keep (fn [[kind position]] (make kind position)) modes)))
    (filter #(= :pattern (:kind %)) (:clauses request))))

(def ^:private empty-vector-charge (value/value-charge []))
(def ^:private batch-full (ex-info "Local batch boundary" {}))
(def ^:private constraints {:max-binding-rows 256 :max-binding-bytes 262144 :max-request-binding-rows 256})

(defn- next-match [plan facts inputs position check! metrics]
  (let [dependent? (boolean (seq (:requires plan)))
        input-count (if dependent? (count inputs) 1)]
    (loop [b (if dependent? (first position) 0), f (if dependent? (second position) position)]
      (check!)
      (cond
        (= b input-count) nil
        (= f (count facts)) (when dependent? (recur (inc b) 0))
        :else
        (let [fact (nth facts f) input (when dependent? (nth inputs b))
              bindings (if dependent?
                         (merge (:bindings plan) (zipmap (map :variable (:requires plan)) (:values input)))
                         (:bindings plan))
              match (when (and (some? (first fact)) (some? (nth fact 2)))
                      (match-row bindings (:terms (:clause plan)) fact))]
          (swap! metrics update :read-rows inc)
          (if match
            {:position (if dependent? [b (inc f)] (inc f))
             :row (mapv match (map :variable (:columns plan))) :id (:binding-id input)}
            (recur b (inc f))))))))

(defn- at-end? [facts inputs dependent? position]
  (if dependent?
    (or (= (first position) (count inputs))
        (and (= (first position) (dec (count inputs))) (= (second position) (count facts))))
    (= position (count facts))))

(defn- batch-window [plan facts inputs start check! metrics max-rows max-bytes]
  ;; The sizing pass borrows one projected row at a time from the pinned view.
  ;; No unadmitted result buffer is built, including for dependent bindings.
  (let [dependent? (boolean (seq (:requires plan))) columns (:columns plan)]
    (loop [p start, n 0, bytes 0]
      (if (or (= n max-rows) (at-end? facts inputs dependent? p))
        {:end p :row-count n :byte-count bytes :exhausted? (at-end? facts inputs dependent? p)}
        (if-let [{:keys [position row id]} (next-match plan facts inputs p check! metrics)]
          (let [candidate
                (try
                  (bounded-charge check!
                    (if (zero? n) 0 (- bytes (* (if dependent? 2 1) empty-vector-charge)))
                    1 max-bytes
                    (fn [attempted] (if (zero? n) (limit! :batch-bytes max-bytes attempted) (throw batch-full)))
                    (fn [observe!]
                      (if (zero? n)
                        (if dependent? (value/dependent-batch-charge-bytes columns [row] [id] observe!)
                            (value/batch-charge-bytes columns [row] observe!))
                        (do (value/value-charge [row] observe!)
                            (when dependent? (value/value-charge [id] observe!))))))
                  (catch Throwable e (when-not (identical? e batch-full) (throw e)) nil))]
            (if candidate (recur position (inc n) (long candidate))
                {:end p :row-count n :byte-count bytes :exhausted? false}))
          {:end (if dependent? [(count inputs) 0] (count facts)) :row-count n
           :byte-count bytes :exhausted? true})))))

(defn- copy-window [plan facts inputs start n check! metrics]
  (loop [p start, remaining n, rows (transient []), ids (when (seq (:requires plan)) (transient []))]
    (if (zero? remaining)
      {:rows (persistent! rows) :ids (when ids (persistent! ids))}
      (let [{:keys [position row id]} (next-match plan facts inputs p check! metrics)]
        (when-not row (fail! :query-source/invalid :batch))
        (recur position (dec remaining) (conj! rows (value/owned-copy row check!))
               (when ids (conj! ids id)))))))

(defrecord Cursor [snapshot plan bindings request position state session-state metrics reservation]
  spi/IQuerySourceCursor
  (-next-batch! [_ {:keys [control max-rows max-bytes]}]
    (active! state control)
    (active! session-state control)
    (when-not (and (identical? control (:control request))
                  (integer? max-rows) (<= 1 max-rows Long/MAX_VALUE)
                  (integer? max-bytes) (<= 1 max-bytes Long/MAX_VALUE))
      (fail! :query-source/invalid :fetch))
    (let [started (System/nanoTime) facts @snapshot inputs (when bindings @bindings)
          dependent? (boolean (seq (:requires plan))) columns (:columns plan)
          check! #(do (active! state control) (active! session-state control))
          start @position]
      (try
        (let [{:keys [end row-count byte-count exhausted?]}
              (batch-window plan facts inputs start check! metrics (long max-rows) (long max-bytes))
              ;; A dependent empty final batch still owns its ID vector.
              empty-bytes (when (and dependent? (zero? row-count))
                            (bounded-charge check! 0 1 max-bytes #(limit! :batch-bytes max-bytes %)
                              #(value/dependent-batch-charge-bytes columns [] [] %)))
              byte-count (or empty-bytes byte-count)
              token (when (pos? byte-count)
                      (spi/reserve! control {:retained-bytes byte-count :in-flight-batches 1}))]
          (try
            (let [{:keys [rows ids]} (copy-window plan facts inputs start row-count check! metrics)
                  bytes (bounded-charge check! 0 1 byte-count #(limit! :batch-bytes byte-count %)
                          (fn [observe!]
                            (if dependent? (value/dependent-batch-charge-bytes columns rows ids observe!)
                                (value/batch-charge-bytes columns rows observe!))))]
              (when-not (= bytes byte-count) (fail! :query-source/invalid :batch))
              (check!)
              (reset! position end)
              (swap! metrics #(-> % (update :batches inc) (update :rows + row-count) (update :charged-bytes + bytes)))
              (cond-> {:columns columns :rows rows :row-count row-count :byte-count bytes
                       :reservation token :exhausted? exhausted?}
                dependent? (assoc :binding-ids ids)))
            (catch Throwable e (when token (spi/release! token)) (throw e))))
        (finally (swap! metrics update :elapsed-ns + (- (System/nanoTime) started))))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (reset! snapshot nil)
      (when bindings (reset! bindings nil))
      (spi/release! reservation))
    nil))

(defrecord Session [owner descriptor snapshot request info state plans cursors reservations]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [_ open]
    (active! state (:control request))
    (when-not (= (:definition-fingerprint descriptor) (:definition-fingerprint open))
      (fail! :query-source/stale-definition :open))
    (let [plan (get @plans (:plan open)) dependent? (boolean (seq (:requires plan)))
          common [:control :query-id :spi-version :features :authorization :consistency :limits]
          ctl (:control request) check! #(active! state ctl)]
      (when-not (and plan (= (:id plan) (:alternative-id open)) (= (:source-symbol plan) (:source-symbol open))
                    (= (:scope-id plan) (:scope-id open)) (= (:region-id plan) (:region-id open))
                    (= (select-keys request common) (select-keys open common)))
        (fail! :query-source/invalid :open))
      (if dependent?
        (let [maximum (min (get-in request [:limits :binding-bytes]) (:max-binding-bytes constraints))]
          (when-not (and (vector? (:bindings open))
                         (<= 1 (count (:bindings open))
                             (min (get-in request [:limits :binding-rows]) (:max-binding-rows constraints))))
            (fail! :query-source/invalid :open))
          (bounded-charge check! 0 1 maximum #(limit! :binding-bytes maximum %)
                          #(value/binding-charge-bytes (:requires plan) (:bindings open) %)))
        (when (contains? open :bindings) (fail! :query-source/invalid :open)))
      (let [scratch (retained-charge request check! 1024 2
                                    [(:terms (:clause plan)) (:columns plan) (:bindings plan) (:bindings open)])
            token (spi/reserve! ctl {:retained-bytes scratch})]
        (try
          (let [cursor (->Cursor (atom @snapshot) plan
                                 (when dependent? (atom (value/owned-copy (:bindings open) check!))) request
                                 (atom (if dependent? [0 0] 0)) (atom {}) state
                                 (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0 :read-bytes nil
                                        :external-requests 0 :elapsed-ns 0}) token)]
            (swap! cursors conj cursor)
            cursor)
          (catch Throwable e (spi/release! token) (throw e))))))
  (-cancel-session! [_ _] (swap! state assoc :cancelled? true) nil)
  (-close-session! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (run! spi/-close-cursor! @cursors)
      (reset! cursors []) (reset! plans {}) (reset! snapshot nil)
      (run! spi/release! @reservations) (reset! reservations []))
    nil))

(defrecord LocalSource [descriptor data options]
  spi/IQuerySource
  (-source-info [_] descriptor)
  (-open-session [this request]
    (spi/check-active! (:control request))
    (when-not (and (= 2 (:spi-version request))
                   (#{:snapshot :statement :best-effort} (:consistency request))
                   (= (:features descriptor) (:features request))
                   (vector? (:source-descriptors request)) (seq (:source-descriptors request))
                   (every? #(= descriptor %) (:source-descriptors request)))
      (fail! :query-source/invalid :acquisition))
    (let [{:keys [facts version]} @data
          bytes (retained-charge request #(spi/check-active! (:control request)) 2048 1 facts)
          token (spi/reserve! (:control request) {:retained-bytes bytes})]
      (try
        (->Session this descriptor (atom facts) request
          (merge (select-keys descriptor [:source-instance-id :compute-context-id])
                 {:spi-version 2 :features #{} :consistency :snapshot :session-id (UUID/randomUUID)
                  :authorization-token :local-caller :acquired-at-ms (System/currentTimeMillis) :data-version version
                  :consumption {:opens :many :max-open-cursors 1}
                  :request-constraints {:max-concurrent-requests 1 :rate-limit nil}})
          (atom {}) (atom {}) (atom []) (atom [token]))
        (catch Throwable e (spi/release! token) (throw e)))))
  (-plan-source [this session request]
    (let [ctl (:control request) check! #(active! (:state session) ctl)]
      (check!)
      (when-not (and (identical? this (:owner session))
                    (= descriptor (:source-descriptor request)) (= 2 (:spi-version request))
                    (identical? ctl (get-in session [:request :control]))
                    (= (:query-id request) (get-in session [:request :query-id]))
                    (= (:scope-id request) (get-in descriptor [:scope :id])))
        (fail! :query-source/invalid :planning))
      {:alternatives
       (mapv (fn [{:keys [id clause columns requires kind] :as candidate}]
               (let [plan (merge candidate (select-keys request [:source-symbol :region-id :scope-id :bindings]))
                     bytes (retained-charge request check! 1024 2 plan)
                     token (spi/reserve! ctl {:retained-bytes bytes})]
                 (try
                   (let [handle (Object.) plan (value/owned-copy plan check!)]
                     (swap! (:reservations session) conj token)
                     (swap! (:plans session) assoc handle plan)
                     {:id id :plan handle :covers #{(:id clause)} :prefilters #{} :requires requires :columns columns
                      :scope-id (:scope-id request) :completion :all-bindings
                      :access {:kind kind :anchor-clause (:id clause)}
                      :properties {:replay :identical :ordering [] :blocking-state :none}
                      :constraints constraints :estimate (unknown-estimate (boolean (seq requires)))
                      :explain {:label "Local declared-schema access"}})
                   (catch Throwable e (spi/release! token) (throw e)))))
             (choices descriptor request options))})))

(defn source
  "Create a caller-owned finite EAV view. Mutable leaves and metadata are owned.
  :mode selects offered accesses (:both, :scan, :lookup or :value). Open sessions
  pin their own immutable version; absence emits no fact."
  ([descriptor facts] (source descriptor facts {}))
  ([descriptor facts options]
   (spi/validate-source-info! descriptor)
   (when-not (and (empty? (:features descriptor)) (empty? (:required-features descriptor))
                  (#{:both :scan :lookup :value} (get options :mode :both))
                  (every? #{:mode} (keys options)))
     (fail! :query-source/unsupported :descriptor))
   (->LocalSource descriptor (atom {:facts (accepted-rows descriptor facts) :version (UUID/randomUUID)}) options)))

(defn replace-rows!
  "Atomically publish an owned EAV fact version without changing its definition."
  [^LocalSource source facts]
  (reset! (:data source) {:facts (accepted-rows (:descriptor source) facts) :version (UUID/randomUUID)})
  nil)
