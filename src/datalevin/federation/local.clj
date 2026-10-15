(ns datalevin.federation.local
  "Declared-schema local relations. Every invocation pins one immutable version."
  (:require [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [java.util Date UUID]))

(defn- fail! [error phase]
  (throw (ex-info "Local query source failed" {:error error :phase phase})))

(defn- pattern-columns [descriptor clause]
  (let [contract (:pattern-contract descriptor)]
    (if (= :tuple (:kind contract)) (:columns contract)
        (let [types (get (:attributes contract) (second (second (:terms clause))))]
          (when-not types (fail! :query-source/unsupported :clauses))
          [{:type (:entity-type types) :nullable? false}
           {:type :keyword :nullable? false}
           {:type (:value-type types) :nullable? false}]))))

(defn- copy-value [x]
  (cond
    (bytes? x) (aclone ^bytes x)
    (= Date (class x)) (Date. (.getTime ^Date x))
    (vector? x) (mapv copy-value x)
    (value/plain-map? x) (into {} (map (fn [[k v]] [(copy-value k) (copy-value v)])) x)
    (set? x) (into #{} (map copy-value) x)
    (list? x) (apply list (map copy-value x))
    :else x))

(defn- accepted-rows [descriptor rows]
  (when-not (vector? rows) (fail! :query-source/unsupported :local-input))
  (let [eav? (= :eav (get-in descriptor [:pattern-contract :kind]))]
    (doseq [row rows]
      (when-not (vector? row) (fail! :query-source/invalid :local-input))
      (let [columns (if eav?
                      (pattern-columns descriptor
                                                {:terms [[:ignore] [:const (get row 1)] [:ignore]]})
                      (get-in descriptor [:pattern-contract :columns]))]
        ;; SQL-null/missing EAV attributes contribute no fact, including `_`.
        (when (and eav? (not= 3 (count row))) (fail! :query-source/invalid :local-input))
        (when-not (and eav? (or (nil? (get row 0)) (nil? (get row 2))))
          (value/rows! columns [row]))))
    (copy-value rows)))

(defn- active! [state control]
  (when (:closed? @state)
    (throw (ex-info "Local source is closed" {:error :query-source/invalid :state :closed})))
  (when (:cancelled? @state) (fail! :query-source/cancelled :execution))
  (spi/check-active! control))

(defn- match-row [bindings terms row]
  (reduce (fn [bindings [term x]]
            (when bindings
              (case (first term)
                :ignore bindings
                :const (when (= (second term) x) bindings)
                :var (let [sym (second term)]
                       (if (contains? bindings sym)
                         (when (= (bindings sym) x) bindings)
                         (assoc bindings sym x)))))) bindings (map vector terms row)))

(defrecord Cursor [snapshot plan descriptor request position state session-state metrics reservation]
  spi/IQuerySourceCursor
  (-next-batch! [_ {:keys [control max-rows max-bytes]}]
    (active! state control)
    (active! session-state control)
    (when-not (and (identical? control (:control request)) (pos-int? max-rows) (pos-int? max-bytes))
      (fail! :query-source/invalid :fetch))
    (let [started (System/nanoTime)
          rows @snapshot
          columns (:columns plan)
          eav? (= :eav (get-in descriptor [:pattern-contract :kind]))
          match (loop []
                  (spi/check-active! control)
                  (when (< @position (count rows))
                    (let [row (nth rows @position)]
                      (swap! position inc)
                      (swap! metrics update :read-rows inc)
                      (if-let [binding (when-not (and eav? (or (nil? (get row 0)) (nil? (get row 2))))
                                         (match-row (:bindings plan) (:terms (:clause plan)) row))]
                        (mapv binding (map :variable columns))
                        (recur)))))
          batch (if match [match] [])
          bytes (spi/batch-charge-bytes columns batch)]
      (when (> bytes max-bytes)
        (throw (ex-info "Local row exceeds batch limit"
                        {:error :query-source/limit :phase :batch :category :batch-bytes
                         :maximum max-bytes :attempted bytes})))
      ;; Single-row batches let the provider reserve the exact conservative
      ;; size before copying mutable leaves, without an unaccounted row buffer.
      (let [reservation (when match (spi/reserve! control {:retained-bytes bytes :in-flight-batches 1}))]
        (try
          (let [owned (copy-value batch)]
            (swap! metrics #(-> % (update :batches inc) (update :rows + (count owned))
                                (update :charged-bytes + bytes)
                                (update :elapsed-ns + (- (System/nanoTime) started))))
            {:columns columns :rows owned :row-count (count owned) :byte-count bytes
             :reservation reservation :exhausted? (= @position (count rows))})
          (catch Throwable e (when reservation (spi/release! reservation)) (throw e))))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [_]
    (swap! state assoc :closed? true)
    (reset! snapshot nil)
    (spi/release! reservation)
    nil))

(defrecord Session [owner descriptor snapshot request info state plans cursors reservation plan-reservations]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [_ cursor-request]
    (active! state (:control request))
    (when-not (= (:definition-fingerprint descriptor) (:definition-fingerprint cursor-request))
      (fail! :query-source/stale-definition :open))
    (let [plan (get @plans (:plan cursor-request))]
      (when-not (and plan (= (:id plan) (:alternative-id cursor-request))
                     (= (:source-symbol plan) (:source-symbol cursor-request)))
        (fail! :query-source/invalid :open))
      (let [reservation (spi/reserve! (:control request) {:retained-bytes 1024})
            cursor (->Cursor (atom @snapshot) plan descriptor request (atom 0) (atom {}) state
                             (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0
                                    :read-bytes nil :external-requests 0 :elapsed-ns 0}) reservation)]
        (swap! cursors conj cursor)
        cursor)))
  (-cancel-session! [_ _] (swap! state assoc :cancelled? true) nil)
  (-close-session! [_]
    (when-not (:closed? @state)
      (swap! state assoc :closed? true)
      (run! spi/-close-cursor! @cursors)
      (reset! cursors [])
      (reset! plans {})
      (run! spi/release! @plan-reservations)
      (reset! plan-reservations [])
      (reset! snapshot nil)
      (spi/release! reservation))
    nil))

(defrecord LocalSource [descriptor data]
  spi/IQuerySource
  (-source-info [_] descriptor)
  (-open-session [_ request]
    (spi/check-active! (:control request))
    (when-not (= :snapshot (:consistency request)) (fail! :query-source/unsupported :acquisition))
    (when-not (every? #(= descriptor %) (:source-descriptors request))
      (fail! :query-source/unsupported :acquisition))
    (let [rows @data
          ;; Charge the pinned version, including old versions kept alive by
          ;; replace-rows!, rather than treating retained snapshot data as free.
          bytes (reduce (fn [n row]
                          (spi/check-active! (:control request))
                          (+' n 48 (value/value-charge row))) 2048 rows)
          reservation (spi/reserve! (:control request) {:retained-bytes bytes})]
      (try
        (->Session data descriptor (atom rows) request
                   (merge (select-keys descriptor [:source-instance-id :compute-context-id])
                          (select-keys request [:spi-version :features :consistency])
                          {:session-id (UUID/randomUUID) :authorization-token :local-caller
                           :acquired-at-ms (System/currentTimeMillis) :data-version nil})
                   (atom {}) (atom {}) (atom []) reservation (atom []))
        (catch Throwable e (spi/release! reservation) (throw e)))))
  (-plan-source [_ session request]
    (active! (:state session) (:control request))
    (when-not (identical? data (:owner session)) (fail! :query-source/unsupported :planning))
    (when-not (= descriptor (:source-descriptor request))
      (fail! :query-source/stale-definition :planning))
    {:alternatives
     (mapv (fn [clause]
             (let [columns (->> (map vector (:terms clause) (pattern-columns descriptor clause))
                                (keep (fn [[term column]]
                                        (when (= :var (first term)) (assoc column :variable (second term)))))
                                (reduce (fn [m column] (assoc m (:variable column) column)) {}) vals vec)
                   reservation (spi/reserve! (:control request)
                                             {:retained-bytes (+' 1024 (*' 2 (value/value-charge
                                                                             [columns clause (:bindings request)])))})
                   _ (swap! (:plan-reservations session) conj reservation)
                   handle (Object.)
                   plan {:id (:id clause) :clause clause :columns columns
                         :bindings (:bindings request) :source-symbol (:source-symbol request)}]
               (swap! (:plans session) assoc handle plan)
               {:id (:id clause) :plan handle :covers #{(:id clause)} :prefilters #{} :requires []
                :columns columns :properties {:replay :reopen :ordering [] :blocking-state :none}
                :estimate {:rows nil :row-bytes nil :startup-ms nil :per-row-ns nil
                           :requests nil :confidence :unknown}
                :explain {:label "Local declared-schema scan"}}))
           (filter #(= :pattern (:kind %)) (:clauses request)))}))

(defn source
  "Create a caller-owned local source from a validated descriptor and vector of
  rows. Copies mutable leaves. The caller grants read access by passing the
  source to d/q; this adapter has no remote authorization or schema discovery."
  [descriptor rows]
  (spi/validate-source-info! descriptor)
  (when-not (and (= #{1} (:spi-versions descriptor))
                 (empty? (:features descriptor)) (empty? (:required-features descriptor)))
    (fail! :query-source/unsupported :descriptor))
  (->LocalSource descriptor (atom (accepted-rows descriptor rows))))

(defn replace-rows!
  "Atomically publish an owned row version. Open sessions retain their old view.
  The accepted descriptor and its definition fingerprint remain unchanged."
  [^LocalSource source rows]
  (reset! (:data source) (accepted-rows (:descriptor source) rows))
  nil)
