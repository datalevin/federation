(ns datalevin.federation.local
  "Declared-schema local relations. Every invocation pins one immutable version."
  (:require [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [java.util UUID]
           [datalevin.utl QueryKey]))

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
    (value/owned-copy rows)))

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

(def ^:private empty-vector-charge (value/value-charge []))
(def ^:private batch-full (ex-info "Local batch boundary" {}))

(defn- batch-window [rows start columns project check! max-rows max-bytes]
  ;; Keep only one projected row at a time. Its values are borrowed from the
  ;; already charged snapshot/bindings; cursor scratch covers its containers.
  (loop [index (long start), n 0, bytes 0]
    (if (or (= index (count rows)) (= n max-rows))
      {:end index :row-count n :byte-count bytes}
      (if-let [row (project (nth rows index))]
        (let [candidate
              (try
                (bounded-charge check! (if (zero? n) 0 (- bytes empty-vector-charge))
                                1 max-bytes
                                (fn [attempted]
                                  (if (zero? n)
                                    (limit! :batch-bytes max-bytes attempted)
                                    (throw batch-full)))
                                (if (zero? n)
                                  #(value/batch-charge-bytes columns [row] %)
                                  ;; Derive each append's charge from the shared
                                  ;; helper, without allocating a row buffer.
                                  #(value/value-charge [row] %)))
                (catch Throwable e
                  (when-not (identical? e batch-full) (throw e))
                  nil))]
          (if candidate
            (recur (inc index) (inc n) (long candidate))
            ;; The oversized row remains for the next fetch; no partial size
            ;; or copy from it is retained in the returned batch.
            {:end index :row-count n :byte-count bytes}))
        (recur (inc index) n bytes)))))

(defn- copy-window [rows start end project check!]
  (loop [index (long start), owned (transient [])]
    (if (= index end)
      (persistent! owned)
      (recur (inc index)
             (if-let [row (project (nth rows index))]
               (conj! owned (value/owned-copy row check!))
               owned)))))

(defrecord Cursor [snapshot plan descriptor request position state session-state metrics reservation]
  spi/IQuerySourceCursor
  (-next-batch! [_ {:keys [control max-rows max-bytes]}]
    (active! state control)
    (active! session-state control)
    (when-not (and (identical? control (:control request))
                  (integer? max-rows) (<= 1 max-rows Long/MAX_VALUE)
                  (integer? max-bytes) (<= 1 max-bytes Long/MAX_VALUE))
      (fail! :query-source/invalid :fetch))
    (let [started (System/nanoTime)
          rows @snapshot
          columns (:columns plan)
          variables (mapv :variable columns)
          eav? (= :eav (get-in descriptor [:pattern-contract :kind]))
          check! #(active! session-state control)
          read-rows (volatile! 0)
          project (fn [row]
                    (active! session-state control)
                    (vswap! read-rows inc)
                    (when-let [binding (when-not (and eav? (or (nil? (get row 0)) (nil? (get row 2))))
                                         (match-row (:bindings plan) (:terms (:clause plan)) row))]
                      (mapv binding variables)))
          start @position]
      (try
        (let [{:keys [end row-count byte-count]}
              (batch-window rows start columns project check! (long max-rows) (long max-bytes))
              ;; Reserve once, before allocating the output vector or copying
              ;; mutable leaves. No row buffer is retained by the sizing pass.
              token (when (pos? row-count)
                      (spi/reserve! control {:retained-bytes byte-count :in-flight-batches 1}))]
          (try
            (let [owned (if token (copy-window rows start end project check!) [])
                  bytes (bounded-charge check! 0 1 byte-count
                                        #(limit! :batch-bytes byte-count %)
                                        #(value/batch-charge-bytes columns owned %))]
              ;; Report the exact output charge. The preflight reservation must
              ;; cover the owned copy, including its preserved metadata.
              (when-not (and (= row-count (count owned))
                             (value/at-most? bytes (long byte-count)))
                (fail! :query-source/invalid :batch))
              (spi/check-active! control)
              (reset! position end)
              (swap! metrics #(-> % (update :batches inc) (update :rows + row-count)
                                  (update :charged-bytes + bytes)))
              {:columns columns :rows owned :row-count row-count :byte-count bytes
               :reservation token :exhausted? (= end (count rows))})
            (catch Throwable e (when token (spi/release! token)) (throw e))))
        (finally
          (swap! metrics #(-> % (update :read-rows + @read-rows)
                              (update :elapsed-ns + (- (System/nanoTime) started))))))))
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
      (let [scratch (retained-charge request #(active! state (:control request)) 1024 2
                                      [(:terms (:clause plan)) (:columns plan)])
            reservation (spi/reserve! (:control request) {:retained-bytes scratch})
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
          ;; Include outer vector metadata, even for an empty row version.
          bytes (retained-charge request #(spi/check-active! (:control request))
                                 2048 1 rows)
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
                                (reduce (fn [m {:keys [variable] :as column}]
                                          ;; A repeated variable must satisfy every
                                          ;; occurrence, regardless of column order.
                                          (assoc m variable
                                                 (update column :nullable?
                                                         #(and % (get-in m [variable :nullable?] true)))))
                                        {}) vals vec)
                   bytes (retained-charge request #(active! (:state session) (:control request))
                                          1024 2 [columns clause (:bindings request)])
                   reservation (spi/reserve! (:control request) {:retained-bytes bytes})
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
