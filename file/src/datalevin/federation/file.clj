(ns datalevin.federation.file
  "Pinned EDN Lines exports. Files are verified and validated under the query
  quotas before any facts are published; each cursor streams the private copy."
  (:require [clojure.java.io :as io]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.file.format :as format]
            [datalevin.federation.mapping :as mapping]
            [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [java.io Closeable InputStream OutputStream]
           [java.nio.channels Channels FileChannel]
           [java.nio.file Files Path OpenOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute]
           [java.security DigestInputStream MessageDigest]
           [java.util HexFormat UUID]
           [datalevin.utl QueryKey]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- ensure! [condition phase]
  (when-not condition (format/fail! :query-source/invalid phase)))

(defn- hex [^bytes bytes] (.formatHex (HexFormat/of) bytes))

(defn- path-input ^InputStream [path]
  (Channels/newInputStream
    (FileChannel/open (.toPath (io/file path)) (into-array OpenOption [StandardOpenOption/READ]))))

(defn- manifest! [manifest]
  (ensure! (and (= #{:format :id :sha256 :bytes :records :max-record-bytes :max-depth}
                   (set (keys manifest)))
                 (= :edn-lines/v1 (:format manifest)) (uuid? (:id manifest))
                 (string? (:sha256 manifest)) (re-matches #"[0-9a-f]{64}" (:sha256 manifest))
                 (nat-int? (:bytes manifest)) (nat-int? (:records manifest))
                 (format/valid-limits? manifest)) :descriptor)
  manifest)

(defn export-token
  "Stable key component for this explicit export ID and verified content.
  A newly captured export uses a new UUID, including for identical content."
  [manifest]
  (manifest! manifest)
  (str (:id manifest) ":" (:sha256 manifest)))

(defn manifest
  "Explicitly inspect a local EDN Lines file, supplying its export UUID. Reads
  incrementally with bounded records/depth; does not generate identity on reopen.
  Optional :max-bytes caps inspection (default 1 GiB), :check-active! cooperates
  with the caller's cancellation/deadline; format limits are recorded."
  ([path export-id] (manifest path export-id {}))
  ([path export-id options]
   (let [limits (merge format/limits (select-keys options (keys format/limits)))
         max-bytes (get options :max-bytes 1073741824)
         check! (get options :check-active! (fn []
                                               (when (.isInterrupted (Thread/currentThread))
                                                 (throw (InterruptedException.)))))
         digest (MessageDigest/getInstance "SHA-256")
         bytes (volatile! 0)]
     (ensure! (and (uuid? export-id) (format/valid-limits? limits) (nat-int? max-bytes)
                   (ifn? check!)
                   (every? #{:max-record-bytes :max-depth :max-bytes :check-active!} (keys options))) :descriptor)
     (check!)
     (with-open [input (DigestInputStream. (path-input path) digest)]
       (let [reader (format/reader input limits check!
                     (fn [n]
                       (vreset! bytes (+ (long @bytes) (long n)))
                       (when (> (long @bytes) (long max-bytes)) (format/fail! :query-source/limit :acquisition))))
             records (loop [n 0] (if ((:read! reader)) (recur (inc n)) n))]
         (manifest! (merge limits {:format :edn-lines/v1 :id export-id :sha256 (hex (.digest digest))
                                  :bytes @bytes :records records})))))))

(defn- expression! [expression]
  (ensure! (and (vector? expression)
                (case (first expression)
                  :field (and (= 2 (count expression))
                              (or (string? (second expression)) (keyword? (second expression))))
                  :path (and (= 2 (count expression)) (vector? (second expression)) (seq (second expression))
                             (every? #(or (string? %) (keyword? %)) (second expression)))
                  false)) :descriptor))

(defn accept-mapping
  "Normalize an explicit export declaration and manifest to accepted v2.
  Required declaration keys: :relation-id, :source-instance-id, :domain,
  :identity-epoch, :identity-attribute, :scope, and F0 :attributes contracts.
  Field/path extraction preserves EDN types; conversion version 1 does no casts.
  Identity is [export-token, one-based physical record ordinal]."
  [declaration export]
  (manifest! export)
  (let [{:keys [relation-id source-instance-id domain identity-epoch identity-attribute attributes scope]} declaration]
    (ensure! (and (= #{:relation-id :source-instance-id :domain :identity-epoch :identity-attribute :attributes :scope}
                     (set (keys declaration)))
                   (keyword? domain) (nat-int? identity-epoch)) :descriptor)
    (let [accepted {:representation-version 2 :relation-id relation-id :source-instance-id source-instance-id
                    :identity {:type (mapping/entity-type ref/type-contract
                                       [source-instance-id domain identity-epoch] [:string :long])
                               :attribute identity-attribute :scope :export
                               :extraction {:shape :key-tuple :expressions [[:export-token] [:record-ordinal]]}}
                    :attributes attributes :scope scope :completion {:domain :finite :extent :all-matches}
                    :requirements (select-keys export [:format :max-record-bytes :max-depth])
                    :dependencies {:provider {:name :file/export :version 1} :conversion-version 1 :export export}}]
      (mapping/compile-logical-mapping accepted)
      (doseq [attribute (vals attributes)]
        (expression! (get-in attribute [:extraction :expression]))
        (ensure! (= {:version 1} (:conversion attribute)) :descriptor))
      accepted)))

(defn- compiled! [accepted]
  (let [compiled (mapping/compile-logical-mapping accepted)
        export (get-in accepted [:dependencies :export])]
    (manifest! export)
    (ensure! (and (= {:name :file/export :version 1} (get-in accepted [:dependencies :provider]))
                  (= 1 (get-in accepted [:dependencies :conversion-version]))
                  (= :export (get-in accepted [:identity :scope]))
                  (= [:string :long] (get-in accepted [:identity :type :parameters :key-types]))
                  (= [[:export-token] [:record-ordinal]] (get-in accepted [:identity :extraction :expressions]))
                  (= (:requirements accepted) (select-keys export [:format :max-record-bytes :max-depth]))) :descriptor)
    (doseq [attribute (vals (:attributes accepted))]
      (expression! (get-in attribute [:extraction :expression]))
      (ensure! (= {:version 1} (:conversion attribute)) :descriptor))
    compiled))

(defn- extract [row expression]
  (case (first expression)
    :field (get row (second expression))
    :path (reduce (fn [current key]
                    (cond (nil? current) (reduced nil)
                          (map? current) (get current key)
                          :else (format/fail! :query-source/invalid :values))) row (second expression))))

(defn- facts [compiled projector token ordinal row attribute]
  (try
    (if-let [contract (get-in compiled [:accepted :attributes attribute])]
      (projector [token ordinal] {attribute (extract row (get-in contract [:extraction :expression]))} #{attribute})
      (projector [token ordinal] {} #{}))
    (catch clojure.lang.ExceptionInfo e
      (if (#{:federation/mapping :federation/entity-ref} (:error (ex-data e)))
        (format/fail! :query-source/invalid :values) (throw e)))))

(defn- active! [state control]
  (spi/check-active! control)
  (when (:closed? @state) (format/fail! :query-source/invalid :execution))
  (when (:cancelled? @state) (format/fail! :query-source/cancelled :execution)))

(defn- cleanup! [actions]
  (let [failure (volatile! nil)]
    (doseq [action actions]
      (try (action) (catch Throwable e
                      (if-let [^Throwable old @failure] (.addSuppressed old e) (vreset! failure e)))))
    (when-let [e @failure] (throw e))))

(defn- close-once [resource]
  (let [owned (atom resource)]
    #(let [[resource _] (swap-vals! owned (constantly nil))]
       (when resource (.close ^Closeable resource)))))

(defn- scratch-bytes ^long [export]
  (+ 32768 (* 128 (long (:max-record-bytes export)))))

(defn- pin! [compiled projector profile request stats]
  (let [control (:control request)
        export (get-in compiled [:accepted :dependencies :export])
        token (export-token export)
        reservation (spi/reserve! control {:spill-bytes (:bytes export)
                                           :retained-bytes (scratch-bytes export)})
        path (volatile! nil)
        started (System/nanoTime)]
    (try
      (vreset! path (if-let [directory (:spool-directory profile)]
                     (Files/createTempFile (.toPath (io/file directory)) "dtlv-export-" ".ednl" (make-array FileAttribute 0))
                     (Files/createTempFile "dtlv-export-" ".ednl" (make-array FileAttribute 0))))
      (with-open [^Closeable limiter (spi/request-limiter control {:max-concurrent-requests 1 :rate-limit nil})
                  ^Closeable _ (spi/acquire-request! limiter)]
        (swap! stats update :acquisitions inc)
        (let [supplied (if-let [open (:open-input profile)] (open request) (path-input (:path profile)))
              _ (when-not (instance? InputStream supplied)
                  (when (instance? Closeable supplied) (.close ^Closeable supplied))
                  (ensure! false :acquisition))
              ^InputStream input supplied
              close-input! (close-once input)]
          (try
            (with-open [^OutputStream output (io/output-stream (.toFile ^Path @path))]
              (let [close-output! (close-once output)
                    registration (spi/register-cancel! control (fn [_] (cleanup! [close-input! close-output!])))
                    digest (MessageDigest/getInstance "SHA-256")
                    buffer (byte-array 8192)]
                (try
                  (loop [bytes 0]
                    (spi/check-active! control)
                    (let [n (.read input buffer 0 (alength buffer))]
                      (cond
                        (neg? n) (when-not (and (= bytes (:bytes export)) (= (hex (.digest digest)) (:sha256 export)))
                                   (format/fail! :query-source/snapshot-lost :acquisition))
                        (zero? n) (format/fail! :query-source/provider :acquisition)
                        :else
                        (let [total (+ bytes n)]
                          (swap! stats update :read-bytes + n)
                          (when (> total (long (:bytes export))) (format/fail! :query-source/snapshot-lost :acquisition))
                          (.update digest buffer 0 n)
                          (.write output buffer 0 n)
                          (recur total)))))
                  (finally (spi/unregister-cancel! registration)))))
            (finally (close-input!)))))
      ;; Validate all records before publishing the view, including rows that a
      ;; selective query or LIMIT would never reach. One record lives at a time.
      (with-open [input (path-input (.toFile ^Path @path))]
        (let [close-input! (close-once input)
              registration (spi/register-cancel! control (fn [_] (close-input!)))]
          (try
            (let [reader (format/reader input export #(spi/check-active! control) (fn [_]))
                  records (loop [ordinal 1]
                            (spi/check-active! control)
                            (if-let [row ((:read! reader))]
                              (do (doseq [attribute (keys (get-in compiled [:accepted :attributes]))]
                                    (spi/check-active! control)
                                    (facts compiled projector token ordinal row attribute))
                                  (recur (inc ordinal))) (dec ordinal)))]
              (when-not (= records (:records export)) (format/fail! :query-source/incomplete :acquisition))
              (swap! stats update :validated-records + records))
            (finally (spi/unregister-cancel! registration)))))
      (spi/check-active! control)
      {:path @path :reservation reservation}
      (catch Throwable e
        (swap! stats update :failures inc)
        (try (cleanup! [(fn [] (when @path (Files/deleteIfExists ^Path @path))) #(spi/release! reservation)])
             (catch Throwable cleanup-error (.addSuppressed e cleanup-error)))
        (spi/check-active! control)
        (throw e))
      (finally (swap! stats update :elapsed-ns + (- (System/nanoTime) started))))))

(defn- columns [descriptor clause]
  (let [attribute (second (second (:terms clause)))
        types (get-in descriptor [:pattern-contract :attributes attribute])]
    (ensure! types :clauses)
    (reduce (fn [out [term type]]
              (if (and (= :var (first term)) (not-any? #(= (second term) (:variable %)) out))
                (conj out {:variable (second term) :type type :nullable? false}) out))
            [] (map vector (:terms clause) [(:entity-type types) :keyword (:value-type types)]))))

(defn- matching-row [plan fact]
  (let [bindings (:bindings plan)
        match (reduce (fn [bound [term x]]
                        (when bound
                          (case (first term)
                            :ignore bound
                            :const (when (QueryKey/equiv (second term) x) bound)
                            :var (let [variable (second term)]
                                   (if (contains? bound variable)
                                     (when (if (contains? bindings variable)
                                             (QueryKey/equals (get bound variable) x)
                                             (QueryKey/equiv (get bound variable) x)) bound)
                                     (assoc bound variable x))))))
                      bindings (map vector (get-in plan [:clause :terms]) fact))]
    (when match (mapv match (map :variable (:columns plan))))))

(defn- next-row! [cursor check!]
  (let [{:keys [state metrics plan reader compiled projector token]} cursor]
    (loop []
      (check!)
      (let [{:keys [pending remaining eof? ordinal]} @state]
        (cond
          pending pending
          (seq remaining)
          (let [fact (first remaining)]
            (swap! state assoc :remaining (subvec remaining 1))
            (if-let [row (matching-row plan fact)]
              (do (swap! state assoc :pending row) row) (recur)))
          eof? nil
          :else
          (if-let [row ((:read! @reader))]
            (do (swap! metrics update :read-rows inc)
                (swap! state assoc :remaining (facts compiled projector token ordinal row
                                                    (second (second (get-in plan [:clause :terms]))))
                       :ordinal (inc (long ordinal)))
                (recur))
            (do (swap! state assoc :eof? true :remaining []) nil)))))))

(def ^:private empty-vector-charge (long (value/value-charge [])))

(defrecord Cursor [compiled projector token plan request reader close-input! state session-state metrics reservation deregister!]
  spi/IQuerySourceCursor
  (-next-batch! [this {:keys [control max-rows max-bytes]}]
    (ensure! (and (identical? control (:control request))
                  (pos-int? max-rows) (pos-int? max-bytes)) :fetch)
    (let [check! #(do (active! state control) (active! session-state control))
          started (System/nanoTime)
          columns (:columns plan)]
      (locking state
        (try
          (check!)
          (let [first-row (next-row! this check!)
                first-charge (when first-row (long (spi/batch-charge-bytes columns [first-row])))
                _ (when (and first-charge (> (long first-charge) (long max-bytes)))
                    (format/fail! :query-source/limit :batch))
                window (max (long (or first-charge 0)) (min (long max-bytes) 65536))
                ;; Admit the bounded output window before retaining any rows.
                ;; The reservation is conservative; byte-count measures values.
                output (when first-row (spi/reserve! control {:retained-bytes window :in-flight-batches 1}))]
            (try
              (let [[rows bytes]
                    (if-not first-row [[] 0]
                      (loop [rows [], bytes 0]
                        (check!)
                        (if (= (count rows) (long max-rows)) [rows bytes]
                          (if-let [row (next-row! this check!)]
                            (let [candidate (if (empty? rows) (long first-charge)
                                              (+ bytes (- (long (value/value-charge [row])) (long empty-vector-charge))))]
                              (if (> candidate window) [rows bytes]
                                (let [owned (value/owned-copy row check!)]
                                  (swap! state assoc :pending nil)
                                  (recur (conj rows owned) candidate))))
                            [rows bytes]))))
                    exhausted? (boolean (and (:eof? @state) (empty? (:remaining @state)) (nil? (:pending @state))))]
                (check!)
                (swap! metrics #(-> % (update :batches inc) (update :rows + (count rows))
                                     (update :charged-bytes + bytes)))
                {:columns columns :rows rows :row-count (count rows) :byte-count bytes
                 :reservation output :exhausted? exhausted?})
              (catch Throwable e (when output (spi/release! output)) (throw e))))
          (finally (swap! metrics update :elapsed-ns + (- (System/nanoTime) started)))))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [this]
    (let [[before _] (swap-vals! state assoc :closed? true :remaining [] :pending nil)]
      (when-not (:closed? before)
        (cleanup! [close-input! #(reset! reader nil) #(spi/release! reservation) #(deregister! this)])))
    nil))

(defrecord Session [owner descriptor compiled projector request info state reservations path registration stats]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [_ open]
    (let [control (:control request)]
      (locking state
        (active! state control)
        (when-not (= (:definition-fingerprint descriptor) (:definition-fingerprint open))
          (format/fail! :query-source/stale-definition :open))
        (let [plan (get-in @state [:plans (:plan open)])
              common [:control :query-id :spi-version :features :authorization :consistency :limits]]
          (ensure! (and plan (= (:id plan) (:alternative-id open))
                        (= (:source-symbol plan) (:source-symbol open))
                        (= (:scope-id plan) (:scope-id open)) (= (:region-id plan) (:region-id open))
                        (= (select-keys request common) (select-keys open common))
                        (not (contains? open :bindings)) (empty? (:cursors @state))) :open)
          (let [export (get-in compiled [:accepted :dependencies :export])
                reservation (spi/reserve! control {:retained-bytes (scratch-bytes export)})]
            (try
              (let [input (path-input (.toFile ^Path path))
                    close-input! (close-once input)]
                (try
                  (let [metrics (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0 :read-bytes 0
                                       :external-requests 0 :elapsed-ns 0})
                        cursor-state (atom {:remaining [] :ordinal 1})
                        check! #(do (active! state control) (active! cursor-state control))
                        reader (format/reader input export check!
                                 #(swap! metrics update :read-bytes + (long %)))
                        cursor (->Cursor compiled projector (export-token export) plan request (atom reader) close-input!
                                         cursor-state state metrics reservation
                                         #(swap! state update :cursors disj %))]
                    (swap! state update :cursors conj cursor)
                    cursor)
                  (catch Throwable e (close-input!) (throw e))))
              (catch Throwable e (spi/release! reservation) (throw e))))))))
  (-cancel-session! [_ _]
    (let [[before _] (swap-vals! state assoc :cancelled? true)]
      (doseq [cursor (:cursors before)] ((:close-input! cursor))))
    nil)
  (-close-session! [_]
    (let [[before _] (swap-vals! state assoc :closed? true :cursors #{} :plans {})]
      (when-not (:closed? before)
        (cleanup!
          (concat [(fn [] (when-let [r @registration] (spi/unregister-cancel! r)))]
                  (map #(fn [] (spi/-close-cursor! %)) (:cursors before))
                  [#(Files/deleteIfExists ^Path path)]
                  (map #(fn [] (spi/release! %)) @reservations)
                  [#(reset! reservations []) #(swap! stats update :live-spools dec)]))))
    nil))

(defn- unknown-estimate []
  (merge (zipmap [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes]
                (repeat nil))
         {:confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}))

(defrecord ExportSource [descriptor compiled projector profile claimed stats]
  spi/IQuerySource
  (-source-info [_] descriptor)
  (-open-session [this request]
    (let [control (:control request)]
      (spi/check-active! control)
      (ensure! (and (= 2 (:spi-version request))
                    (#{:snapshot :statement :best-effort} (:consistency request))
                    (= #{} (:features request)) (seq (:source-descriptors request))
                    (every? #(= descriptor %) (:source-descriptors request))) :acquisition)
      (when (and (= :once (:reopen profile)) (not (compare-and-set! claimed false true)))
        (format/fail! :query-source/snapshot-lost :acquisition))
      (let [metadata (spi/reserve! control
                       {:retained-bytes (+ 4096 (long (value/value-charge [descriptor compiled])))})
            pinned (volatile! nil)
            session (volatile! nil)]
        (try
          (vreset! pinned (pin! compiled projector profile request stats))
          (swap! stats update :live-spools inc)
          (let [export (get-in compiled [:accepted :dependencies :export])
                info (merge (select-keys descriptor [:source-instance-id :compute-context-id])
                            {:spi-version 2 :features #{} :consistency :snapshot :session-id (UUID/randomUUID)
                             :authorization-token :caller-export-profile :acquired-at-ms (System/currentTimeMillis)
                             :data-version (export-token export) :consumption {:opens :many :max-open-cursors 1}
                             :request-constraints {:max-concurrent-requests 1 :rate-limit nil}})
                acquired (->Session this descriptor compiled projector request info (atom {:plans {} :cursors #{}})
                                    (atom [metadata (:reservation @pinned)]) (:path @pinned) (atom nil) stats)]
            (vreset! session acquired)
            (reset! (:registration acquired)
                    (spi/register-cancel! control #(spi/-cancel-session! acquired %)))
            (spi/check-active! control)
            acquired)
          (catch Throwable e
            (try
              (if @session (spi/-close-session! @session)
                (cleanup! (concat [#(spi/release! metadata)]
                                  (when @pinned [#(Files/deleteIfExists ^Path (:path @pinned))
                                                 #(spi/release! (:reservation @pinned))
                                                 #(swap! stats update :live-spools dec)]))))
              (catch Throwable cleanup-error (.addSuppressed e cleanup-error)))
            (throw e))))))
  (-plan-source [this session request]
    (let [state (:state session) control (:control request)]
      (locking state
        (active! state control)
        (ensure! (and (identical? this (:owner session))
                      (= descriptor (:source-descriptor request)) (= 2 (:spi-version request))
                      (identical? control (get-in session [:request :control]))
                      (= (:query-id request) (get-in session [:request :query-id]))
                      (= (:scope-id request) (get-in descriptor [:scope :id]))) :planning)
        {:alternatives
         (mapv
           (fn [clause]
             (let [columns (columns descriptor clause)
                   plan (merge {:id (keyword "export-scan" (str (:id clause))) :clause clause :columns columns}
                               (select-keys request [:source-symbol :region-id :scope-id :bindings]))
                   reservation (spi/reserve! control {:retained-bytes (+ 2048 (* 2 (long (value/value-charge plan))))})]
               (try
                 (let [handle (Object.)
                       owned (value/owned-copy plan #(active! state control))]
                   (swap! state assoc-in [:plans handle] owned)
                   (swap! (:reservations session) conj reservation)
                   {:id (:id plan) :plan handle :covers #{(:id clause)} :prefilters #{} :requires [] :columns columns
                    :scope-id (:scope-id request) :completion :all-bindings
                    :access {:kind :attribute-enumeration :anchor-clause (:id clause)}
                    :properties {:replay :identical :ordering [] :blocking-state :none}
                    :constraints {:max-binding-rows 256 :max-binding-bytes 262144 :max-request-binding-rows 256}
                    :estimate (unknown-estimate) :explain {:label "Pinned EDN Lines export scan"}})
                 (catch Throwable e (spi/release! reservation) (throw e)))))
           (filter #(= :pattern (:kind %)) (:clauses request)))}))))

(defn source
  "Create an export source without I/O. accepted is a saved/normalized mapping.
  Profile requires :compute-context-id and either :path or :open-input fn.
  An opener receives the SPI acquisition request and must return an InputStream
  promptly, using its control to bound setup. The adapter owns the returned
  stream. :reopen :once consumes one acquisition attempt; :many is the default.
  :spool-directory optionally selects a private local scratch directory.
  Offers scan accesses only, exact snapshot replay, and no provider fusion."
  [accepted profile]
  (let [compiled (compiled! accepted)
        descriptor (mapping/source-info accepted (:compute-context-id profile))]
    (ensure! (and (map? profile)
                  (not= (contains? profile :path) (contains? profile :open-input))
                  (or (string? (:path profile)) (fn? (:open-input profile)))
                  (some? (:compute-context-id profile))
                  (#{:once :many} (get profile :reopen :many))
                  (or (not (contains? profile :spool-directory)) (string? (:spool-directory profile)))
                  (every? #{:path :open-input :compute-context-id :reopen :spool-directory} (keys profile))) :descriptor)
    (->ExportSource descriptor compiled (mapping/fact-projector compiled) profile (atom false)
                    (atom {:acquisitions 0 :read-bytes 0 :validated-records 0 :elapsed-ns 0 :failures 0 :live-spools 0}))))

(defn statistics
  "Cumulative acquisition work, separately from SPI cursor metrics. Contains
  counts only, including live temporary copies; no paths, records, or handles."
  [^ExportSource source]
  @(:stats source))
