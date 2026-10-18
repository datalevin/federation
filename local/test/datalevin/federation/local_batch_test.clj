(ns datalevin.federation.local-batch-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.federation.local :as local]
            [datalevin.federation.local-test :refer [descriptor category]]
            [datalevin.query.source :as spi]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value])
  (:import [java.util Date]))

(defn- with-session [info rows limits f]
  (let [ctl (control/create {:limits limits})
        s (local/source info rows)
        common {:control ctl :limits (control/limits ctl)
                :query-id (random-uuid) :authorization {:principal-id :test}
                :spi-version 1 :features #{} :consistency :snapshot
                :source-descriptors [info]}
        session (spi/-open-session s common)]
    (try (f {:control ctl :source s :session session :request common})
      (finally
        (spi/-close-session! session)
        (control/close! ctl)
        (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))))))

(defn- plan! [{:keys [source session request]} info terms bindings]
  (first (:alternatives
           (spi/-plan-source source session
             (merge request {:source-descriptor info :source-symbol '$s :bindings bindings
                             :clauses [{:id 0 :kind :pattern :terms terms}]})))))

(defn- open-cursor! [{:keys [session request]} info alternative]
  (spi/-open-cursor session
    (contract/cursor-request request '$s (:definition-fingerprint info) alternative)))

(defn- with-cursor [info rows terms bindings limits f]
  (with-session info rows limits
    (fn [fixture]
      (let [alternative (plan! fixture info terms bindings)
            cursor (open-cursor! fixture info alternative)]
        (f (assoc fixture :cursor cursor :columns (:columns alternative)))))))

(defn- fetch! [{:keys [control cursor columns]} max-rows max-bytes]
  (let [request {:control control :max-rows max-rows :max-bytes max-bytes}
        batch (spi/-next-batch! cursor request)]
    (try (contract/batch! columns request batch)
         (catch Throwable e
           (when-let [token (:reservation batch)] (spi/release! token))
           (throw e)))))

(defn- release! [batch]
  (when-let [token (:reservation batch)] (spi/release! token)))

(def terms [[:var '?id] [:var '?v]])
(defn- rows [n] (mapv #(vector % "x") (range n)))

(deftest local-cursor-fills-row-and-byte-limits
  (doseq [[max-rows byte-bound expected]
          [[3 :unbounded [3 3 3 2]] [64 :three [3 3 3 2]]
           [64 :below-three [2 2 2 2 2 1]] [2 :three [2 2 2 2 2 1]]]]
    (with-cursor descriptor (rows 11) terms {} {:in-flight-batches 1}
      (fn [{:keys [control columns cursor] :as fixture}]
        (let [three (long (spi/batch-charge-bytes columns (rows 3)))
              maximum (case byte-bound :unbounded Long/MAX_VALUE :three three :below-three (dec three))
              baseline (control/usage control)
              received (loop [received []]
                         (let [batch (fetch! fixture max-rows maximum)]
                           (is (<= (long (:byte-count batch)) (long maximum)))
                           (is (= 1 (:in-flight-batches (control/usage control))))
                           (is (= (:byte-count batch)
                                  (- (long (:retained-bytes (control/usage control)))
                                     (long (:retained-bytes baseline)))))
                           (release! batch)
                           (is (= baseline (control/usage control)))
                           (let [received (conj received batch)]
                             (if (:exhausted? batch) received (recur received)))))
              metrics (spi/-cursor-metrics cursor)]
          (is (= expected (mapv :row-count received)))
          (is (= (rows 11) (vec (mapcat :rows received))))
          (is (= (count expected) (:batches metrics)))
          (is (= 11 (:rows metrics)))
          (is (= (reduce + (map :byte-count received)) (:charged-bytes metrics)))
          ;; A byte-boundary lookahead is examined again on the next fetch;
          ;; both sizing and copy passes contribute physical read work.
          (is (= (+ 22 (if (= 64 max-rows) (dec (count expected)) 0)) (:read-rows metrics))))))))

(deftest local-batches-skip-nonmatches-and-missing-eav-values
  (let [info (assoc descriptor :pattern-contract
                    {:kind :eav :attributes {:value {:entity-type :long :value-type :string}}})
        input [[0 :value nil] [1 :value "x"] [2 :value nil] [3 :value "x"] [4 :value nil]]]
    (with-cursor info input [[:var '?id] [:const :value] [:ignore]] {} {}
      (fn [{:keys [control] :as fixture}]
        (let [first-batch (fetch! fixture 2 10000)]
          (is (= [[1] [3]] (:rows first-batch)))
          (is (false? (:exhausted? first-batch)))
          (release! first-batch)
          (let [last-batch (fetch! fixture 2 10000)]
            (is (= [] (:rows last-batch)))
            (is (true? (:exhausted? last-batch)))
            (is (nil? (:reservation last-batch)))
            (is (zero? (:in-flight-batches (control/usage control)))))))))
  (with-cursor descriptor [[0 "skip"] [1 "x"] [2 "skip"] [3 "x"] [4 "skip"]]
    [[:var '?id] [:const "x"]] {} {}
    (fn [fixture]
      (let [batch (fetch! fixture 3 10000)]
        (is (= [[1] [3]] (:rows batch)))
        (is (true? (:exhausted? batch)))
        (release! batch)))))

(deftest local-batches-own-mutable-leaves-after-close
  (let [bytes (byte-array [1 2]) date (Date. 1000)
        info (assoc-in descriptor [:pattern-contract :columns 1] {:type :data :nullable? false})
        input (mapv #(vector % {:bytes bytes :date date}) (range 5))]
    (with-cursor info input terms {} {}
      (fn [{:keys [control cursor session] :as fixture}]
        (aset-byte bytes 0 (byte 9))
        (.setTime date 9000)
        (let [a (fetch! fixture 2 10000)]
          (release! a)
          (aset-byte ^bytes (get-in a [:rows 0 1 :bytes]) 0 (byte 8))
          (.setTime ^Date (get-in a [:rows 0 1 :date]) 8000)
          (let [b (fetch! fixture 2 10000)]
            (release! b)
            (let [c (fetch! fixture 2 10000)]
              (is (= [2 2 1] (mapv :row-count [a b c])))
              (spi/-close-cursor! cursor)
              (spi/-close-session! session)
              ;; A transferred batch belongs to the consumer, even after the
              ;; source closes. The last token remains charged until release.
              (is (= 1 (:in-flight-batches (control/usage control))))
              (is (= (:byte-count c) (:retained-bytes (control/usage control))))
              (doseq [[_ v] (concat (next (:rows a)) (:rows b) (:rows c))]
                (is (= [1 2] (vec (:bytes v))))
                (is (= 1000 (.getTime ^Date (:date v)))))
              (release! c)
              (is (zero? (:retained-bytes (control/usage control)))))))))))

(deftest local-batch-reservation-failure-does-not-consume-rows
  (let [needed (atom nil) input (rows 4)]
    (with-cursor descriptor input terms {} {}
      (fn [{:keys [control columns]}]
        (reset! needed (+ (long (:retained-bytes (control/usage control)))
                          (long (spi/batch-charge-bytes columns input))))))
    (with-cursor descriptor input terms {} {:retained-bytes (dec (long @needed))}
      (fn [{:keys [control] :as fixture}]
        (let [baseline (control/usage control)]
          (is (= :query-source/limit (category #(fetch! fixture 4 Long/MAX_VALUE))))
          (is (= baseline (control/usage control)))
          (let [batch (fetch! fixture 1 Long/MAX_VALUE)]
            (is (= [[0 "x"]] (:rows batch)))
            (release! batch)
            (is (= baseline (control/usage control)))))))))

(deftest oversized-row-is-left-for-the-next-fetch
  (let [small (rows 2) large [2 (.repeat "x" 10000)]]
    (with-cursor descriptor (conj small large) terms {} {}
      (fn [{:keys [control columns] :as fixture}]
        (let [maximum (spi/batch-charge-bytes columns small)
              baseline (control/usage control)
              first-batch (fetch! fixture 10 maximum)]
          (is (= small (:rows first-batch)))
          (is (false? (:exhausted? first-batch)))
          (release! first-batch)
          (is (= :query-source/limit (category #(fetch! fixture 10 maximum))))
          (is (= baseline (control/usage control)))
          (let [last-batch (fetch! fixture 10 Long/MAX_VALUE)]
            (is (= [large] (:rows last-batch)))
            (is (true? (:exhausted? last-batch)))
            (release! last-batch)))))))

(deftest local-batches-honor-cancellation-and-copy-charges
  (with-cursor descriptor (rows 4) terms {} {}
    (fn [{:keys [session control] :as fixture}]
      (let [baseline (control/usage control)]
        (spi/-cancel-session! session {:error :query-source/cancelled :phase :fetch})
        (is (= :query-source/cancelled (category #(fetch! fixture 4 10000))))
        (is (= baseline (control/usage control))))))
  (let [info (assoc-in descriptor [:pattern-contract :columns 1] {:type :data :nullable? false})
        bound (with-meta [:x] {:extra (.repeat "metadata" 100)})]
    (with-cursor info [[1 [:x]] [2 [:x]]] terms {'?v bound} {}
      (fn [{:keys [control] :as fixture}]
        (let [before (long (:retained-bytes (control/usage control)))
              batch (fetch! fixture 2 100000)]
          (is (= [[1 [:x]] [2 [:x]]] (:rows batch)))
          (is (= (mapv meta [bound bound]) (mapv (comp meta second) (:rows batch))))
          (is (= (long (:byte-count batch))
                 (- (long (:retained-bytes (control/usage control))) before)))
          (release! batch))))))

(deftest local-batches-preserve-and-own-metadata-including-bound-values
  (doseq [bound? [false true]]
    (let [bytes (byte-array [1 2]) date (Date. 1000)
          x (with-meta [(with-meta 'leaf {:date date})] {:bytes bytes})
          info (assoc-in descriptor [:pattern-contract :columns 1] {:type :data :nullable? false})
          input (mapv #(vector % x) (range 4))]
      (with-cursor info input terms (if bound? {'?v x} {}) {}
        (fn [{:keys [cursor session] :as fixture}]
          (let [a (fetch! fixture 2 10000)
                values (mapv second (:rows a))]
            (release! a)
            (doseq [v values]
              (is (= [1 2] (vec (:bytes (meta v)))))
              (is (= 1000 (.getTime ^Date (:date (meta (first v)))))))
            (aset-byte ^bytes (:bytes (meta (first values))) 0 (byte 8))
            (.setTime ^Date (:date (meta (ffirst values))) 8000)
            (is (= [1 2] (vec bytes)))
            (is (= 1000 (.getTime date)))
            (let [b (fetch! fixture 2 10000)]
              (release! b)
              (spi/-close-cursor! cursor)
              (spi/-close-session! session)
              (doseq [v (concat (next values) (map second (:rows b)))]
                (is (= [1 2] (vec (:bytes (meta v)))))
                (is (= 1000 (.getTime ^Date (:date (meta (first v))))))))))))))

(deftest local-batches-preserve-default-sorted-collections
  (doseq [make-value [(fn [date] (sorted-map :date date :ratio 1/2 :char \λ))
                      (fn [date] (sorted-set date (Date. 2000)))]]
    (let [date (Date. 1000)
          x (with-meta (make-value date) {:stamp date})
          info (assoc-in descriptor [:pattern-contract :columns 1] {:type :data :nullable? false})
          collected (atom [])]
      (with-cursor info (mapv #(vector % x) (range 5)) terms {} {}
        (fn [{:keys [cursor session] :as fixture}]
          (.setTime date 9000)
          (dotimes [_ 3]
            (let [batch (fetch! fixture 2 10000)]
              (swap! collected into (map second (:rows batch)))
              (release! batch)))
          (spi/-close-cursor! cursor)
          (spi/-close-session! session)))
      (is (= 5 (count @collected)))
      (doseq [v @collected]
        (is (= (class x) (class v)))
        (is (= (seq (make-value (Date. 1000))) (seq v)))
        (is (= 1000 (.getTime ^Date (:stamp (meta v))))))
      (.setTime ^Date (:stamp (meta (first @collected))) 8000)
      (doseq [v (next @collected)]
        (is (= 1000 (.getTime ^Date (:stamp (meta v)))))))))

(def ^:private data-descriptor
  (assoc-in descriptor [:pattern-contract :columns 1] {:type :data :nullable? false}))

(deftype MeasuredLeaf [id bytes on-size on-compare]
  value/IQuerySourceValue
  (-logical-type [_] {:kind :custom :name :test/measured-leaf :version 1
                     :fingerprint "measured:v1" :parameters {}})
  (-retained-bytes [_]
    (when-let [f @on-size] (f))
    bytes)
  Comparable
  (compareTo [_ other]
    (when-let [f @on-compare] (f id))
    (Long/compare (long id) (long (.-id ^MeasuredLeaf other)))))

(defn- leaf [id bytes on-size]
  (MeasuredLeaf. id bytes on-size (atom nil)))

(defn- error-data [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))

(deftest pinned-snapshot-includes-outer-metadata
  (doseq [rows [[] [[1 "x"]]]]
    (let [input (with-meta rows {:payload (byte-array 32768)})
          source (local/source descriptor input)
          q '{:find [?id] :in [$s] :where [[$s ?id _]]
              :query-source {:limits {:retained-bytes 16384}}}
          error (error-data #(d/q q source))]
      (is (= :query-source/limit (:error error)))
      (is (= :acquisition (:phase error)))
      (is (= :retained-bytes (:category error)))
      (with-session descriptor input {}
        (fn [{:keys [control source session]}]
          (is (<= (value/value-charge input) (:retained-bytes (control/usage control))))
          (let [charge (:retained-bytes (control/usage control))]
            (local/replace-rows! source [])
            (is (= charge (:retained-bytes (control/usage control))))
            (spi/-close-session! session)
            (is (zero? (:retained-bytes (control/usage control))))))))))

(deftest acquisition-stops-before-later-nested-values
  (doseq [mode [:limit :cancel :overflow]]
    (testing (name mode)
      (let [first-action (atom nil) later-action (atom nil) visited (atom 0)
            rows [[1 [(leaf 1 (if (= mode :overflow) Long/MAX_VALUE 65536) first-action)
                      (leaf 2 64 later-action)]]]
            source (local/source data-descriptor rows)
            ctl (control/create {:limits {:retained-bytes (case mode
                                                           :limit 16384
                                                           :cancel 1048576
                                                           :overflow Long/MAX_VALUE)}})
            request {:control ctl :limits (control/limits ctl) :spi-version 1
                     :features #{} :consistency :snapshot :source-descriptors [data-descriptor]}]
        (try
          (reset! later-action #(swap! visited inc))
          (when (= mode :cancel)
            (reset! first-action #(control/cancel! ctl {:error :query-source/cancelled :phase :acquisition})))
          (is (= (if (= mode :cancel) :query-source/cancelled :query-source/limit)
                 (category #(spi/-open-session source request))))
          (is (zero? @visited))
          (is (every? zero? (vals (control/usage ctl))))
          (finally (control/close! ctl)))))))

(deftest planning-and-cursor-sizing-observe-nested-cancellation
  (doseq [phase [:planning :open]]
    (testing (name phase)
      (let [first-action (atom nil) later-action (atom nil) visited (atom 0)
            x [(leaf 1 64 first-action) (leaf 2 64 later-action)]]
        (with-session data-descriptor [] {}
          (fn [{:keys [control] :as fixture}]
            (let [constant-terms [[:var '?id] [:const x]]
                  alternative (when (= phase :open)
                                (plan! fixture data-descriptor constant-terms {}))
                  baseline (control/usage control)]
              (reset! first-action #(control/cancel! control {:error :query-source/cancelled :phase phase}))
              (reset! later-action #(swap! visited inc))
              (is (= :query-source/cancelled
                     (category #(if alternative
                                  (open-cursor! fixture data-descriptor alternative)
                                  (plan! fixture data-descriptor constant-terms {})))))
              (is (zero? @visited))
              (is (= baseline (control/usage control))))))))))

(deftest batch-sizing-stops-at-the-first-excess-nested-charge
  (let [first-action (atom nil) later-action (atom nil) visited (atom 0)
        large [1 [(leaf 1 65536 first-action) (leaf 2 64 later-action)]]]
    (with-cursor data-descriptor [[0 :small] large] terms {} {}
      (fn [{:keys [control columns] :as fixture}]
        (let [baseline (control/usage control)
              maximum (spi/batch-charge-bytes columns [[0 :small]])]
          (reset! later-action #(swap! visited inc))
          (let [batch (fetch! fixture 2 maximum)]
            (is (= [[0 :small]] (:rows batch)))
            (is (false? (:exhausted? batch)))
            (is (zero? @visited))
            (release! batch))
          (is (= :query-source/limit (category #(fetch! fixture 2 16384))))
          (is (zero? @visited))
          (is (= baseline (control/usage control)))
          (reset! later-action nil)
          (let [batch (fetch! fixture 2 Long/MAX_VALUE)]
            (is (= [large] (:rows batch)))
            (is (true? (:exhausted? batch)))
            (release! batch)
            (is (= baseline (control/usage control)))))))))

(deftest cancellation-inside-batch-sizing-releases-state
  (let [first-action (atom nil) later-action (atom nil) visited (atom 0)
        rows [[1 [(leaf 1 64 first-action) (leaf 2 64 later-action)]]]]
    (with-cursor data-descriptor rows terms {} {}
      (fn [{:keys [control cursor] :as fixture}]
        (let [baseline (control/usage control)]
          (reset! first-action #(control/cancel! control {:error :query-source/cancelled :phase :fetch}))
          (reset! later-action #(swap! visited inc))
          (is (= :query-source/cancelled (category #(fetch! fixture 1 100000))))
          (is (zero? @visited))
          (is (zero? (:rows (spi/-cursor-metrics cursor))))
          (is (= baseline (control/usage control))))))))

(deftest cancellation-inside-owned-copy-releases-the-batch-reservation
  (let [on-compare (atom nil) comparisons (atom [])
        values (into (sorted-set)
                     (map #(MeasuredLeaf. % 64 (atom nil) on-compare)) (range 8))]
    (with-cursor data-descriptor [[1 values]] terms {} {}
      (fn [{:keys [control cursor] :as fixture}]
        (let [baseline (control/usage control)]
          ;; Default sorted-set copying calls its elements' natural comparison.
          ;; Cancel on the first comparison, after the output has been reserved.
          (reset! on-compare #(do (swap! comparisons conj %)
                                 (control/cancel! control {:error :query-source/cancelled :phase :fetch})))
          (is (= :query-source/cancelled (category #(fetch! fixture 1 100000))))
          ;; One set insertion can compare twice; no later element is copied.
          (is (= 1 (count (set @comparisons))))
          (is (zero? (:rows (spi/-cursor-metrics cursor))))
          (is (= baseline (control/usage control))))))))
