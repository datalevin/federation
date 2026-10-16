(ns datalevin.federation.local-batch-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.federation.local :as local]
            [datalevin.federation.local-test :refer [descriptor category]]
            [datalevin.query.source :as spi]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control])
  (:import [java.util Date]))

(defn- with-cursor [info rows terms bindings limits f]
  (let [ctl (control/create {:limits limits})
        s (local/source info rows)
        common {:control ctl :spi-version 1 :features #{} :consistency :snapshot
                :source-descriptors [info]}
        session (spi/-open-session s common)]
    (try
      (let [response (spi/-plan-source s session
                       (merge common {:source-descriptor info :source-symbol '$s :bindings bindings
                                      :clauses [{:id 0 :kind :pattern :terms terms}]}))
            alternative (first (:alternatives response))
            cursor (spi/-open-cursor session
                     (merge common {:definition-fingerprint (:definition-fingerprint info)
                                    :alternative-id (:id alternative) :plan (:plan alternative)
                                    :source-symbol '$s}))]
        (f {:control ctl :source s :session session :cursor cursor :columns (:columns alternative)}))
      (finally
        (spi/-close-session! session)
        (control/close! ctl)
        (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))))))

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
