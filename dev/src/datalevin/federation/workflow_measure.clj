(ns datalevin.federation.workflow-measure
  "One reproducible, correctness-gated topology per fresh JVM. Controlled eager
  observations provide the oracle; production adapters perform actual HTTP/file
  reads. Instrumentation never substitutes another evaluator or provider."
  (:refer-clojure :exclude [run!])
  (:require [clojure.edn :as edn]
            [datalevin.core :as d]
            [datalevin.federation.file :as export]
            [datalevin.federation.github :as github]
            [datalevin.federation.release-triage :as triage]
            [datalevin.federation.workflow-fixture :as fixture]
            [datalevin.federation.workflow-observer :as observer]
            [jsonista.core :as json])
  (:import [com.sun.management ThreadMXBean]
           [java.lang.management ManagementFactory MemoryMXBean GarbageCollectorMXBean]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util HexFormat]))

(def ^:private memory (ManagementFactory/getMemoryMXBean))
(defn- heap-used [] (.getUsed (.getHeapMemoryUsage ^MemoryMXBean memory)))
(defn- gc-state []
  (reduce (fn [out ^GarbageCollectorMXBean bean]
            (-> out (update :count + (max 0 (.getCollectionCount bean)))
                (update :ms + (max 0 (.getCollectionTime bean)))))
          {:count 0 :ms 0} (ManagementFactory/getGarbageCollectorMXBeans)))
(defn- allocation []
  (let [bean (ManagementFactory/getThreadMXBean)]
    (when (and (instance? ThreadMXBean bean) (.isThreadAllocatedMemorySupported ^ThreadMXBean bean))
      (when-not (.isThreadAllocatedMemoryEnabled ^ThreadMXBean bean)
        (.setThreadAllocatedMemoryEnabled ^ThreadMXBean bean true))
      (.getThreadAllocatedBytes ^ThreadMXBean bean (.getId (Thread/currentThread))))))
(defn- delta [after before] (merge-with - after before))
(defn- digest [rows]
  (.formatHex (HexFormat/of)
              (.digest (MessageDigest/getInstance "SHA-256")
                       (.getBytes (pr-str (vec (sort-by pr-str rows))) StandardCharsets/UTF_8))))

(defn- timed [f]
  (let [running (atom true) before (heap-used) peak (atom before)
        sampler (doto (Thread. ^Runnable
                        #(while @running
                           (swap! peak max (heap-used))
                           (try (Thread/sleep 2) (catch InterruptedException _ nil)))
                              "federation-workflow-heap-sampler")
                  (.setDaemon true) .start)
        gc-before (gc-state) allocation-before (allocation) started (System/nanoTime)]
    (try
      (let [result (f) elapsed (- (System/nanoTime) started) allocation-after (allocation)]
        (swap! peak max (heap-used))
        {:result result :elapsed-ns elapsed :heap-before-bytes before :sampled-heap-peak-bytes @peak
         :caller-allocated-bytes (when (and allocation-before allocation-after) (- allocation-after allocation-before))
         :gc (delta (gc-state) gc-before)})
      (finally (reset! running false) (.interrupt sampler) (.join sampler 1000)))))

(def ^:private native-query
  '{:find [?number ?priority] :in [$ ?repository ?minimum]
    :where [[?t :triage/repository ?repository] [?t :triage/number ?number]
            [?t :triage/priority ?priority] [(>= ?priority ?minimum)]]})

(def ^:private api-native-query
  (-> triage/lookup-query
      (assoc :in '[$ $github ?repository ?minimum [[?window ?witness]]]
             :where '[[?t :triage/repository ?repository] [?t :triage/number ?number]
                      [?t :triage/priority ?priority] [(>= ?priority 9)]
                      [(= ?window "now")]
                      [$github ?issue :github.issue/number ?number]
                      [$github ?issue :github.issue/title ?title]])))

(defn- invoke [q fixture sources]
  (d/q q @(:conn fixture) (:github sources) (:release sources) 42 9 (:windows fixture)))
(defn- source-counts [fixture]
  {:http @(:requests (:api fixture))
   :github (github/statistics (get-in fixture [:sources :github]))
   :export (export/statistics (get-in fixture [:sources :release]))})
(defn- numeric-delta [after before]
  (into {} (keep (fn [[key value]] (when (number? value) [key (- value (long (get before key 0)))]))) after))

(defn- measure [fixture eager expected q mode]
  (let [observed (observer/recorder) sources (observer/sources (:sources fixture) observed)
        before (source-counts fixture)
        measurement
        (timed #(case mode
                  :composed (invoke q fixture sources)
                  :composed-literal (invoke (assoc-in q [:where 3] (quote [(>= ?priority 9)])) fixture sources)
                  :eager (invoke (dissoc q :query-source) fixture eager)
                  :native (d/q native-query @(:conn fixture) 42 9)
                  :native-api (d/q (assoc api-native-query :query-source (:query-source q) :timeout (:timeout q)) @(:conn fixture) (:github sources) 42 9 (:windows fixture))))
        result (:result measurement)
        answer (if (= mode :native) (set (map #(subvec % 0 2) expected)) expected)
        after (source-counts fixture)]
    (when-not (= answer result) (throw (ex-info "Measurement failed its complete eager oracle" {:mode mode})))
    (when-not (and (fixture/clean? fixture) (observer/clean? observed)
                   (zero? (get-in after [:github :live-sessions])) (zero? (get-in after [:export :live-spools])))
      (throw (ex-info "Measurement leaked an acquired view" {:mode mode})))
    (merge (dissoc measurement :result)
           {:http-key-counts (numeric-delta (get-in after [:http :number-counts]) (get-in before [:http :number-counts]))
            :mode mode :result-rows (count result) :result-sha256 (digest result)
            :source-work (into {} (map (fn [label] [label (numeric-delta (get after label) (get before label))]))
                               [:http :github :export])}
           (when (#{:composed :composed-literal :native-api} mode) @(:state observed)))))

(defn- validate! [options]
  (when-not (and (map? options)
                 (every? #{:name :native-rows :export-rows :selected-keys :latency-ms :batch-rows :binding-rows
                           :warmups :samples :fork :input-rows} (keys options))
                 (keyword? (:name options))
                 (every? #(and (pos-int? %) (<= % 10000))
                         ((juxt :native-rows :export-rows :selected-keys :batch-rows :binding-rows :samples :fork :input-rows) options))
                 (<= (:selected-keys options) (min (:native-rows options) (:export-rows options)))
                 (nat-int? (:warmups options)) (<= (:warmups options) 10)
                 (nat-int? (:latency-ms options)) (<= (:latency-ms options) 1000)
                 (<= (:samples options) 20))
    (throw (ex-info "Invalid bounded measurement options" {})))
  options)

(defn run! [options]
  (validate! options)
  (let [setup-started (System/nanoTime) fixture (assoc (fixture/create! options) :windows
                       (mapv (fn [n] [(if (zero? n) "now" (str "later-" n)) (str "witness-" n)])
                             (range (:input-rows options))))
        setup-ns (- (System/nanoTime) setup-started)
        limits {:binding-rows (:binding-rows options) :batch-rows (:batch-rows options)
                :retained-bytes 268435456 :external-requests 8192 :spill-bytes 67108864}
        q (-> triage/lookup-query
              (update :where conj (quote [(= ?window "now")]))
              (assoc :timeout 120000) (assoc-in [:query-source :limits] limits))]
    (try
      (let [eager-started (System/nanoTime) eager (fixture/eager fixture)
            eager-ns (- (System/nanoTime) eager-started)
            expected (invoke (dissoc q :query-source) fixture eager)
            first-query (measure fixture eager expected q :composed)
            _ (dotimes [_ (:warmups options)]
                (doseq [mode [:composed :eager :composed-literal :native-api :native]] (measure fixture eager expected q mode)))
            ;; Plan-only includes real repository authorization and export copy/
            ;; validation. It is a separate invocation, never subtracted from total.
            plan-started (System/nanoTime)
            sources (:sources fixture)
            explanation (d/explain {:run? false} q @(:conn fixture) (:github sources) (:release sources) 42 9 (:windows fixture))
            plan-ns (- (System/nanoTime) plan-started)
            samples (mapv (fn [index]
                            {:index index
                             :runs (mapv #(measure fixture eager expected q %)
                                         (if (even? index) [:composed :eager :composed-literal :native-api :native]
                                             [:native :native-api :composed-literal :eager :composed]))}) (range (:samples options)))
            runtime (ManagementFactory/getRuntimeMXBean)]
        {:format-version 1 :options options :limits limits
         :workload {:query (pr-str q) :api-native-control (pr-str api-native-query)
                    :native-control (pr-str native-query) :input-sha256 (digest (:windows fixture))} :setup-ns setup-ns :eager-materialization-ns eager-ns
         :environment {:java-version (System/getProperty "java.version") :vm (System/getProperty "java.vm.name")
                       :clojure-version (clojure-version) :os (System/getProperty "os.name") :arch (System/getProperty "os.arch")
                       :processors (.availableProcessors (Runtime/getRuntime)) :max-heap-bytes (.maxMemory (Runtime/getRuntime))
                       :vm-arguments (vec (.getInputArguments runtime)) :process-uptime-ms (.getUptime runtime)}
         :first-query first-query :plan-only-ns plan-ns :explanation explanation :samples samples})
      (finally ((:close! fixture))))))

(defn -main [options output]
  (try
    (when (> (count options) 4096) (throw (ex-info "Oversized options" {})))
    (let [result (run! (edn/read-string options))]
      (spit output (pr-str result))
      (spit (str output ".json") (json/write-value-as-string (dissoc result :explanation)))
      (prn {:scenario (get-in result [:options :name]) :fork (get-in result [:options :fork])
            :result-sha256 (get-in result [:first-query :result-sha256]) :samples (count (:samples result))}))
    (finally (shutdown-agents))))
