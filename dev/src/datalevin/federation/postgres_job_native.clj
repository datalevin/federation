(ns datalevin.federation.postgres-job-native
  "Replay native SQL with normal JDBC fetching, retaining a separate adapter
  screen as evidence. Useful when correcting a native measurement confound."
  (:refer-clojure :exclude [run!])
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datalevin.federation.postgres-job :as job]
            [datalevin.federation.postgres-job-suite :as suite]))

(defn comparison [reference entries sample-count]
  (let [adapter (:results reference)
        native-by-id (into {} (map (juxt :id identity)) entries)
        matched (filterv #(and (= :ok (:status %))
                                (= :ok (:status (native-by-id (:id %))))) adapter)
        sum-mode (fn [rows i mode]
                   (reduce + 0 (keep #(get-in % [:samples i mode :elapsed-ns]) rows)))
        n (count adapter)]
    {:expected-queries n :native-completed (count (filter #(= :ok (:status %)) entries))
     :adapter-verified (count matched) :adapter-incomplete (mapv :id (remove #(= :ok (:status %)) adapter))
     :adapter-outcomes (get-in reference [:summary :adapter-outcomes])
     :full-suite-comparable? (and (= 113 n (count entries) (count matched)))
     :totals
     (mapv (fn [i]
             (let [native-complete? (and (= n (count entries))
                                          (every? #(= :ok (get-in % [:samples i :native :status])) entries))
                   native-ns (when native-complete? (sum-mode entries i :native))
                   adapter-ns (sum-mode matched i :adapter)]
               {:sample i :native-total-ns native-ns :adapter-verified-subtotal-ns adapter-ns
                :native-matched-subtotal-ns (sum-mode (mapv #(native-by-id (:id %)) matched) i :native)
                :adapter-attempted-ns (sum-mode adapter i :adapter)
                :adapter-full-suite-ns (when (= 113 n (count matched)) adapter-ns)
                :speedup (when (and native-complete? (= 113 n (count matched))) (/ (double native-ns) adapter-ns))}))
           (range sample-count))}))

(defn run! [profile opts reference-path output]
  (let [options (suite/options! opts) reference (edn/read-string (slurp reference-path))
        _ (when-not (and (= :complete (:status reference))
                          (= (:samples options) (get-in reference [:options :samples]))
                          (= (:schema options) (get-in reference [:options :schema])))
            (throw (ex-info "Native replay requires a completed adapter screen with the same sample count" {})))
        entries (:results reference)
        _ (when-not (= (count entries) (count (suite/corpus! options)))
            (throw (ex-info "Native replay query selection must match the reference screen" {})))
        _ (when-not (= (into {} (map (juxt :id :sha256)) entries)
                       (into {} (map (juxt :id :sha256)) (suite/corpus! options)))
            (throw (ex-info "Native replay SQL corpus changed" {})))
        reference (if (zero? (get reference :native-aggregate-fetch-size 4096)) reference
                    (-> reference
                        (assoc :native-aggregate-fetch-size 4096 :native-timings-valid? false
                               :native-timings-superseded-by output
                               :superseded-native-summary (:summary reference))
                        (update-in [:summary :totals]
                                   #(mapv (fn [total]
                                            (assoc total :native-total-ns nil :native-matched-subtotal-ns nil
                                                   :native-complete? false :speedup nil)) %))))
        _ (when (false? (:native-timings-valid? reference)) (suite/write-report! reference-path reference))
        profile (assoc profile :statement-timeout-ms (:timeout-ms options))
        started (System/nanoTime)
        report (atom {:format-version 1 :status :running :started-at (str (java.time.Instant/now))
                      :options options :native-aggregate-fetch-size 0
                      :sampling-policy :separate-sequential-native-pass-after-adapter-screen
                      :timing-scope :client-wall-including-connect-snapshot-planning-execute-decode-close
                      :reference {:path reference-path :sha256 (job/digest [(slurp reference-path)])
                                  :native-timings-used? false
                                  :adapter-options (:options reference)
                                  :adapter-effective-limits (:effective-limits reference)
                                  :adapter-implementation-sha256 (:implementation-sha256 reference)
                                  :reason (if (zero? (get reference :native-aggregate-fetch-size 4096))
                                            :independent-control-replay
                                            :nonzero-jdbc-fetch-count-disables-native-parallel-execution)}
                      :implementation-sha256 (into (sorted-map)
                                                   (for [p ["datalevin/federation/postgres_job_native.clj"
                                                            "datalevin/federation/postgres_job_suite.clj"]]
                                                     [p (job/digest [(slurp (io/resource p))])]))
                      :environment {:java (System/getProperty "java.version") :clojure (clojure-version)
                                    :max-heap-bytes (.maxMemory (Runtime/getRuntime))}
                      :database (suite/native! profile "SELECT version(), current_database(), datcollate FROM pg_database WHERE datname=current_database()")
                      :native-settings (suite/native! profile "SELECT current_setting('max_parallel_workers_per_gather'), current_setting('max_parallel_workers'), current_setting('work_mem'), current_setting('shared_buffers'), current_setting('jit'), current_setting('geqo_threshold')")
                      :results []})]
    (suite/write-report! output @report)
    (doseq [entry entries]
      (dotimes [_ (:warmups options)] (suite/native! profile (:sql entry)))
      (let [samples (mapv (fn [i]
                            (let [native (suite/attempt #(suite/native! profile (:sql entry)))
                                  check (when (= :ok (:status native))
                                          (suite/attempt
                                            #(when (and (some? (:expected entry)) (not= (:expected entry) (:result native)))
                                               (throw (ex-info "Native replay result changed"
                                                               {:error :benchmark/result-mismatch
                                                                :expected-sha256 (job/digest (:expected entry))
                                                                :actual-sha256 (job/digest (:result native))})))))]
                              {:index i :native (dissoc native :result) :check check})) (range (:samples options)))
            result {:id (:id entry) :sha256 (:sha256 entry) :sql (:sql entry) :samples samples
                    :status (if (every? #(and (= :ok (get-in % [:native :status])) (= :ok (get-in % [:check :status]))) samples) :ok :failed)}]
        (swap! report update :results conj result)
        (swap! report assoc :comparison (comparison reference (:results @report) (:samples options))
               :harness-elapsed-ns (- (System/nanoTime) started))
        (suite/write-report! output @report)
        (prn {:query (:id entry) :status (:status result)
              :native-ms (/ (get-in samples [0 :native :elapsed-ns]) 1e6)})
        (flush)))
    (swap! report assoc :status :complete :finished-at (str (java.time.Instant/now))
           :harness-elapsed-ns (- (System/nanoTime) started))
    (suite/write-report! output @report)
    @report))

(defn -main [opts reference output]
  (let [url (System/getenv "POSTGRES_JOB_URL")]
    (when-not (and url opts reference output) (throw (ex-info "Set POSTGRES_JOB_URL; pass EDN options, reference and output" {})))
    (let [properties (cond-> {} (System/getenv "POSTGRES_JOB_USER") (assoc "user" (System/getenv "POSTGRES_JOB_USER"))
                                (System/getenv "POSTGRES_JOB_PASSWORD") (assoc "password" (System/getenv "POSTGRES_JOB_PASSWORD")))
          report (run! {:jdbc-url url :properties properties} (edn/read-string opts) reference output)]
      (prn (:comparison report)) (shutdown-agents))))
