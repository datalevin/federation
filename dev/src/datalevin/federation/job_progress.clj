(ns datalevin.federation.job-progress
  "Compare optimizer revisions against fixed JOB answers and measurement settings."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.federation.job-comparison :as comparison]
            [jsonista.core :as json])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.security MessageDigest]))

(def protocol-resources
  ["datalevin/federation/job_competitors.clj"
   "datalevin/federation/postgres_job_suite.clj"
   "datalevin/federation/postgres_job_sql.clj"
   "datalevin/federation/postgres_job.clj"])

(defn experiment! [{:keys [id parent kind mechanism hypothesis] :as experiment}]
  (comparison/check!
    (and (every? #(and (string? %) (not (str/blank? %))) [id parent mechanism hypothesis])
         (#{"single-change" "ablation" "cumulative" "validation"} kind))
    "Experiment needs id, parent, kind, mechanism, and hypothesis")
  experiment)

(defn protocol-key [run]
  (let [key (comparison/comparison-key run)]
    (-> key
        (update :protocol dissoc :implementation-sha256)
        (update :environment dissoc "revisions")
        (assoc :engine (get-in run [:options :engine])
               :harness (select-keys (:implementation-sha256 run) protocol-resources)))))

(defn ratio [a b] (when (and (number? a) (number? b) (pos? b)) (/ a b)))

(defn changed-resources [before after]
  (vec (for [resource (sort (into (set (keys before)) (keys after)))
             :when (not= (before resource) (after resource))]
         {:resource resource :before (before resource) :after (after resource)})))

(defn summarize [baseline candidate experiment]
  (comparison/validate! baseline)
  (comparison/validate! candidate)
  (experiment! experiment)
  (comparison/check! (and (get-in baseline [:options :include-adapter?])
                          (get-in candidate [:options :include-adapter?]))
                     "Progress comparison requires paired Datalevin controls")
  (comparison/check! (= (protocol-key baseline) (protocol-key candidate))
                     "Corpus, answers, source, host, settings, sampling, or harness changed; establish a new measurement baseline")
  (let [before (mapv #(comparison/summarize-query baseline %) (:results baseline))
        after (mapv #(comparison/summarize-query candidate %) (:results candidate))
        _ (comparison/check! (every? #(and (= :match (get-in % [:modes :adapter :status]))
                                           (= :match (get-in % [:modes :native :status]))) before)
                             "Baseline native and Datalevin calls must all match")
        queries
        (mapv (fn [a b]
                (let [base (get-in a [:modes :adapter]) next (get-in b [:modes :adapter])
                      correct? (and (= :match (:status next))
                                    (= :match (get-in b [:modes :native :status])))
                      speedup (when correct? (ratio (:median-ms base) (:median-ms next)))
                      base-relative (ratio (:median-ms base) (get-in a [:modes :native :median-ms]))
                      next-relative (ratio (:median-ms next) (get-in b [:modes :native :median-ms]))]
                  {:id (:id a) :baseline-ms (:median-ms base) :candidate-ms (:median-ms next)
                   :status (:status next) :native-status (get-in b [:modes :native :status])
                   :delta-ms (when correct? (- (:median-ms next) (:median-ms base)))
                   :speedup speedup :speedup-relative-to-native (when correct? (ratio base-relative next-relative))
                   :baseline-native-ms (get-in a [:modes :native :median-ms])
                   :candidate-native-ms (get-in b [:modes :native :median-ms])})) before after)
        all-correct? (every? #(and (= :match (:status %)) (= :match (:native-status %))) queries)
        baseline-seconds (/ (reduce + (map :baseline-ms queries)) 1000.0)
        candidate-seconds (when all-correct? (/ (reduce + (map :candidate-ms queries)) 1000.0))
        base-native (/ (reduce + (map :baseline-native-ms queries)) 1000.0)
        next-native (/ (reduce + (map :candidate-native-ms queries)) 1000.0)]
    {:format-version 1 :experiment experiment :query-count (count queries)
     :full-corpus? (= 113 (count queries)) :all-answers-match? all-correct?
     :timing-statistic :sum-of-per-query-client-wall-medians
     :attribution :descriptive-requires-controlled-repeats-and-ablations
     :sampling (select-keys (:options baseline) [:samples :warmups :timeout-ms])
     :coverage (frequencies (map :status queries))
     :baseline-seconds baseline-seconds :candidate-seconds candidate-seconds
     :speedup (when all-correct? (ratio baseline-seconds candidate-seconds))
     :baseline-native-seconds base-native :candidate-native-seconds next-native
     :native-time-ratio (ratio next-native base-native)
     :speedup-relative-to-native
     (when all-correct? (ratio (ratio baseline-seconds base-native)
                              (ratio candidate-seconds next-native)))
     :changed-resources (changed-resources (:implementation-sha256 baseline)
                                           (:implementation-sha256 candidate))
     :queries queries}))

(defn file-sha256 [file]
  (let [digest (MessageDigest/getInstance "SHA-256")]
    (with-open [stream (io/input-stream file)]
      (let [buffer (byte-array 65536)]
        (loop [] (let [n (.read stream buffer)]
                   (when (pos? n) (.update digest buffer 0 n) (recur))))))
    (apply str (map #(format "%02x" (bit-and (int %) 0xff)) (.digest digest)))))

(defn markdown [report]
  (str "# JOB optimization progress: " (get-in report [:experiment :id]) "\n\n"
       "Parent: " (get-in report [:experiment :parent]) ". Mechanism: "
       (get-in report [:experiment :mechanism]) ".\n\n"
       (get-in report [:experiment :hypothesis]) "\n\n"
       (:query-count report) " fixed queries; candidate coverage: " (pr-str (:coverage report)) ".\n\n"
       (if (:all-answers-match? report)
         (format "Datalevin: %.6f s → %.6f s; observed speedup %.4fx.\n\n"
                 (:baseline-seconds report) (:candidate-seconds report) (or (:speedup report) 0.0))
         "Candidate correctness regressed. No aggregate speedup is reported; failed/mismatching queries stay in the fixed query set.\n\n")
       (format "Paired native totals: %.6f s → %.6f s.\n\n"
               (:baseline-native-seconds report) (:candidate-native-seconds report))
       "Timings include planning, execution, consumption, and cleanup. Samples are descriptive; "
       "causal attribution requires controlled repeats, one mechanism at a time, and ablations. "
       "Native-relative ratios expose control drift and do not replace elapsed time or establish causality.\n\n"
       "See summary.json, queries.csv, changed-resources.json, experiment.json, and inputs.json.\n"))

(defn write-report! [output baseline-path candidate-path experiment-path]
  (let [report (summarize (edn/read-string (slurp baseline-path))
                          (edn/read-string (slurp candidate-path))
                          (json/read-value (slurp experiment-path) json/keyword-keys-object-mapper))
        directory (io/file output)
        inputs {:baseline {:path (str baseline-path) :sha256 (file-sha256 baseline-path)}
                :candidate {:path (str candidate-path) :sha256 (file-sha256 candidate-path)}
                :experiment {:path (str experiment-path) :sha256 (file-sha256 experiment-path)}}]
    (io/make-parents directory)
    (Files/createDirectory (.toPath directory) (make-array FileAttribute 0))
    (doseq [[name content] [["summary.json" report] ["inputs.json" inputs]
                            ["experiment.json" (:experiment report)]
                            ["changed-resources.json" (:changed-resources report)]]]
      (json/write-value (io/file directory name) content))
    (spit (io/file directory "README.md") (markdown report))
    (comparison/write-csv! (io/file directory "queries.csv")
                           [:query :status :native-status :baseline-ms :candidate-ms :delta-ms :speedup
                            :baseline-native-ms :candidate-native-ms :speedup-relative-to-native]
                           (for [q (:queries report)]
                             (map #(get q %) [:id :status :native-status :baseline-ms :candidate-ms :delta-ms :speedup
                                              :baseline-native-ms :candidate-native-ms :speedup-relative-to-native])))
    report))

(defn -main [baseline candidate experiment output & extra]
  (comparison/check! (and baseline candidate experiment output (empty? extra))
                     "Usage: clojure -M:job-progress BASELINE.edn CANDIDATE.edn EXPERIMENT.json NEW_OUTPUT")
  (prn (select-keys (write-report! output baseline candidate experiment)
                    [:query-count :all-answers-match? :baseline-seconds :candidate-seconds :speedup]))
  (shutdown-agents))
