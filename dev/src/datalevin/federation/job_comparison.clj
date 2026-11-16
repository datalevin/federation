(ns datalevin.federation.job-comparison
  "Validate and summarize arbitrary completed JOB baseline runs. No database access."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jsonista.core :as json])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.security MessageDigest]))

(defn check! [condition message]
  (when-not condition (throw (ex-info message {}))))

(defn sql-digest [sql]
  ;; The harness hashes an EDN vector containing SQL, not the bare file bytes.
  (let [digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (pr-str [sql]) "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and (int %) 0xff)) digest))))

(defn normalized [rows]
  (set (map #(mapv (fn [v] (when (some? v) (str v))) %) rows)))

(defn modes [run]
  (cond-> [:native (get-in run [:options :engine])]
    (get-in run [:options :include-adapter?]) (conj :adapter)))

(defn validate! [run]
  (let [{:keys [results options query-ids]} run
        {:keys [samples warmups queries engine]} options
        expected-count (if queries (count queries) 113)]
    (check! (= 2 (:format-version run)) "Expected baseline format version 2")
    (check! (= :complete (:status run)) "Run is incomplete; retain its checkpoint and rerun into a new directory")
    (check! (and (#{:trino :drill :duckdb :accio} engine) (pos-int? samples) (nat-int? warmups)) "Invalid sampling options")
    (check! (= expected-count (count results) (count (set query-ids))) "Missing or duplicate queries")
    (check! (= query-ids (mapv :id results)) "Recorded query coverage disagrees with results")
    (when queries (check! (= (set queries) (set query-ids)) "Requested query coverage disagrees with results"))
    (doseq [q results]
      (check! (= (:sha256 q) (sql-digest (:sql q))) "SQL hash disagrees with recorded SQL")
      (check! (= samples (count (:samples q))) "Missing timed samples")
      (check! (= warmups (count (:warmups q))) "Missing warmup samples")
      (check! (set? (:expected q)) "Missing native oracle")
      (doseq [[index sample] (map-indexed vector (:samples q))]
        (check! (= index (:index sample)) "Sample indexes are inconsistent")
        (check! (and (= (set (modes run)) (set (:order sample)))
                     (= (count (modes run)) (count (:order sample)))) "Missing or duplicate modes")
        (doseq [mode (modes run) :let [s (get sample mode)]]
          (check! (and (#{:ok :failed} (:status s)) (number? (:elapsed-ns s))
                       (<= 0 (:elapsed-ns s))) "Invalid or missing measurement")
          (when (= :ok (:status s))
            (check! (and (set? (:rows s))
                         (= (:answer-matches? s)
                            (= (normalized (:expected q)) (normalized (:rows s)))))
                    "Correctness flag disagrees with returned rows")))))
    run))

(defn median [numbers]
  (let [xs (vec (sort numbers)) n (count xs)]
    (check! (pos? n) "Cannot summarize an empty sample")
    (if (odd? n) (double (xs (quot n 2)))
        (/ (+ (xs (dec (quot n 2))) (xs (quot n 2))) 2.0))))

(defn classify [sample]
  (cond (not= :ok (:status sample)) :failed
        (true? (:answer-matches? sample)) :match
        :else :mismatch))

(defn summarize-mode [samples]
  (let [counts (frequencies (map classify samples))]
    {:status (cond (:failed counts) :failed (:mismatch counts) :mismatch :else :match)
     :sample-counts counts
     :median-ms (/ (median (map :elapsed-ns samples)) 1e6)
     :attempted-seconds (/ (reduce + (map :elapsed-ns samples)) 1e9)}))

(defn summarize-query [run q]
  {:id (:id q) :sha256 (:sha256 q) :sql-normalization (:sql-normalization q)
   :diagnostic-status (get-in q [:diagnostic :status])
   :warmup-counts (frequencies (for [w (:warmups q) m (modes run)] (classify (get w m))))
   :modes (into {} (for [m (modes run)] [m (summarize-mode (map m (:samples q)))]))})

(defn matching? [query]
  (every? #(= :match (:status %)) (vals (:modes query))))

(defn totals [queries modes]
  (into {} (for [m modes]
             [m (/ (reduce + (map #(get-in % [:modes m :median-ms]) queries)) 1000.0)])))

(defn comparison-key [run]
  {:protocol (select-keys run [:timing-scope :sampling :additional-tuple-oracles?
                              :adapter-limits :database :native-settings :client
                              :source-metadata :implementation-sha256])
   :options (select-keys (:options run) [:samples :warmups :timeout-ms :schema :include-adapter?
                                        :capture-plans? :drill-ansi-not-equal?])
   :environment (select-keys (:environment run)
                            ["source" "resources" "client_java" "platform" "architecture" "cpus"
                             "physical_memory_bytes" "corpus_manifest_sha256"
                             "runner_sha256" "config_sha256" "revisions" "dataset_provenance" "semantics"])
   :queries (mapv #(select-keys % [:id :sha256 :expected]) (:results run))})

(defn summarize [runs]
  (check! (seq runs) "Supply at least one raw run")
  (run! validate! runs)
  (check! (= (count runs) (count (set (map #(get-in % [:options :engine]) runs))))
          "Supply one run per engine; use --samples for repeated measurements")
  (check! (apply = (map comparison-key runs))
          "Runs differ in corpus, oracle, implementation, environment, or protocol; report them separately")
  (let [summaries (mapv (fn [r] {:engine (get-in r [:options :engine])
                               :modes (modes r) :options (:options r)
                               :queries (mapv #(summarize-query r %) (:results r))}) runs)
        common (filterv (fn [id] (every? (fn [r] (matching? (some #(when (= id (:id %)) %)
                                                                                (:queries r)))) summaries))
                        (:query-ids (first runs)))]
    {:format-version 1 :query-count (count (:query-ids (first runs)))
     :full-corpus? (= 113 (count (:query-ids (first runs))))
     :common-correct-query-ids common
     :timing-statistic :sum-of-per-query-client-wall-medians
     :comparisons
     (mapv (fn [{:keys [queries modes] :as r}]
             (let [selected (filterv #(contains? (set common) (:id %)) queries)]
               (assoc r
                      :coverage (into {} (for [m modes]
                                           [m (frequencies (map #(get-in % [:modes m :status]) queries))]))
                      :attempted-seconds (into {} (for [m modes]
                                                    [m (reduce + (map #(get-in % [:modes m :attempted-seconds]) queries))]))
                      :common-correct-seconds (totals selected modes)
                      :exclusions (mapv (fn [q] {:id (:id q)
                                                :statuses (into {} (for [[m s] (:modes q)] [m (:status s)]))})
                                       (remove matching? queries))))) summaries)}))

(defn csv-cell [value]
  (str "\"" (str/replace (if (keyword? value) (name value) (str value)) "\"" "\"\"") "\""))

(defn write-csv! [file header rows]
  (spit file (str/join "\n" (map #(str/join "," (map csv-cell %)) (cons header rows)))))

(defn markdown [summary]
  (str "# JOB baseline comparison\n\n"
       (:query-count summary) " queries; " (count (:common-correct-query-ids summary))
       " match in every timed sample of every mode and run. "
       "Each row retains its own paired native/Datalevin controls.\n\n"
       "| Run | Mode | Matching queries | Mismatches | Failures | All attempted calls, s | Common matching subset, s |\n"
       "| --- | --- | ---: | ---: | ---: | ---: | ---: |\n"
       (str/join "\n"
                 (for [r (:comparisons summary) m (:modes r)
                       :let [counts (get-in r [:coverage m])]]
                   (format "| %s | %s | %d | %d | %d | %.3f | %.3f |"
                           (name (:engine r)) (name m) (get counts :match 0)
                           (get counts :mismatch 0) (get counts :failed 0)
                           (get-in r [:attempted-seconds m])
                           (get-in r [:common-correct-seconds m]))))
       "\n\nCommon subset: " (str/join ", " (map name (:common-correct-query-ids summary))) ".\n\n"
       "Common-subset time sums per-query medians. Attempted time sums all timed calls, including failures and mismatches; "
       "these columns have different denominators when there are repeated samples or exclusions. "
       "A query is classified as failed if any timed sample failed, otherwise as mismatching if any answer differed.\n\n"
       "Startup, mapping discovery, Datalevin SQL-to-Datalog translation, native answer oracles, warmups, and plan capture are excluded. "
       "Engine optimization, including Accio's rewrite and cardinality estimation, remains inside timed calls. "
       "The native oracle warms the source before timing. One sample is a coverage measurement, not statistical evidence. "
       "Collation differences remain mismatches. See summary.json, queries.csv, samples.csv, plans.edn, "
       "and the input run files for exclusions, individual observations, plans, and environment.\n"))

(defn write-summary! [output paths]
  (let [runs (mapv #(edn/read-string (slurp %)) paths)
        summary (assoc (summarize runs) :input-files (mapv str paths))
        directory (io/file output)]
    (io/make-parents directory)
    (Files/createDirectory (.toPath directory) (make-array FileAttribute 0))
    (json/write-value (io/file directory "summary.json") summary)
    (spit (io/file directory "README.md") (markdown summary))
    (write-csv! (io/file directory "queries.csv")
                ["run" "query" "mode" "status" "samples" "median_ms" "common_correct"]
                (for [r (:comparisons summary) q (:queries r) m (:modes r)
                      :let [s (get-in q [:modes m])]]
                  [(:engine r) (:id q) m (:status s) (get-in r [:options :samples]) (:median-ms s)
                   (contains? (set (:common-correct-query-ids summary)) (:id q))]))
    (write-csv! (io/file directory "samples.csv")
                ["run" "query" "sample" "order" "mode" "status" "elapsed_ms" "query_id" "failure"]
                (for [r runs q (:results r) sample (:samples q) m (:order sample)
                      :let [s (get sample m)]]
                  [(get-in r [:options :engine]) (:id q) (:index sample)
                   (str/join ">" (map name (:order sample))) m (classify s)
                   (/ (:elapsed-ns s) 1e6) (:query-id s) (when (:failure s) (pr-str (:failure s)))]))
    (spit (io/file directory "plans.edn")
          (pr-str (into {} (for [r runs]
                            [(get-in r [:options :engine])
                             (into {} (for [q (:results r)] [(:id q) (:diagnostic q)]))]))))
    summary))

(defn -main [output & paths]
  (check! (and output (seq paths)) "Usage: clojure -M:job-baselines-report <new-directory> <run.edn> [run.edn ...]")
  (let [summary (write-summary! output paths)]
    (prn (select-keys summary [:query-count :full-corpus? :common-correct-query-ids])))
  (shutdown-agents))
