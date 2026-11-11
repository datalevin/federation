(ns datalevin.federation.job-competitors-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datalevin.federation.job-competitors :as competitors]
            [datalevin.federation.job-comparison :as report]
            [datalevin.federation.postgres-job-sql :as sql]
            [datalevin.federation.postgres-job-suite :as suite])
  (:import [java.nio.file Files FileAlreadyExistsException]
           [java.nio.file.attribute FileAttribute]))

(deftest comparison-options
  (let [opts (competitors/options! {:engine :trino :endpoint "http://127.0.0.1:18080"})]
    (is (nil? (:queries opts)))
    (is (:drill-ansi-not-equal? opts))
    (is (= 300000 (:timeout-ms opts))))
  (doseq [bad [{:engine :other} {:samples 0} {:warmups 4} {:timeout-ms 999}
               {:queries [:6a :6a]} {:capture-plans? :yes} {:typo true}]]
    (is (thrown? Exception (competitors/options!
                           (merge {:engine :trino :endpoint "http://localhost:18080"} bad)))))
  (let [path (Files/createTempFile "job-output" ".edn" (make-array FileAttribute 0))]
    (try
      (spit (.toFile path) "preserve")
      (is (thrown? FileAlreadyExistsException (competitors/reserve-output! (str path))))
      (is (= "preserve" (slurp (.toFile path))))
      (finally (Files/delete path)))))

(deftest inequality-normalization
  (doseq [statement ["SELECT '!=' WHERE x != 1"
                     "SELECT 'it''s !=' WHERE x != 1"
                     "SELECT \"!=\" WHERE x != 1"
                     "-- !=\nSELECT 1 WHERE x != 1"
                     "/* != */ SELECT 1 WHERE x != 1"]]
    (is (= (str/replace statement "x != 1" "x <> 1")
           (competitors/ansi-not-equal statement))))
  (let [entries (suite/corpus! suite/defaults)
        shape #(select-keys (sql/parse %) [:outputs :from :where])]
    (is (= 113 (count entries)))
    (is (= 20 (count (filter #(not= (:sql %) (competitors/ansi-not-equal (:sql %))) entries))))
    (doseq [entry entries]
      (is (= (shape (:sql entry)) (shape (competitors/ansi-not-equal (:sql entry))))))))

(defn sample [ms]
  {:status :ok :answer-matches? true :rows #{["answer"]} :elapsed-ns (* ms 1000000)})

(defn fixture [engine]
  {:format-version 2 :status :complete :query-ids [:6a :11a]
   :options {:engine engine :queries [:6a :11a] :samples 3 :warmups 0 :include-adapter? true}
   :implementation-sha256 {"engine" "same"}
   :results (mapv (fn [id]
                    {:id id :sql "SELECT 1" :sha256 (report/sql-digest "SELECT 1")
                     :expected #{["answer"]} :warmups []
                     :samples (mapv (fn [i ms] {:index i :order [:native engine :adapter]
                                               :native (sample (* 2 ms))
                                               :adapter (sample ms) engine (sample (* 3 ms))})
                                    (range) [10 90 20])}) [:6a :11a])})

(deftest fair-common-subset
  (let [trino (assoc-in (fixture :trino) [:results 1 :samples 0 :trino]
                      (assoc (sample 5) :answer-matches? false :rows #{["different"]}))
        drill (fixture :drill)
        summary (report/summarize [trino drill])]
    (is (= [:6a] (:common-correct-query-ids summary)))
    (is (false? (:full-corpus? summary)))
    (is (= {:match 1 :mismatch 1} (get-in summary [:comparisons 0 :coverage :trino])))
    (is (= {:native 0.04 :trino 0.06 :adapter 0.02}
           (get-in summary [:comparisons 0 :common-correct-seconds])))
    (is (= 0.24 (get-in summary [:comparisons 0 :attempted-seconds :adapter]))))
  (testing "A timeout in one sample excludes the query even when its other samples match"
    (let [run (assoc-in (fixture :drill) [:results 0 :samples 1 :drill]
                        {:status :failed :elapsed-ns 300000000000 :failure {:message "timeout"}})
          summary (report/summarize [run])]
      (is (= [:11a] (:common-correct-query-ids summary)))
      (is (= {:failed 1 :match 1} (get-in summary [:comparisons 0 :coverage :drill])))
      (is (> (get-in summary [:comparisons 0 :attempted-seconds :drill]) 300))))
  (is (= 2.5 (report/median [1 4 2 3]))))

(deftest invalid-evidence-is-rejected
  (doseq [invalid [(assoc (fixture :trino) :status :running)
                   (assoc (fixture :trino) :query-ids [:6a :6a])
                   (update-in (fixture :trino) [:results 0 :samples] pop)
                   (assoc-in (fixture :trino) [:results 0 :sql] "SELECT 2")
                   (assoc-in (fixture :trino) [:results 0 :samples 0 :trino :answer-matches?] false)]]
    (is (thrown? Exception (report/summarize [invalid]))))
  (doseq [different [(assoc-in (fixture :drill) [:implementation-sha256 "engine"] "changed")
                     (assoc (fixture :drill) :native-settings #{["different"]})
                     (assoc-in (fixture :drill) [:options :timeout-ms] 1000)]]
    (is (thrown? Exception (report/summarize [(fixture :trino) different]))))
  (is (thrown? Exception (report/summarize [(fixture :trino) (fixture :trino)]))))

(deftest report-artifacts
  (let [directory (.toFile (Files/createTempDirectory "job-summary" (make-array FileAttribute 0)))
        raw (io/file directory "run.edn") output (io/file directory "summary")]
    (try
      (spit raw (pr-str (fixture :drill)))
      (report/write-summary! (str output) [(str raw)])
      (doseq [file ["README.md" "summary.json" "queries.csv" "samples.csv" "plans.edn"]]
        (is (pos? (.length (io/file output file)))))
      (is (thrown? FileAlreadyExistsException (report/write-summary! (str output) [(str raw)])))
      (finally (doseq [file (reverse (file-seq directory))] (io/delete-file file))))))
