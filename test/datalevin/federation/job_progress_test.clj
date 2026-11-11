(ns datalevin.federation.job-progress-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [datalevin.federation.job-competitors-test :as fixtures]
            [datalevin.federation.job-progress :as progress]
            [jsonista.core :as json])
  (:import [java.nio.file Files FileAlreadyExistsException]
           [java.nio.file.attribute FileAttribute]))

(def experiment {:id "O01" :parent "B0" :kind "single-change"
                 :mechanism "candidate construction" :hypothesis "Less repeated construction reduces total time."})

(defn candidate []
  (-> (fixtures/fixture :duckdb)
      (assoc-in [:implementation-sha256 "engine"] "optimized")
      (update :results (fn [qs] (mapv #(update % :samples
                                              (fn [ss] (mapv (fn [s] (update-in s [:adapter :elapsed-ns] / 2)) ss))) qs)))))

(deftest permits-optimizer-changes-and-measures-fixed-corpus
  (let [r (progress/summarize (fixtures/fixture :duckdb) (candidate) experiment)]
    (is (= 2.0 (:speedup r) (:speedup-relative-to-native r)))
    (is (= 1.0 (:native-time-ratio r)))
    (is (= {:match 2} (:coverage r)))
    (is (= [{:resource "engine" :before "same" :after "optimized"}] (:changed-resources r))))
  (let [c (update (candidate) :results
                  #(mapv (fn [q] (update q :samples (fn [ss] (mapv (fn [s] (update-in s [:native :elapsed-ns] / 2)) ss)))) %))
        r (progress/summarize (fixtures/fixture :duckdb) c experiment)]
    (is (= 2.0 (:speedup r)))
    (is (= 1.0 (:speedup-relative-to-native r)))))

(deftest rejects-confounded-or-invalid-evidence
  (doseq [c [(assoc-in (candidate) [:options :samples] 1)
             (assoc (candidate) :native-settings #{["changed"]})
             (assoc-in (candidate) [:implementation-sha256 (first progress/protocol-resources)] "new harness")
             (assoc-in (candidate) [:environment "resources"] {"threads" 99})
             (assoc-in (candidate) [:environment "semantics"] {"text_min_order" "source-libc"})
             (assoc-in (candidate) [:results 0 :sql] "different SQL")]]
    (is (thrown? Exception (progress/summarize (fixtures/fixture :duckdb) c experiment))))
  (is (thrown? Exception (progress/summarize (fixtures/fixture :duckdb) (candidate) {}))))

(deftest correctness-regressions-cannot-become-a-speedup
  (doseq [sample [{:status :failed :elapsed-ns 1 :failure {:message "timeout"}}
                  {:status :ok :elapsed-ns 1 :rows #{["wrong"]} :answer-matches? false}]]
    (let [c (assoc-in (candidate) [:results 0 :samples 0 :adapter] sample)
          r (progress/summarize (fixtures/fixture :duckdb) c experiment)]
      (is (= 2 (:query-count r)))
      (is (false? (:all-answers-match? r)))
      (is (nil? (:candidate-seconds r)))
      (is (nil? (:speedup r)))
      (is (nil? (get-in r [:queries 0 :speedup]))))))

(deftest progress-artifacts-are-new-and-traceable
  (let [root (.toFile (Files/createTempDirectory "job-progress" (make-array FileAttribute 0)))
        base (io/file root "baseline.edn") next (io/file root "candidate.edn")
        spec (io/file root "experiment.json") output (io/file root "report")]
    (try
      (spit base (pr-str (fixtures/fixture :duckdb)))
      (spit next (pr-str (candidate)))
      (json/write-value spec experiment)
      (progress/write-report! output base next spec)
      (doseq [name ["summary.json" "queries.csv" "inputs.json" "changed-resources.json" "experiment.json" "README.md"]]
        (is (pos? (.length (io/file output name)))))
      (is (= 64 (count (progress/file-sha256 base))))
      (is (thrown? FileAlreadyExistsException (progress/write-report! output base next spec)))
      (finally (doseq [file (reverse (file-seq root))] (io/delete-file file))))))
