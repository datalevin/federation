(ns datalevin.federation.postgres-job-suite-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [datalevin.federation.postgres-job-sql :as sql]
            [datalevin.federation.postgres-job-suite :as suite]
            [datalevin.federation.postgres-job-native :as native]))

(def catalogs
  {"name" {:columns {"id" {:type "int4" :not-null? true} "name" {:type "text" :not-null? true} "gender" {:type "varchar" :not-null? false}}}
   "title" {:columns {"id" {:type "int4" :not-null? true} "title" {:type "text" :not-null? true} "production_year" {:type "int4" :not-null? false}}}
   "cast_info" {:columns {"id" {:type "int4" :not-null? true} "person_id" {:type "int4" :not-null? true} "movie_id" {:type "int4" :not-null? true}}}})

(deftest sql-precedence-escaping-and-independent-outputs
  (let [parsed (sql/parse "SELECT MIN(n.name) AS a, MIN(n.name) AS b FROM name AS n WHERE (n.name LIKE 'O''Brien%' OR n.gender IN ('m', 'f')) AND n.id = n.id;")]
    (is (= [[:col "n" "name"] [:col "n" "name"]] (:outputs parsed)))
    (is (= :and (first (:where parsed))))
    (is (= [:like [:col "n" "name"] [:lit "O'Brien%"]] (get-in parsed [:where 1 1])))
    (is (str/includes? (sql/sql-query parsed "MixedCase" true) "\"MixedCase\".\"name\""))
    (let [spec (sql/compile-query parsed catalogs)]
      (is (= '[(min ?n_name) (min ?n_name)] (get-in spec [:query :find])))
      (is (= #{'?n_name} (set (keys (:native-extrema spec)))))
      ;; A conditionally needed nullable column stays inside its OR branch.
      (is (not-any? #(and (vector? %) (= :job.name/gender (nth % 2 nil))) (get-in spec [:query :where])))
      (is (some #(and (seq? %) (= 'or-join (first %))) (get-in spec [:query :where]))))))

(deftest every-from-alias-must-have-an-existence-witness
  (let [spec (sql/compile-query
               (sql/parse "SELECT MIN(n.name) AS a FROM name AS n, cast_info AS ci, title AS t WHERE n.id=ci.person_id AND ci.movie_id=t.id AND n.name='X';") catalogs)
        clauses (get-in spec [:query :where])]
    (is (some #(= ['$t '?row_t :job.title/id '?identity_t] %) clauses))
    (is (some #(= ['$ci '?row_ci :job.cast_info/movie_id '?row_t] %) clauses))
    (is (some #(= ['$ci '?row_ci :job.cast_info/person_id '?row_n] %) clauses))))

(deftest null-predicates-and-independent-null-minima-preserve-absence
  (let [spec (sql/compile-query (sql/parse "SELECT MIN(t.title) AS a FROM title AS t WHERE t.production_year IS NULL;") catalogs)]
    (is (some #(= '(not-join [?row_t] [$t ?row_t :job.title/production_year _]) %) (get-in spec [:query :where])))
    (is (not-any? #(and (vector? %) (= :job.title/production_year (nth % 2 nil))) (get-in spec [:query :where]))))
  (let [spec (sql/compile-query (sql/parse "SELECT MIN(t.production_year) AS a FROM title AS t WHERE t.id=t.id;") catalogs)]
    (is (= '#{?t_production_year} (:ignore-nil-extrema spec)))
    (is (some #(and (seq? %) (= 'or-join (first %))) (get-in spec [:query :where])))
    (is (not-any? #(and (vector? %) (= :job.title/production_year (nth % 2 nil))) (get-in spec [:query :where]))))
  (let [spec (sql/compile-query (sql/parse "SELECT MIN(t.production_year) AS a FROM title AS t WHERE t.production_year BETWEEN 2000 AND 2010;") catalogs)]
    (is (some #(= '[(<= 2000 ?t_production_year 2010)] %) (get-in spec [:query :where])))))

(deftest bounded-attempts-and-totals-never-hide-failures
  (let [ok {:status :ok :samples [{:native {:status :ok :elapsed-ns 30} :adapter {:status :ok :elapsed-ns 20}}]}
        failed {:id :2a :status :incomplete :samples [{:native {:status :ok :elapsed-ns 50} :adapter {:status :failed :elapsed-ns 60 :failure {:error :query-source/timeout}}}]}
        report (suite/summary [(assoc ok :id :1a) failed] 2 1)]
    (is (= 80 (get-in report [:totals 0 :native-total-ns])))
    (is (= 20 (get-in report [:totals 0 :adapter-verified-subtotal-ns])))
    (is (= [:2a] (:incomplete-queries report)))
    (is (nil? (get-in report [:totals 0 :adapter-full-suite-ns])))
    (is (nil? (get-in report [:totals 0 :speedup])))
    (let [full (suite/summary (vec (repeat 113 ok)) 113 1)]
      (is (true? (get-in full [:totals 0 :full-suite-comparable?])))
      (is (= 1.5 (get-in full [:totals 0 :speedup])))))
  (is (= :benchmark/test (get-in (suite/attempt #(throw (ex-info "secret" {:error :benchmark/test}))) [:failure :error])))
  (is (not (str/includes? (pr-str (suite/attempt #(throw (Exception. "secret")))) "secret"))))

(deftest reject-invalid-options
  (doseq [opts [{:samples 0} {:timeout-ms -1} {:queries [:6a :6a]} {:secret "no"} {:limits {:bogus 1}} {:limits nil}
               {:max-row-bytes 0} {:max-row-bytes 1048577}]]
    (is (thrown? Exception (suite/options! opts)))))

(deftest original-corpus-inventory
  (when (.isDirectory (io/file (:corpus suite/defaults)))
    (let [corpus (suite/corpus! suite/defaults) parsed (mapv #(sql/parse (:sql %)) corpus)]
      (is (= 113 (count parsed)))
      (is (= 295 (reduce + (map #(count (:outputs %)) parsed))))
      (is (= [:1a :1b :1c] (mapv :id (take 3 corpus))))
      (is (= :33c (:id (last corpus)))))))

(deftest corrected-native-control-keeps-unmatched-work-out-of-totals
  (let [reference {:results [{:id :1a :status :ok :samples [{:adapter {:status :ok :elapsed-ns 20}}]}
                             {:id :1b :status :incomplete :samples [{:adapter {:status :failed :elapsed-ns 60}}]}]}
        entries [{:id :1a :status :ok :samples [{:native {:status :ok :elapsed-ns 30}}]}
                 {:id :1b :status :ok :samples [{:native {:status :ok :elapsed-ns 50}}]}]
        report (native/comparison reference entries 1)]
    (is (= 80 (get-in report [:totals 0 :native-total-ns])))
    (is (= 30 (get-in report [:totals 0 :native-matched-subtotal-ns])))
    (is (= 20 (get-in report [:totals 0 :adapter-verified-subtotal-ns])))
    (is (= 80 (get-in report [:totals 0 :adapter-attempted-ns])))
    (is (= 1 (:adapter-verified report)))
    (is (false? (:full-suite-comparable? report)))
    (is (nil? (get-in report [:totals 0 :speedup])))
    (is (nil? (get-in report [:totals 0 :adapter-full-suite-ns])))))


(deftest benchmark-limits-can-be-overridden-independently
  (let [configured (suite/options! {:limits {:external-requests 8192} :max-row-bytes 65536})]
    (is (= 8192 (get-in configured [:limits :external-requests])))
    (is (= 8589934592 (get-in configured [:limits :retained-bytes])))
    (is (= 65536 (:max-row-bytes configured)))))
