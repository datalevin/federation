(ns datalevin.federation.postgres-job-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.federation.postgres-job :as job]))

(defn pairs [native adapter]
  (mapv (fn [n a] {:native {:elapsed-ns n} :adapter {:elapsed-ns a}}) native adapter))

(deftest a-win-requires-repeated-measurements-and-a-margin
  (let [fast (pairs (repeat 7 200) (repeat 7 100))]
    (is (:win? (job/comparison fast true)))
    (is (= 2.0 (:speedup (job/comparison fast true))))
    (is (false? (:win? (job/comparison fast false))))
    (is (false? (:win? (job/comparison (take 4 fast) true)))))
  (testing "a faster median alone does not establish a repeatable win"
    (is (false? (:win? (job/comparison (pairs (repeat 7 200) [100 100 100 100 300 300 300]) true)))))
  (is (false? (:win? (job/comparison (pairs (repeat 7 105) (repeat 7 100)) true)))))

(deftest summary-retains-the-observed-tail
  (is (= {:samples 5 :min-ns 1 :median-ns 3 :p95-ns 100 :max-ns 100}
         (job/summarize [3 1 100 2 4])))
  (is (= 2.5 (:median-ns (job/summarize [4 2 1 3]))))
  (is (nil? (job/summarize []))))

(deftest benchmark-configuration-stays-explicit
  (is (= :6a (:query (job/options! {}))))
  (doseq [options [{:query :unknown} {:samples 0} {:timeout-ms -1}
                   {:jdbc-url "secret"} {:limits {:made-up 1}} {:limits {:external-requests 0}}]]
    (is (thrown? clojure.lang.ExceptionInfo (job/options! options))))
  (is (= 8192 (get-in (job/options! {:limits {:external-requests 8192}}) [:limits :external-requests]))))

(deftest job-6d-retains-its-keyword-set-and-year-bound
  (is (= :6d (:query (job/options! {:query :6d}))))
  (is (= true (:filtered-statistics? (job/options! {}))))
  (is (= '[(> ?year 2000)] (get-in job/query-6d [:where 5])))
  (is (= (list 'in '?keyword job/hero-keywords) (first (get-in job/query-6d [:where 1]))))
  (let [sql (job/sql-6a "public" true :6d)]
    (is (.contains sql "k.keyword IN ('superhero','sequel','second-part','marvel-comics','based-on-comic','tv-special','fight','violence')"))
    (is (.contains sql "t.production_year > 2000"))))

(deftest sql-retains-the-original-job-predicates
  (let [sql (job/sql-6a "quoted\"schema" true)]
    (is (.contains sql "\"quoted\"\"schema\".\"cast_info\""))
    (is (.contains sql "SELECT MIN(k.keyword), MIN(n.name), MIN(t.title)"))
    (is (.contains sql "ci.movie_id = mk.movie_id"))
    (is (.contains sql "n.name LIKE '%Downey%Robert%'"))
    (is (not (.contains sql "COLLATE")))))

(deftest followup-queries-preserve-output-shapes-and-original-filters
  (doseq [[variant width] [[:6b 3] [:17c 2] [:17d 1] [:20b 1]]]
    (is (= variant (:query (job/options! {:query variant}))))
    (is (= width (count (get-in job/query-specs [variant :outputs]))))
    (is (= width (count (get-in job/query-specs [variant :query :find])))))
  (is (.contains (job/sql-query "public" true :6b) "t.production_year > 2014"))
  (is (.contains (job/sql-query "public" true :17c) "SELECT MIN(n.name), MIN(n.name)"))
  (is (.contains (job/sql-query "public" false :17c) "SELECT DISTINCT n.name, n.name"))
  (is (.contains (job/sql-query "public" true :17d) "n.name LIKE '%Bert%'"))
  (let [sql (job/sql-query "quoted\"schema" true :20b)]
    (doseq [clause ["\"quoted\"\"schema\".\"comp_cast_type\" AS cct1"
                    "\"quoted\"\"schema\".\"comp_cast_type\" AS cct2"
                    "chn.name NOT LIKE '%Sherlock%'"
                    "(chn.name LIKE '%Tony%Stark%' OR chn.name LIKE '%Iron%Man%')"
                    "cct1.id = cc.subject_id" "cct2.id = cc.status_id"]]
      (is (.contains sql clause)))))
