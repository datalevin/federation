(ns datalevin.federation.postgres-job-suite-integration-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres-job-suite :as suite]
            [datalevin.federation.postgres-job-integration-test :refer [with-job]])
  (:import [java.sql Statement]))

(deftest generated-queries-match-original-sql-including-disjunctions-and-orphans
  (with-job
    (fn [profile schema stmt]
      (doseq [statement ["DELETE FROM name WHERE name IS NULL"
                         "ALTER TABLE name ALTER COLUMN name SET NOT NULL"
                         "ALTER TABLE title ALTER COLUMN title SET NOT NULL"
                         "ALTER TABLE name ADD gender text"
                         "UPDATE name SET gender='f' WHERE id=2"
                         "INSERT INTO name (id,name) VALUES (5,'AAA missing movie')"
                         "INSERT INTO cast_info VALUES (100,999,5)"]]
        (.execute ^Statement stmt statement))
      (let [profile (assoc profile :compute-context-id :test/job-suite :statement-timeout-ms 10000)
            catalogs (into {} (map (fn [t] [t (pg/discover profile [schema t])]) ["name" "title" "cast_info"]))
            sqls ["SELECT MIN(n.name) AS n, MIN(t.title) AS t FROM name AS n, cast_info AS ci, title AS t WHERE n.id=ci.person_id AND ci.movie_id=t.id AND (n.name LIKE '%Jr.%' OR n.gender='f');"
                  "SELECT MIN(n.name) AS n, MIN(n.name) AS duplicate FROM name AS n, cast_info AS ci, title AS t WHERE n.id=ci.person_id AND ci.movie_id=t.id AND n.name LIKE '%';"
                  "SELECT MIN(n.name) AS n FROM name AS n WHERE n.name LIKE '%NotPresent%';"
                  "SELECT MIN(n.name) AS n FROM name AS n WHERE n.gender IS NULL;"
                  "SELECT MIN(n.name) AS n FROM name AS n, cast_info AS ci, title AS t WHERE n.id=ci.person_id AND ci.movie_id=t.id AND n.gender IS NULL;"
                  "SELECT MIN(n.name) AS n FROM name AS n WHERE (n.gender IS NULL OR n.gender='f');"
                  "SELECT MIN(n.gender) AS g, MIN(n.name) AS n FROM name AS n WHERE n.id=n.id;"
                  "SELECT MIN(n.gender) AS g, MIN(n.name) AS n FROM name AS n WHERE n.gender IS NULL;"
                  "SELECT MIN(n.gender) AS g, MIN(n.name) AS n FROM name AS n WHERE n.name LIKE '%NotPresent%';"
                  "SELECT MIN(t.production_year) AS y, MIN(t.title) AS t FROM title AS t WHERE t.title LIKE '%Null%';"]]
        (doseq [[i statement] (map-indexed vector sqls)]
          (let [opts (suite/options! {:schema schema :timeout-ms 10000})
                result (suite/run-query! profile opts catalogs i {:id :1a :sql statement})]
            (is (= :ok (:status result)) (pr-str (dissoc result :sql :query)))
            (is (every? #(zero? (:live-sessions %)) (:source-statistics result)))
            (is (= :ok (get-in result [:tuple-verification :check :status])))))  ))))


(deftest benchmark-payload-limit-reaches-the-source-profile
  (with-job
    (fn [profile schema stmt]
      (.execute ^Statement stmt "UPDATE name SET name=repeat('x',20000) WHERE id=1")
      (let [profile (assoc profile :compute-context-id :test/job-limits :statement-timeout-ms 10000)
            catalogs {"name" (pg/discover profile [schema "name"])}
            entry {:id :1a :sql "SELECT MIN(n.name) AS n FROM name AS n WHERE n.name IS NOT NULL;"}
            run (fn [cap entry]
                  (suite/run-query! profile
                    (suite/options! {:schema schema :timeout-ms 10000 :max-row-bytes cap})
                    catalogs 0 entry))
            small (run 16384 entry)
            larger (run 32768 entry)
            large-result (run 16384 (assoc entry :sql "SELECT MIN(n.name) AS n FROM name AS n WHERE n.name LIKE 'xxxx%';"))]
        ;; A small SQL MIN result need not transfer the oversized input value.
        ;; The tuple oracle and an aggregate returning that value still fail.
        (is (= :ok (get-in small [:samples 0 :adapter :status])))
        (is (= :query-source/limit (get-in small [:tuple-verification :adapter :failure :error])))
        (is (= :query-source/limit (get-in large-result [:samples 0 :adapter :failure :error])))
        (is (= :ok (:status larger)))
        (is (= :ok (get-in larger [:tuple-verification :check :status])))
        (is (every? #(zero? (:live-sessions %)) (concat (:source-statistics small) (:source-statistics larger)
                                                      (:source-statistics large-result))))))))
