(ns datalevin.federation.postgres-job-integration-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres-job :as job]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control])
  (:import [java.sql Connection DriverManager]
           [java.util UUID]))

(defn with-job [f]
  (let [url (or (System/getenv "POSTGRES_TEST_URL")
                (throw (ex-info "Set POSTGRES_TEST_URL to a disposable database" {})))
        schema (str "job_" (.replace (str (UUID/randomUUID)) "-" ""))]
    (with-open [conn (DriverManager/getConnection url)
                stmt (.createStatement ^Connection conn)]
      (.execute stmt (str "CREATE SCHEMA " (jdbc/identifier schema)))
      (try
        (.execute stmt (str "SET search_path TO " (jdbc/identifier schema)))
        (doseq [sql ["CREATE TABLE keyword (id integer PRIMARY KEY, keyword text)"
                     "CREATE TABLE name (id integer PRIMARY KEY, name text)"
                     "CREATE TABLE title (id integer PRIMARY KEY, title text, production_year integer)"
                     "CREATE TABLE cast_info (id integer PRIMARY KEY, movie_id integer, person_id integer)"
                     "CREATE TABLE movie_keyword (id integer PRIMARY KEY, movie_id integer, keyword_id integer)"
                     "INSERT INTO keyword VALUES (1,'marvel-cinematic-universe'),(2,'other')"
                     "INSERT INTO name VALUES (1,'Downey, Robert Jr.'),(2,'Downey, Robert Sr.'),(3,'Robert Downey'),(4,NULL)"
                     "INSERT INTO title VALUES (1,'Iron Man 3',2013),(2,'The Avengers',2012),(3,'Wrong year',2010),(4,'Wrong keyword',2018),(5,'Wrong actor',2018),(6,'Null year',NULL),(7,'Missing actor',2018)"
                     "INSERT INTO movie_keyword VALUES (1,1,1),(2,2,1),(3,3,1),(4,4,2),(5,5,1),(6,6,1),(7,7,1),(8,1,1)"
                     "INSERT INTO cast_info VALUES (1,1,1),(2,2,2),(3,2,1),(4,1,1),(5,3,1),(6,4,1),(7,5,3),(8,6,1),(9,7,999),(10,1,4)"]]
          (.execute stmt sql))
        (f {:jdbc-url url} schema stmt)
        (finally (.execute stmt (str "DROP SCHEMA " (jdbc/identifier schema) " CASCADE")))))))

(defn guarded-join-provider [provider observations]
  (reify spi/IQuerySource
    (-source-info [_] (spi/-source-info provider))
    (-open-session [_ request] (spi/-open-session provider request))
    (-plan-source [_ session request] (spi/-plan-source provider session request))
    spi/IQuerySourceJoinPlanning
    (-plan-joins [_ session request]
      (let [before (:external-requests (control/usage (:control request)))
            result (spi/-plan-joins provider session request)]
        (swap! observations conj [before (:external-requests (control/usage (:control request)))])
        result))))

(deftest real-adapter-and-original-sql-agree-before-and-after-aggregation
  (with-job
    (fn [profile schema _]
      (let [result (job/run! profile {:schema schema :warmups 0 :samples 1})]
        (is (= :ok (:status result)) (pr-str (:failure result)))
        (is (= #{["marvel-cinematic-universe" "Downey, Robert Jr." "Iron Man 3"]} (:expected result)))
        (is (= 3 (get-in result [:verification :matched-distinct-rows])))
        (is (= 1 (count (:samples result))))
        (is (false? (get-in result [:comparison :win?])))
        (is (string? (:native-plan result)))
        (is (not (.contains (pr-str result) (:jdbc-url profile))))))))

(deftest followup-queries-check-existence-disjunctions-and-all-original-outputs
  (with-job
    (fn [profile schema stmt]
      (doseq [sql ["CREATE TABLE company_name (id integer PRIMARY KEY)"
                   "CREATE TABLE movie_companies (id integer PRIMARY KEY, movie_id integer, company_id integer)"
                   "CREATE TABLE kind_type (id integer PRIMARY KEY, kind text)"
                   "CREATE TABLE char_name (id integer PRIMARY KEY, name text)"
                   "CREATE TABLE comp_cast_type (id integer PRIMARY KEY, kind text)"
                   "CREATE TABLE complete_cast (id integer PRIMARY KEY, movie_id integer, subject_id integer, status_id integer)"
                   "ALTER TABLE title ADD kind_id integer DEFAULT 1"
                   "ALTER TABLE cast_info ADD person_role_id integer DEFAULT 1"
                   "INSERT INTO company_name VALUES (1)"
                   "INSERT INTO movie_companies VALUES (1,1,1),(2,2,1),(3,4,999),(4,999,1),(5,1,1)"
                   "INSERT INTO kind_type VALUES (1,'movie'),(2,'tv series')"
                   "INSERT INTO char_name VALUES (1,'Iron Man'),(2,'Tony Stark'),(3,'Sherlock Tony Stark'),(4,'Unrelated')"
                   "INSERT INTO comp_cast_type VALUES (1,'cast'),(2,'complete'),(3,'announced')"
                   "INSERT INTO complete_cast VALUES (1,1,1,2),(2,2,1,2)"
                   "UPDATE title SET production_year=2016 WHERE id=1"
                   "UPDATE keyword SET keyword='superhero' WHERE id=2"
                   "INSERT INTO keyword VALUES (3,'character-name-in-title')"
                   "INSERT INTO name VALUES (5,'Xavier, Actor'),(6,'Bert, Actor'),(7,'X AAA Missing'),(8,'Bert AAA Missing')"
                   "INSERT INTO cast_info (id,movie_id,person_id) VALUES (11,1,5),(12,2,6),(13,4,7),(14,999,7),(15,4,8)"
                   "INSERT INTO movie_keyword VALUES (9,1,3),(10,2,3),(11,4,3),(12,999,3),(13,1,2),(14,2,2)"
                   "INSERT INTO title VALUES (8,'AAA Sherlock',2018,1),(9,'AAA Unrelated',2018,1),(10,'AAA Announced',2018,1),(11,'AAA Wrong Kind',2018,2)"
                   "INSERT INTO cast_info VALUES (16,8,1,3),(17,9,1,4),(18,10,1,1),(19,11,1,1)"
                   "INSERT INTO movie_keyword VALUES (15,8,2),(16,9,2),(17,10,2),(18,11,2)"
                   "INSERT INTO complete_cast VALUES (3,8,1,2),(4,9,1,2),(5,10,1,3),(6,11,1,2)"]]
        (.execute ^java.sql.Statement stmt sql))
      (doseq [[variant expected] [[:6b #{["superhero" "Downey, Robert Jr." "AAA Announced"]}]
                                  [:17c #{["Xavier, Actor" "Xavier, Actor"]}]
                                  [:17d #{["Bert, Actor"]}]
                                  [:20b #{["Iron Man 3"]}]]]
        (let [result (job/run! profile {:query variant :schema schema :warmups 0 :samples 1 :timeout-ms 10000})]
          (is (= :ok (:status result)) (pr-str [variant (:failure result)]))
          (is (= expected (:expected result)) (name variant))
          (is (pos? (get-in result [:verification :matched-distinct-rows] 0)))
          (is (every? #(zero? (:live-sessions %)) (:source-statistics result)))))
      (let [profile (assoc profile :compute-context-id :test/existence-joins)
            sources (job/sources! profile schema :17c)
            control (mapv #(pg/source (:accepted %) (assoc profile :relational? false)) sources)
            base job/query-17c]
        (doseq [q [(assoc base :find '[?name])
                   (assoc base :find '[(count ?name)] :with '[?mc])
                   (assoc base :find '[(count ?name)] :with '[?ci ?mk ?mc])
                   (assoc base :find '[?name ?mc ?cn ?ci ?mk])]]
          (is (= (set (apply d/q q control)) (set (apply d/q q sources))) (pr-str (:find q))))
        (is (every? #(zero? (:live-sessions (pg/statistics %))) (concat sources control)))))))

(deftest empty-aggregate-input-cannot-qualify-as-a-win
  (with-job
    (fn [profile schema stmt]
      (.execute ^java.sql.Statement stmt "DELETE FROM keyword")
      (let [result (job/run! profile {:schema schema :warmups 0 :samples 1})]
        (is (= :ok (:status result)) (pr-str (:failure result)))
        (is (= #{[nil nil nil]} (:expected result)))
        (is (zero? (get-in result [:verification :matched-distinct-rows])))
        (is (false? (get-in result [:comparison :win?])))))))

(deftest job-6d-validates-the-original-three-output-columns
  (with-job
    (fn [profile schema stmt]
      (.execute ^java.sql.Statement stmt "UPDATE keyword SET keyword='superhero' WHERE id=1")
      (doseq [table ["keyword" "name" "title" "cast_info" "movie_keyword"]]
        (.execute ^java.sql.Statement stmt (str "ANALYZE " (jdbc/relation [schema table]))))
      (let [result (job/run! profile {:query :6d :schema schema :warmups 0 :samples 1})]
        (is (= :ok (:status result)) (pr-str (:failure result)))
        (is (= #{["superhero" "Downey, Robert Jr." "Iron Man 3"]} (:expected result)))
        (is (= 4 (get-in result [:verification :matched-distinct-rows])))
        (let [plan (edn/read-string (:adapter-plan result))]
          ;; Remote estimation is too expensive for this fixture. Complete-path
          ;; costing may still choose either feasible driver using catalog data.
          (is (#{'$k '$n} (get-in plan [:regions 0 :steps 0 :source])))
          (is (zero? (get-in plan [:regions 0 :estimation :requests]))))))))

(deftest dependent-joins-preserve-witnesses-membership-and-original-minima
  (with-job
    (fn [profile schema stmt]
      (.execute ^java.sql.Statement stmt "UPDATE keyword SET keyword='superhero' WHERE id=1")
      (doseq [table ["keyword" "name" "title" "cast_info" "movie_keyword"]]
        (.execute ^java.sql.Statement stmt (str "ANALYZE " (jdbc/relation [schema table]))))
      (let [profile (assoc profile :compute-context-id :test/joined-job :statement-timeout-ms 10000)
            joined (job/sources! profile schema)
            control (mapv #(pg/source (:accepted %) (assoc profile :relational? false)) joined)
            base (-> job/query-6d (assoc :timeout 10000)
                     (update :where conj '[$n ?n :name/id 1]))
            unbound (assoc job/query-6d :timeout 10000)
            cases [base unbound
                   (assoc base :find '[?keyword ?name ?title])
                   (assoc base :find '[(count ?keyword)])
                   (assoc base :find '[(count ?keyword)] :with '[?ci ?mk])
                   (assoc base :find '[?title (count ?ci)] :with '[?mk])
                   (assoc base :find '[?ci ?mk ?keyword ?title])
                   (assoc unbound :find '[?keyword ?name ?title])
                   (assoc unbound :find '[?name (count ?ci)] :with '[?mk])]
            explained (apply d/explain {:run? true} base joined)]
        (is (some #(= :join (get-in % [:access :kind])) (get-in explained [:regions 0 :steps])))
        (doseq [q cases]
          (is (= (set (apply d/q q control)) (set (apply d/q q joined))) (pr-str (:find q))))
        (doseq [call '[(in ?keyword ["superhero" nil]) (not-in ?keyword ["other" nil])
                      (in ?keyword []) (not-in ?keyword [])]]
          (let [q (assoc-in base [:where 1] [call])]
            (is (= (set (apply d/q q control)) (set (apply d/q q joined))) (pr-str call))))
        (let [q (-> base (assoc :in '[$k $n $t $ci $mk ?allowed])
                    (assoc-in [:where 1] '[(in ?keyword ?allowed)]))]
          (is (= (set (apply d/q q (conj control ["superhero" nil])))
                 (set (apply d/q q (conj joined ["superhero" nil]))))))
        (let [q (-> base (assoc :in '[$k $n $t $ci $mk ?seed])
                    (update :where into '[[(+ ?seed 0) ?movie-id] [$t ?t :title/id ?movie-id]]))]
          (is (= (set (apply d/q q (conj control 1)))
                 (set (apply d/q q (conj joined 1))))))
        (let [observations (atom [])
              plan (apply d/explain {:run? false} base (mapv #(guarded-join-provider % observations) joined))]
          (is (seq @observations))
          (is (every? (fn [[before after]] (= before after)) @observations))
          (is (empty? (:cursors plan)))
          (is (some #(= :join (get-in % [:access :kind])) (get-in plan [:regions 0 :steps]))))
        (is (not (.contains (pr-str explained) "WITH matched")))
        (is (every? #(zero? (:live-sessions (pg/statistics %))) (concat joined control)))))))

(deftest query-budget-failures-remain-visible-in-the-report
  (with-job
    (fn [profile schema _]
      (let [result (job/run! profile {:schema schema :warmups 0 :samples 1 :limits {:external-requests 1}})]
        (is (= :failed (:status result)))
        (is (= :query-source/limit (get-in result [:failure :error])))
        (is (empty? (:samples result)))
        (is (not (.contains (pr-str result) (:jdbc-url profile))))))))

(deftest different-text-minima-fail-even-when-all-matched-tuples-agree
  (with-job
    (fn [profile schema stmt]
      ;; C orders UTF-8 bytes; Java orders UTF-16 code units. Both actor names
      ;; match LIKE, but their supplementary/BMP suffixes reverse the minimum.
      (doseq [sql ["ALTER TABLE name ALTER COLUMN name TYPE text COLLATE \"C\""
                   "DELETE FROM cast_info" "DELETE FROM name"
                   (str "INSERT INTO name VALUES (1,'Downey, Robert \uE000'),(2,'Downey, Robert "
                        (String. (Character/toChars 0x10000)) "')")
                   "INSERT INTO cast_info VALUES (1,1,1),(2,1,2)"]]
        (.execute ^java.sql.Statement stmt sql))
      (let [result (job/run! profile {:schema schema :warmups 0 :samples 1 :native-extrema? false})]
        (is (= :failed (:status result)))
        (is (= :first-aggregate-query (get-in result [:failure :stage])))
        (is (= :result-mismatch (get-in result [:failure :error])))
        (is (= 2 (get-in result [:verification :matched-distinct-rows])))
        (is (every? #(zero? (:live-sessions %)) (:source-statistics result)))
        (is (empty? (:samples result))))
      (let [result (job/run! profile {:schema schema :warmups 0 :samples 1})]
        (is (= :ok (:status result)) (pr-str (:failure result)))))))
