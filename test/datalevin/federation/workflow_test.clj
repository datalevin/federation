(ns datalevin.federation.workflow-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.file :as export]
            [datalevin.federation.github :as github]
            [datalevin.federation.install :as install]
            [datalevin.federation.release-triage :as triage]
            [datalevin.federation.workflow-fixture :as fixture]
            [datalevin.federation.workflow-observer :as observer])
  (:import [com.sun.net.httpserver HttpExchange]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn- category [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
(defn- with-fixture [options f]
  (let [fixture (fixture/create! options)]
    (try (f fixture) (finally ((:close! fixture))))))
(defn- execute [q fixture sources windows & [rules]]
  (apply d/q q @(:conn fixture) (:github sources) (:release sources)
         (concat (when rules [rules]) [42 9 windows])))
(defn- verify-clean [fixture observed]
  (is (fixture/clean? fixture))
  (is (zero? (:live-sessions (github/statistics (get-in fixture [:sources :github])))))
  (is (zero? (:live-spools (export/statistics (get-in fixture [:sources :release])))))
  (is (observer/clean? observed)))

(deftest complete-three-source-queries-match-controlled-eager-observations
  (with-fixture {:native-rows 5 :export-rows 5 :selected-keys 5}
    (fn [fixture]
      (let [eager (fixture/eager fixture)
            windows [["now" :a] ["now" :b] ["later" :c]]]
        (doseq [q [triage/query triage/summary-query triage/lookup-query]
                limits [{:batch-rows 1 :binding-rows 1} {:batch-rows 64 :binding-rows 64}]]
          (let [observed (observer/recorder) sources (observer/sources (:sources fixture) observed)
                actual (execute (assoc-in q [:query-source :limits] limits) fixture sources windows)
                expected (execute (dissoc q :query-source) fixture eager windows)]
            (is (= expected actual) (pr-str q))
            (is (some? actual))
            (verify-clean fixture observed)))
        (let [result (triage/run (:conn fixture) (:sources fixture) 42 9 windows)]
          (is (= 12 (count result)))
          (is (= #{1 2 5} (set (map first result))))
          (is (every? ref/entity-ref? (map #(nth % 4) result))))))))

(deftest file-absence-domain-keys-and-input-witnesses-retain-their-meaning
  (let [rows [{:issue [42 1] :owners [7 7 nil 8] :window "now"}
              {:issue [42 1] :owners [7] :window "now"}
              {:issue [42 2] :owners [] :window "now"}
              {:issue [42 3] :owners [7] :window "now"}
              {:issue [42 4] :owners [7] :window "now"}
              {:issue [42 5] :owners [7]}
              {:issue [99 1] :owners [7] :window "now"}]]
    (with-fixture {:native-rows 5 :selected-keys 5 :records rows}
      (fn [fixture]
        (let [windows [["now" :a] ["now" :b]] eager (fixture/eager fixture)
              result (execute triage/query fixture (:sources fixture) windows)
              count-result (execute triage/summary-query fixture (:sources fixture) windows)]
          (is (= result (execute (dissoc triage/query :query-source) fixture eager windows)))
          (is (= #{1} (set (map first result))))
          (is (= 4 (count result)))
          (is (= 12 count-result (execute (dissoc triage/summary-query :query-source) fixture eager windows)))
          (is (empty? (execute triage/query fixture (:sources fixture) [])))
          (is (empty? (execute triage/query fixture (:sources fixture) [["missing" :a]])))
          (is (fixture/clean? fixture)))))))

(deftest positive-mapped-rules-preserve-complete-result-set-semantics
  (with-fixture {:native-rows 2 :export-rows 2 :selected-keys 2}
    (fn [fixture]
      (let [windows [["now" :a] ["now" :b]] eager (fixture/eager fixture)
            observed (observer/recorder) sources (observer/sources (:sources fixture) observed)]
        (is (= (execute triage/query fixture eager windows)
               (execute (dissoc triage/rule-query :query-source) fixture eager windows triage/rules)
               (execute triage/rule-query fixture sources windows triage/rules)))
        (verify-clean fixture observed)))))

(deftest saved-cross-source-mappings-reopen-and-refresh-atomically
  (with-fixture {:native-rows 2 :export-rows 2 :selected-keys 2}
    (fn [fixture]
      (let [conn (:conn fixture) before (install/load-definitions conn)
            expected (execute triage/query fixture (:sources fixture) [["now" :a]])]
        (d/close conn)
        (let [reopened (d/create-conn (str (:root fixture) "/native"))
              fixture (assoc fixture :conn reopened)
              loaded (install/load-definitions reopened)
              profiles (:profiles fixture)
              sources (triage/sources loaded (:github profiles) (:release profiles))]
          (try
            (is (= (:compiled before) (:compiled loaded)))
            (is (= expected (execute triage/query fixture sources [["now" :a]])))
            (let [path (str (:root fixture) "/next.ednl")
                  rows [{:issue [42 2] :owners [8] :window "later"}]
                  accepted (fixture/write-export! path rows (random-uuid))
                  definitions (triage/definitions accepted)
                  result (install/install! reopened
                           {:release/assignments (:release/assignments definitions)}
                           {:expected-versions {:release/assignments 1}
                            :schema-update {:triage/comment {:db/valueType :db.type/string}}})
                  new-sources (triage/sources result (:github profiles) (assoc (:release profiles) :path path))
                  fresh (assoc fixture :accepted accepted :records rows)
                  windows [["later" :a]]
                  owner :datalevin.federation/definition]
              (is (= 2 (get-in result [:schema :release/assignments owner :version])))
              (is (= :db.type/string (get-in result [:schema :triage/comment :db/valueType])))
              (is (= (get-in before [:compiled :github/work-items :accepted :identity])
                     (get-in result [:compiled :github/work-items :accepted :identity])))
              (is (not= (get-in before [:compiled :release/assignments :definition-fingerprint])
                        (get-in result [:compiled :release/assignments :definition-fingerprint])))
              (is (= (execute (dissoc triage/query :query-source) fresh (fixture/eager fresh) windows)
                     (execute triage/query fresh new-sources windows)))
              (is (= expected (execute triage/query fixture sources [["now" :a]]))))
            (finally (d/close reopened))))))))

(deftest grouped-consistency-and-actual-work-stay-source-specific
  (with-fixture {:native-rows 8 :export-rows 8 :selected-keys 1}
    (fn [fixture]
      (let [sources (:sources fixture)
            explanation (d/explain {:run? true} triage/lookup-query @(:conn fixture)
                                  (:github sources) (:release sources) 42 9 [["now" :a]])
            modes (into {} (map (juxt :source :consistency)) (:sources explanation))
            versions (into {} (map (juxt :source :data-version)) (:sources explanation))
            metrics (group-by :source-symbol (:cursors explanation))]
        (is (= {'$github :best-effort '$release :snapshot} modes))
        (is (nil? (versions '$github)))
        (is (= (export/export-token (get-in fixture [:accepted :release :dependencies :export])) (versions '$release)))
        (is (= 1 (:result-rows explanation)))
        ;; Captured native thresholds narrow the keys before API lookups.
        (is (= #{1} (:numbers @(:requests (:api fixture)))))
        (is (= 2 (:issues @(:requests (:api fixture)))))
        (is (= 1 (get-in @(:requests (:api fixture)) [:repository])))
        (is (= (get @(:requests (:api fixture)) :issues)
               (reduce + (map #(get-in % [:metrics :external-requests]) (metrics '$github)))))
        (is (every? #(zero? (get-in % [:metrics :external-requests])) (metrics '$release)))
        (is (= 1 (:acquisitions (export/statistics (:release sources)))))
        (is (fixture/clean? fixture))))))

(deftest integrated-failures-never-return-a-prefix-and-release-every-view
  (with-fixture {:native-rows 2 :export-rows 2 :selected-keys 2}
    (fn [fixture]
      (doseq [failure [:request-quota :body :drift]]
        (testing (name failure)
          (let [observed (observer/recorder) sources (observer/sources (:sources fixture) observed)
                q (if (= failure :request-quota)
                    (assoc-in triage/query [:query-source :limits :external-requests] 2) triage/query)]
            (case failure
              :body (reset! (:behavior fixture)
                      (fn [^HttpExchange exchange default]
                        (if (.contains (.getPath (.getRequestURI exchange)) "/issues/")
                          {:status 200 :raw "{truncated"} {:status 200 :json default})))
              :drift (spit (get-in fixture [:profiles :release :path]) "{:corrupt true}\n")
              nil)
            (is (= (case failure :request-quota :query-source/limit :body :query-source/provider
                                  :drift :query-source/snapshot-lost)
                   (category #(execute q fixture sources [["now" :a]]))))
            (verify-clean fixture observed)))))))

(deftest query-timeout-closes-api-body-and-pinned-export-together
  (with-fixture {:native-rows 1 :export-rows 1 :selected-keys 1}
    (fn [fixture]
      (let [reading (CountDownLatch. 1) unblock (CountDownLatch. 1)
            observed (observer/recorder) sources (observer/sources (:sources fixture) observed)]
        (reset! (:behavior fixture)
                (fn [^HttpExchange exchange default]
                  (cond-> {:status 200 :json default}
                    (.contains (.getPath (.getRequestURI exchange)) "/issues/")
                    (assoc :before-body #(do (.countDown reading) (.await unblock 5 TimeUnit/SECONDS))))))
        (let [task (future (category #(execute (assoc triage/query :timeout 1000) fixture sources [["now" :a]])))]
          (try
            (is (.await reading 3 TimeUnit/SECONDS))
            (is (= :query/timeout (deref task 3000 ::stuck)))
            (verify-clean fixture observed)
            (finally (.countDown unblock) (future-cancel task))))))))

(deftest aliases-share-the-pinned-export-and-per-source-read-requirements
  (with-fixture {:native-rows 2 :export-rows 2 :selected-keys 1}
    (fn [fixture]
      (let [sources (:sources fixture)
            q (-> triage/lookup-query
                  (assoc :in '[$ $github $release $again ?repository ?minimum [[?window ?witness]]])
                  (update :where conj '[$again ?plan :release/window ?window])
                  (assoc-in [:query-source :sources]
                    {'$release {:consistency :snapshot} '$again {:consistency :snapshot}
                     '$github {:consistency :best-effort}}))
            before (:acquisitions (export/statistics (:release sources)))
            expected (execute (dissoc triage/lookup-query :query-source) fixture (fixture/eager fixture) [["now" :a]])
            actual (d/q q @(:conn fixture) (:github sources) (:release sources) (:release sources) 42 9 [["now" :a]])]
        (is (= expected actual))
        (is (= 1 (- (:acquisitions (export/statistics (:release sources))) before)))
        (is (fixture/clean? fixture))
        (let [before @(:requests (:api fixture))]
          (is (= :query-source/unsupported
                 (category #(execute (assoc-in triage/lookup-query [:query-source :consistency] :snapshot)
                                     fixture sources [["now" :a]]))))
          (is (= before @(:requests (:api fixture)))))))))

(deftest native-and-mapped-rule-boundaries-deduplicate-export-witnesses
  (with-fixture {:native-rows 1 :selected-keys 1
                 :records [{:issue [42 1] :owners [7 8] :window "now"}
                           {:issue [42 1] :owners [7] :window "now"}]}
    (fn [fixture]
      (let [rules (conj triage/rules
                    '[(eligible [?repository ?minimum] ?number ?priority)
                      [?t :triage/repository ?repository] [?t :triage/number ?number]
                      [?t :triage/priority ?priority] [(>= ?priority ?minimum)]])
            q (-> triage/rule-query
                  (assoc :find '[(count ?issue) .] :with '[?label ?owner ?witness])
                  (update :where #(into ['(eligible ?repository ?minimum ?number ?priority)] (drop 4 %))))
            observed (observer/recorder) sources (observer/sources (:sources fixture) observed)
            windows [["now" :a] ["now" :b]]]
        ;; The release-owner rule projects away export-row identity before
        ;; counting, so its duplicate owner-7 witness contributes one head.
        (is (= 8 (execute (dissoc q :query-source) fixture (fixture/eager fixture) windows rules)
               (execute q fixture sources windows rules)))
        (let [relation-query (-> q (assoc :find (:find triage/query)) (dissoc :with))]
          (is (= (execute (dissoc relation-query :query-source) fixture (fixture/eager fixture) windows rules)
                 (execute relation-query fixture sources windows rules))))
        (is (= 12 (execute triage/summary-query fixture (:sources fixture) windows)))
        (verify-clean fixture observed)))))

(deftest large-ordinary-input-is-pruned-by-an-explicit-window-predicate
  (with-fixture {:native-rows 16 :export-rows 16 :selected-keys 1}
    (fn [fixture]
      (let [windows (mapv (fn [n] [(if (zero? n) "now" (str "later-" n)) (str "witness-" n)]) (range 1024))
            q (-> triage/lookup-query
                  (update :where conj '[(= ?window "now")])
                  (assoc-in [:query-source :limits] {:retained-bytes 67108864 :batch-rows 1 :binding-rows 1}))
            observed (observer/recorder) sources (observer/sources (:sources fixture) observed)
            expected (execute (dissoc q :query-source) fixture (fixture/eager fixture) windows)
            result (execute q fixture sources windows)]
        (is (= expected result))
        (is (= 1 (count result)))
        (verify-clean fixture observed)))))
