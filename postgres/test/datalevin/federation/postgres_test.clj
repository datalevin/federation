(ns datalevin.federation.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.mapping :as mapping]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.captured :as captured]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.temporal :as temporal]
            [datalevin.federation.postgres.statistics :as statistics]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]))

(deftest exact-interval-fields-and-immutable-calendar-keys
  (doseq [[text expected] [["PT0S" {:months 0 :days 0 :microseconds 0}]
                           ["P-1Y2M-3DT4H-5M6.123456S" {:months -10 :days -3 :microseconds 14106123456}]
                           ["PT2562047788H54.775807S" {:months 0 :days 0 :microseconds Long/MAX_VALUE}]
                           ["PT-2562047788H-54.775808S" {:months 0 :days 0 :microseconds Long/MIN_VALUE}]]]
    (is (= expected (temporal/interval text))))
  (doseq [text ["P" "PT0.0000001S" "PT99999999999999999999H"]]
    (is (thrown? clojure.lang.ExceptionInfo (temporal/interval text))))
  (doseq [x [(java.time.LocalDate/parse "0001-01-01")
             (java.time.LocalDate/parse "-0001-12-31")
             (java.time.LocalTime/parse "23:59:59.999999999")
             (java.time.LocalDateTime/parse "1969-12-31T23:59:59.123456789")
             (java.time.OffsetTime/parse "12:34:56.123456789-05:45")]]
    (let [type (mapping/entity-type ref/type-contract [:calendar 0] [:data])
          entity (ref/entity-ref type [x])]
      (is (= x (value/owned-copy x)))
      (is (<= 64 (value/value-charge x)))
      (is (= [x] (ref/key-tuple (ref/decode (ref/encode entity))))))))

(defn- admission-pool []
  (reify javax.sql.DataSource
    (getConnection [_] (throw (AssertionError. "admission must not check out a connection")))
    (getConnection [_ _ _] (throw (AssertionError. "admission must not check out a connection")))
    Object
    (equals [_ other] (instance? javax.sql.DataSource other))
    (hashCode [_] 0)))

(defn- admission-context [pool options]
  {:profile (jdbc/profile! {:data-source pool :connect-timeout-ms (get options :connect-timeout-ms 1000)})
   :control (control/create (select-keys options [:timeout-ms :limits]))
   :cancelled (atom false)})

(defn- admission-result [context]
  (try
    (jdbc/close! (jdbc/admit-acquisition! context))
    :admitted
    (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(deftest pool-admission-is-shared-across-contexts-and-released-once
  (let [pool (admission-pool) a (admission-context pool {})
        b (assoc-in (admission-context pool {}) [:profile :role] "another-reader")
        owner (jdbc/admit-acquisition! a) started (java.util.concurrent.CountDownLatch. 1)
        admitted (java.util.concurrent.CountDownLatch. 1) finish (java.util.concurrent.CountDownLatch. 1)
        job (future
              (.countDown started)
              (let [lease (jdbc/admit-acquisition! b)]
                (try (.countDown admitted) (.await finish) :admitted
                     (finally (jdbc/close! lease)))))]
    (try
      (is (.await started 1 java.util.concurrent.TimeUnit/SECONDS))
      (is (= ::waiting (deref job 30 ::waiting)))
      (is (pos? (:retained-bytes (control/usage (:control a)))))
      (is (zero? (:external-requests (control/usage (:control b)))))
      (jdbc/close! owner)
      (is (.await admitted 1 java.util.concurrent.TimeUnit/SECONDS))
      (jdbc/close! owner)
      ;; B still owns admission: a duplicate close of A must not let C pass.
      (let [waiting (admission-context pool {:connect-timeout-ms 30})]
        (try (is (= :query/timeout (admission-result waiting)))
             (finally (control/close! (:control waiting)))))
      (.countDown finish)
      (is (= :admitted (deref job 2000 ::blocked)))
      (doseq [context [a b]] (is (every? zero? (vals (control/usage (:control context))))))
      (finally (.countDown finish) (jdbc/close! owner) (future-cancel job)
               (doseq [context [a b]] (control/close! (:control context)))))))

(deftest pool-admission-uses-identity-and-does-not-block-other-pools
  (let [a (admission-context (admission-pool) {}) b (admission-context (admission-pool) {})
        owner (jdbc/admit-acquisition! a)]
    (try
      (is (= (get-in a [:profile :data-source]) (get-in b [:profile :data-source])))
      (is (= :admitted (admission-result b)))
      (finally (jdbc/close! owner)
               (doseq [context [a b]] (control/close! (:control context)))))))

(deftest waiting-admission-honors-cancellation-and-interruption
  (doseq [interrupt? [false true]]
    (let [pool (admission-pool) a (admission-context pool {}) b (admission-context pool {})
          owner (jdbc/admit-acquisition! a) thread (atom nil)
          started (java.util.concurrent.CountDownLatch. 1)
          job (future
                (reset! thread (Thread/currentThread)) (.countDown started)
                (try (admission-result b) (finally (Thread/interrupted))))]
      (try
        (is (.await started 1 java.util.concurrent.TimeUnit/SECONDS))
        (is (= ::waiting (deref job 30 ::waiting)))
        (if interrupt? (.interrupt ^Thread @thread)
            (control/cancel! (:control b) {:error :query-source/cancelled :phase :acquisition}))
        (is (= :query-source/cancelled (deref job 2000 ::blocked)))
        (is (every? zero? (vals (control/usage (:control b)))))
        (jdbc/close! owner)
        (is (= :admitted (admission-result a)))
        (finally (jdbc/close! owner) (future-cancel job)
                 (doseq [context [a b]] (control/close! (:control context))))))))

(deftest admission-and-primary-checkout-share-a-bounded-wait-budget
  (doseq [options [{:connect-timeout-ms 30} {:timeout-ms 30} {:limits {:retained-bytes 0}}]]
    (let [pool (admission-pool) a (admission-context pool {}) b (admission-context pool options)
          owner (jdbc/admit-acquisition! a)]
      (try
        (is (= (if (:limits options) :query-source/limit :query/timeout) (admission-result b)))
        (is (every? zero? (vals (control/usage (:control b)))))
        (jdbc/close! owner)
        (is (= :admitted (admission-result a)))
        (finally (jdbc/close! owner)
                 (doseq [context [a b]] (control/close! (:control context)))))))
  (let [context (admission-context (admission-pool) {})
        expired (assoc context :checkout-deadline (dec (System/nanoTime)))]
    (try
      (is (= :query/timeout (admission-result expired)))
      (is (= :query/timeout (try (jdbc/connect! expired) :unexpected
                                (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
      (is (every? zero? (vals (control/usage (:control context)))))
      (finally (control/close! (:control context))))))

(def source-id #uuid "e8d23d75-321f-4d88-a293-077ed468b8b1")
(def catalog
  {:table ["public" "orders"] :oid 42 :kind "r" :row-security? false :database "example"
   :columns {"id" {:type "int8" :type-id 20 :modifier -1 :not-null? true :collation 0}
             "title" {:type "text" :type-id 25 :modifier -1 :not-null? false :collation 100}
             "amount" {:type "numeric" :type-id 1700 :modifier -1 :not-null? false :collation 0}}
   :primary-key [{:column "id" :deferred? false :validated? true}]})
(def declaration {:source-instance-id source-id :relation-id :sales/orders
                  :identity-attribute :order/id :attributes {:order/title "title" :order/amount "amount"}})
(def accepted (pg/accept-mapping catalog declaration))
(def profile {:compute-context-id :test/postgres :jdbc-url "jdbc:postgresql://localhost/example"})

(deftest accepted-mapping-is-portable-and-reopening-does-no-io
  (let [s (pg/source accepted profile) d (spi/-source-info s)]
    (is (= accepted (:accepted (pm/compiled! accepted))))
    (is (= #{:snapshot} (:read-capabilities d)))
    (is (= :one (get-in d [:logical-schema :attributes :order/title :cardinality])))
    (is (= accepted (read-string (pr-str accepted))))
    (is (not (.contains (pr-str d) "jdbc:postgresql")))
    (is (= {:external-requests 0 :live-sessions 0} (pg/statistics s)))
    (is (= d (spi/-source-info (pg/source accepted (assoc profile :fusion? false)))))
    (is (= (:definition-fingerprint d)
           (:definition-fingerprint (spi/-source-info (pg/source
              (pg/accept-mapping (assoc-in catalog [:columns "unused"] {:type "jsonb"}) declaration) profile)))))))

(deftest physical-key-and-conversion-proofs-are-required
  (doseq [bad [(assoc catalog :primary-key [])
               (assoc-in catalog [:primary-key 0 :deferred?] true)
               (assoc-in catalog [:primary-key 0 :validated?] false)
               (assoc-in catalog [:columns "id" :not-null?] false)
               (assoc-in catalog [:columns "id" :type] "numeric")
               (assoc catalog :kind "v") (assoc catalog :row-security? true)
               (assoc-in catalog [:columns "title" :type-id] 12345)
               (assoc-in catalog [:columns "title" :type] "bpchar")]]
    (is (thrown? clojure.lang.ExceptionInfo (pg/accept-mapping bad declaration))))
  (is (thrown? clojure.lang.ExceptionInfo
               (pg/accept-mapping catalog (assoc-in declaration [:attributes :order/unknown] "missing"))))
  (is (thrown? clojure.lang.ExceptionInfo
               (pg/source (assoc-in accepted [:attributes :order/title :cardinality] :many) profile)))
  (is (thrown? clojure.lang.ExceptionInfo
               (pm/verify! accepted (assoc-in catalog [:columns "title" :modifier] 8)))))

(defn request [clauses]
  {:clauses clauses :bindings {} :available-inputs [] :max-alternatives 32})
(def patterns
  [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:var ?title]]}
   {:id 1 :kind :pattern :terms '[[:var ?e] [:const :order/amount] [:ignore]]}])

(deftest postgres-cardinalities-distinguish-scans-lookups-and-unknowns
  (let [stats {:rows 1000.0 :observed-at-ms 1
               :columns {"id" {:distinct -1.0 :null-fraction 0.0}
                         "title" {:distinct 100.0 :null-fraction 0.2}
                         "amount" {:distinct -0.5 :null-fraction 0.1}}}
        candidate {:clauses patterns :bindings {} :requires []}
        count! #(statistics/estimate accepted % stats)]
    (is (= 800.0 (:rows (count! candidate))))
    (is (= 8.0 (:rows (count! (assoc-in candidate [:clauses 0 :terms 2] [:const "paid"])))))
    (let [dependent (assoc candidate :requires [{:variable '?title :type :string :nullable? false}])
          estimate (count! dependent)]
      (is (= 8.0 (:rows-per-binding estimate)))
      (is (not (contains? estimate :rows)))
      (is (nil? (:per-binding-ns estimate)))
      (is (= :statistics (get-in estimate [:provenance :kind]))))
    (is (= 1.0 (:rows-per-binding (count! (assoc candidate :requires [{:variable '?e}])))))
    (is (= 1.0 (:rows (count! (assoc candidate :bindings {'?e :an-entity})))))
    (doseq [rows [nil -1.0 Double/NaN Double/POSITIVE_INFINITY]]
      (is (= :unknown (:confidence (statistics/estimate accepted candidate (assoc stats :rows rows))))))
    (is (= 0.0 (:rows (statistics/estimate accepted candidate (assoc stats :rows 0.0)))))
    (is (nil? (:rows (statistics/estimate accepted candidate nil))))))

(deftest distinct-counts-handle-absolute-and-proportional-statistics
  (is (= 100.0 (statistics/distinct-count 1000.0 100.0)))
  (is (= 1000.0 (statistics/distinct-count 1000.0 -1.0)))
  (is (= 500.0 (statistics/distinct-count 1000.0 -0.5)))
  (doseq [n [nil 0 Double/NaN Double/POSITIVE_INFINITY]]
    (is (nil? (statistics/distinct-count 1000.0 n)))))

(deftest prepared-filter-counts-override-only-independent-cardinalities
  (let [candidate {:clauses patterns :bindings {} :requires []}
        catalog {:rows 1000.0 :observed-at-ms 1 :columns {}}
        filtered {:rows 2.0 :observed-at-ms 2}
        estimate (statistics/estimate accepted candidate catalog filtered)]
    (is (= 2.0 (:rows estimate)))
    (is (= "postgres-explain-filtered-cardinality-v1" (get-in estimate [:provenance :id])))
    (is (= 2 (get-in estimate [:provenance :observed-at-ms])))
    (is (= 0.0 (:rows (statistics/estimate accepted candidate catalog (assoc filtered :rows 0)))))
    (doseq [rows [nil -1 Double/NaN Double/POSITIVE_INFINITY]]
      (is (= 1000.0 (:rows (statistics/estimate accepted candidate catalog (assoc filtered :rows rows))))))
    (let [dependent (assoc candidate :requires [{:variable '?e}])]
      (is (= 1.0 (:rows-per-binding (statistics/estimate accepted dependent catalog filtered)))))
    (is (= 2.0 (:rows (statistics/estimate accepted candidate nil filtered))))))

(deftest joined-filters-price-selectivity-without-duplicating-bound-keys
  (let [stats {:rows 1000.0 :columns {"title" {:distinct 100.0}}}
        eq {:kind :predicate :operator := :args '[[:var ?title] [:const "paid"]]}
        member {:kind :predicate :operator :in :args '[[:var ?title] [:const ["a" "b" "b" nil]]]}
        c {:clauses patterns :bindings {} :requires [{:variable '?e}] :predicates [eq member]}]
    (is (= 0.01 (statistics/filter-selectivity accepted c stats)))
    (is (= 0.02 (statistics/filter-selectivity accepted (assoc c :predicates [member]) stats)))
    (is (= 1.0 (statistics/filter-selectivity accepted (assoc c :requires [{:variable '?title}]) stats)))
    (is (= 1.0 (statistics/filter-selectivity accepted c nil)))
    (is (= 1.0 (statistics/filter-selectivity accepted c (assoc-in stats [:columns "title" :distinct] 0))))))

(deftest acquired-reference-domains-refine-sampled-fanout-without-proving-completeness
  (let [target accepted
        domain (get-in target [:identity :type :parameters :identity-domain-id])
        child (pg/accept-mapping (assoc-in (assoc-in catalog [:columns "amount" :type] "int8")
                                          [:columns "amount" :type-id] 20)
                (assoc declaration :relation-id :sales/child
                       :attributes {:order/owner {:column "amount" :reference
                                                   {:domain domain :type (get-in target [:identity :type])}}}))
        stats {:rows 100000.0 :observed-at-ms 1 :columns {"amount" {:distinct 100.0}}}
        candidate {:clauses [{:terms '[[:var ?e] [:const :order/owner] [:var ?owner]]}]
                   :requires [{:variable '?owner}] :bindings {}}
        refined (statistics/reference-statistics child stats [{:accepted target :statistics (atom {:rows 20000.0})}])]
    (is (= 1000.0 (:rows-per-binding (statistics/estimate child candidate stats))))
    (is (= 5.0 (:rows-per-binding (statistics/estimate child candidate refined))))
    (is (= :low (:confidence (statistics/estimate child candidate refined))))
    (is (= stats (statistics/reference-statistics child stats [])))
    (is (= stats (statistics/reference-statistics child stats [{:accepted target :statistics (atom {:rows 0.0})}])))))

(defn captured-amount-candidate [term]
  (let [descriptor (spi/-source-info (pg/source accepted profile))
        clause {:id 0 :kind :pattern :terms [[:var '?e] [:const :order/amount] term]}]
    (assoc (first (filter #(= :value-lookup (:kind %))
                         (plan/choices descriptor
                           (assoc (request [clause]) :available-inputs [{:variable '?v :type :bigdec :nullable? false}]) true)))
           :bindings {})))

(deftest captured-indexes-preserve-decimal-join-and-constant-equality
  (let [rows [{"id" 1 "amount" 1.0M} {"id" 2 "amount" 1.00M} {"id" 3 "amount" nil}]
        capture {:snapshot (atom rows) :indexes (atom {})}
        ctl (control/create) context {:control ctl :cancelled (atom false) :metrics (atom {:read-rows 0})}]
    (try
      (doseq [[term bindings expected] [[[:var '?v] {'?v 1.0M} [1]]
                                       [[:var '?v] {'?v 1.00M} [2]]
                                       [[:const 1.000M] {} [1 2]]]]
        (let [candidate (captured-amount-candidate term)
              selected ((captured/row-source! context accepted capture candidate) bindings)]
          (is (= expected (mapv #(get % "id") selected)))
          (is (= (vec (keep #(plan/match-row accepted candidate bindings %) rows))
                 (vec (keep #(plan/match-row accepted candidate bindings %) selected))))))
      ;; Each equality mode is built once, even across different lookup values.
      (is (= 6 (:read-rows @(:metrics context))))
      (is (pos? (:retained-bytes (control/usage ctl))))
      (finally (captured/close! (:indexes capture))))
    (is (nil? @(:indexes capture)))
    (is (every? zero? (vals (control/usage ctl))))
    (control/close! ctl)))

(deftest incomplete-captured-indexes-release-their-budget-and-fall-back-once
  (let [rows (mapv #(hash-map "id" % "amount" (bigdec %)) (range 100))
        capture {:snapshot (atom rows) :indexes (atom {})}
        ctl (control/create {:limits {:retained-bytes (+ 1048576 10000)}})
        metrics (atom {:read-rows 0}) context {:control ctl :cancelled (atom false) :metrics metrics}
        candidate (captured-amount-candidate [:var '?v])]
    (try
      (is (= rows ((captured/row-source! context accepted capture candidate) {'?v 2M})))
      (is (< 0 (:read-rows @metrics) (count rows)))
      (is (zero? (:retained-bytes (control/usage ctl))))
      (let [before @metrics]
        (is (= rows ((captured/row-source! context accepted capture candidate) {'?v 3M})))
        (is (= before @metrics)))
      (finally (captured/close! (:indexes capture))))
    (is (every? zero? (vals (control/usage ctl))))
    (control/close! ctl)))

(deftest cancelled-captured-index-builds-do-not-publish-or-leak
  (let [rows (mapv #(hash-map "id" % "amount" (bigdec %)) (range 100))
        capture {:snapshot (atom rows) :indexes (atom {})}
        ctl (control/create) metrics (atom {:read-rows 0})
        context {:control ctl :cancelled (atom false) :metrics metrics}]
    (add-watch metrics :cancel (fn [_ _ _ after]
                                (when (= 3 (:read-rows after))
                                  (control/cancel! ctl {:error :query-source/cancelled :phase :execution}))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (captured/row-source! context accepted capture (captured-amount-candidate [:var '?v]))))
    (is (empty? @(:indexes capture)))
    (is (every? zero? (vals (control/usage ctl))))
    (captured/close! (:indexes capture))
    (control/close! ctl)))

(deftest fusion-keeps-existence-and-entity-boundaries
  (let [d (spi/-source-info (pg/source accepted profile))
        fused (first (plan/choices d (request patterns) true))
        sql (plan/sql accepted fused {} 4096)]
    (is (= 2 (count (:clauses fused))))
    (is (= #{'?e '?title} (set (map :variable (:columns fused)))))
    (is (.contains (:sql sql) "\"amount\" IS NOT NULL"))
    (is (.contains (:sql sql) "FROM ONLY \"public\".\"orders\""))
    (doseq [entity ['[:var ?other] [:ignore]]]
      (let [different (assoc-in patterns [1 :terms 0] entity)]
        (is (every? #(= 1 (count (:clauses %))) (plan/choices d (request different) true)))))
    (is (every? #(= 1 (count (:clauses %))) (plan/choices d (request patterns) false)))))

(deftest bounded-alternatives-keep-a-complete-independent-cover
  (doseq [width [11 40]]
    (let [attributes (into {} (map #(vector (keyword "row" (str "value" %)) "title")) (range width))
          accepted (pg/accept-mapping catalog (assoc declaration :attributes attributes))
          d (spi/-source-info (pg/source accepted profile))
          entity (ref/entity-ref (get-in accepted [:identity :type]) [1])
          clauses (conj (mapv (fn [i]
                               {:id i :kind :pattern
                                :terms [[:var '?e] [:const (keyword "row" (str "value" i))]
                                        [:var (symbol (str "?v" i))]]}) (range width))
                        {:id width :kind :pattern
                         :terms '[[:var ?other] [:const :row/value0] [:var ?v0]]})]
      (doseq [fusion? [false true]
              :let [minimum (if fusion? 2 (count clauses))]
              cap (distinct [minimum (max minimum 32)])
              bindings [{} {'?e entity}]
              available [[{:variable '?e :type (get-in accepted [:identity :type]) :nullable? false}]
                         (plan/columns d clauses)]]
        (testing (pr-str {:width width :fusion? fusion? :cap cap :bound? (seq bindings)
                          :inputs (count available)})
          (let [req (assoc (request clauses) :bindings bindings :available-inputs available :max-alternatives cap)
                choices (plan/choices d req fusion?)
                scans (filter #(= :attribute-enumeration (:kind %)) choices)
                scheduled (dependency/schedule
                            {:inputs [] :required-work (set (map :id clauses)) :required-vars #{}
                             :choices (mapv #(assoc % :covers (set (map :id (:clauses %)))
                                                    :after #{} :resources #{}) scans)
                             :check! (fn []) :reserve! (fn [_]) :max-states 4096})]
            (is (<= (count choices) cap))
            (is (= choices (plan/choices d req fusion?)))
            (is (= (count choices) (count (set (map :id choices)))))
            ;; Scans alone must form a disjoint, executable cover. Merely
            ;; retaining each clause in some dependent lookup is insufficient.
            (is (every? #(empty? (:requires %)) scans))
            (is (= :complete (:status scheduled)))
            (is (= (zipmap (map :id clauses) (repeat 1))
                   (frequencies (mapcat :covers (:choices scheduled)))))))))))

(deftest fused-entity-lookups-do-not-repeat-the-same-access
  (let [d (spi/-source-info (pg/source accepted profile))
        req (assoc (request patterns) :available-inputs
                   [{:variable '?e :type (get-in accepted [:identity :type]) :nullable? false}])
        lookups (filter #(and (= :entity-lookup (:kind %)) (= 2 (count (:clauses %))))
                        (plan/choices d req true))]
    (is (= 1 (count lookups)))
    (is (= '?e (get-in (first lookups) [:requires 0 :variable])))))

(deftest insufficient-alternative-budgets-fail-explicitly
  (let [d (spi/-source-info (pg/source accepted profile))]
    (doseq [fusion? [false true]
            clauses [patterns (assoc-in patterns [1 :terms 0] [:var '?other])
                     (mapv #(assoc-in % [:terms 0] [:ignore]) patterns)]
            :when (or (not fusion?) (not= clauses patterns))]
      (is (= {:error :query-source/limit :phase :planning}
             (try
               (plan/choices d (assoc (request clauses) :max-alternatives 1) fusion?)
               nil
               (catch clojure.lang.ExceptionInfo e (ex-data e))))))))

(deftest identifiers-and-values-cannot-become-sql
  (is (= "\"odd\"\"name\"" (jdbc/identifier "odd\"name")))
  (let [d (spi/-source-info (pg/source accepted profile))
        input "'; DROP TABLE orders; --"
        clauses (assoc-in patterns [0 :terms 2] [:const input])
        choice (first (plan/choices d (request clauses) true))
        q (plan/sql accepted choice {} 4096)]
    (is (= [input] (:params q)))
    (is (not (.contains (:sql q) input)))))

(deftest batched-keys-use-parameters-and-keep-constant-filters
  (let [d (spi/-source-info (pg/source accepted profile))
        clauses (conj patterns {:id 2 :kind :pattern :terms '[[:var ?e] [:const :order/id] [:const 1]]})
        req (assoc (request clauses) :available-inputs [{:variable '?title :type :string :nullable? false}])
        choice (first (plan/choices d req true))
        input "'; DROP TABLE orders; --"
        q (plan/batch-sql accepted choice [{:binding-id 91 :values [input]}
                                           {:binding-id 92 :values ["Alpha"]}] 4096)]
    (is (.contains (:sql q) "JOIN (VALUES (0, ?::text), (1, ?::text)) AS b(i, k0) ON r.\"title\" = b.k0"))
    (is (.contains (:sql q) "r.\"amount\" IS NOT NULL"))
    (is (.contains (:sql q) "r.\"id\" = ?"))
    (is (= [input "Alpha" 1] (:params q)))
    (is (not (.contains (:sql q) input)))))

(deftest batched-integral-keys-are-not-narrowed-to-the-physical-column-type
  (let [accepted (pg/accept-mapping (assoc-in catalog [:columns "amount"]
                                             {:type "int2" :type-id 21 :modifier -1 :not-null? false :collation 0})
                                    declaration)
        d (spi/-source-info (pg/source accepted profile))
        clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/amount] [:var ?amount]]}]
        req (assoc (request clauses) :available-inputs [{:variable '?amount :type :long :nullable? false}])
        q (plan/batch-sql accepted (first (plan/choices d req true))
                          [{:binding-id 1 :values [40000]} {:binding-id 2 :values [1]}] 4096)]
    (is (.contains (:sql q) "?::int8"))
    (is (not (.contains (:sql q) "?::int2")))
    (is (= [40000 1] (:params q)))))

(deftest comparison-prefilters-preserve-decimal-and-temporal-precision
  (is (= ["x > ?::numeric" [10.2500M]] (plan/comparison-filter "numeric" "x" :> 10.2500M)))
  (is (nil? (plan/comparison-filter "numeric" "x" :> 10.25)))
  (is (nil? (plan/comparison-filter "numeric" "x" :> (bigdec "1E+200000"))))
  (doseq [stamp ["2026-01-01T00:00:00.000000999Z" "1969-12-31T23:59:59.999999999Z"]
          :let [instant (java.time.Instant/parse stamp)
                floor (str (.truncatedTo instant java.time.temporal.ChronoUnit/MICROS))]]
    (doseq [[operator sql] [[:> ">"] [:>= ">"] [:< "<="] [:<= "<="]]]
      (is (= [(str "x " sql " ?::timestamptz") [floor]]
             (plan/comparison-filter "timestamptz" "x" operator instant))))
    (is (= ["FALSE" []] (plan/comparison-filter "timestamptz" "x" := instant)))
    (is (= ["TRUE" []] (plan/comparison-filter "timestamptz" "x" :not= instant))))
  (doseq [bound [java.time.Instant/MIN java.time.Instant/MAX (java.util.Date.)]]
    (is (nil? (plan/comparison-filter "timestamptz" "x" :>= bound)))))


(deftest like-prefilters-use-bound-parameters-and-native-escape-rules
  (doseq [type ["text" "varchar"] operator [:like :not-like]
          [pattern parameter] [["%Downey%Robert%" "%Downey%Robert%"]
                               ["" ""] ["!" ""] ["abc!" "abc"]
                               ["!!" "!!"] ["abc!!!" "abc!!"]
                               ["!%!_" "!%!_"] ["a\\b%" "a\\b%"]
                               ["%'; DROP TABLE x; --%" "%'; DROP TABLE x; --%"]]]
    (let [[sql params] (plan/like-filter type "r.\"title\"" operator pattern)]
      (is (= [parameter] params))
      (is (= (str "r.\"title\" COLLATE pg_catalog.\"C\" " (if (= :like operator) "LIKE" "NOT LIKE")
                  " ?::text ESCAPE '!'") sql))))
  (doseq [pattern [nil 42 "!x" "%!x" "a!猫" "猫%" "\u0000" (str (char 0xD800))]]
    (is (nil? (plan/like-filter "text" "r.x" :like pattern))))
  (is (nil? (plan/like-filter "int8" "r.x" :like "%")))
  (is (nil? (plan/like-filter "text" "r.x" :ilike "%"))))

(deftest like-prefilters-keep-argument-direction-and-unsupported-calls-local
  (let [descriptor (spi/-source-info (pg/source accepted profile))
        candidate (assoc (first (plan/choices descriptor (request patterns) true)) :bindings {})
        predicate {:id 2 :kind :predicate :operator :like :semantics-version 1
                   :args '[[:var ?title] [:const "%Alpha%"]]}
        predicates (plan/predicates accepted candidate (request (conj patterns predicate)))
        query (plan/sql accepted (assoc candidate :predicates predicates) {} 4096)]
    (is (= [2] (mapv :id predicates)))
    (is (.contains (:sql query) "COLLATE pg_catalog.\"C\" LIKE ?::text ESCAPE '!'"))
    (is (= ["%Alpha%"] (:params query)))
    (doseq [args ['[[:const "Alpha"] [:var ?title]]
                  '[[:var ?title] [:var ?pattern]]
                  '[[:var ?title] [:const "%!x"]]]]
      (is (empty? (plan/predicates accepted candidate
                    (request (conj patterns (assoc predicate :args args)))))))))

(deftest explain-projections-keep-the-filter-and-never-run-or-fetch-values
  (let [d (spi/-source-info (pg/source accepted profile))
        candidate (assoc (first (plan/choices d (request patterns) true)) :bindings {})
        predicate {:id 2 :kind :predicate :operator :like :semantics-version 1
                   :args '[[:var ?title] [:const "%Alpha%"]]}
        filters (plan/predicates accepted candidate (request (conj patterns predicate)))
        query (plan/estimate-sql accepted (assoc candidate :predicates filters))]
    (is (.startsWith (:sql query) "SELECT 1 FROM ONLY \"public\".\"orders\" AS r WHERE "))
    (is (.contains (:sql query) "COLLATE pg_catalog.\"C\" LIKE ?::text ESCAPE '!'"))
    (is (= ["%Alpha%"] (:params query)))
    (is (nil? (plan/estimate-sql accepted candidate)))
    (is (nil? (plan/estimate-sql accepted (assoc candidate :predicates filters :requires [{:variable '?e}]))))))

(deftest unbindable-offsets-remain-local
  (doseq [offset ["+16:00" "-16:00" "+18:00" "-18:00"]]
    (is (nil? (plan/comparison-filter "timetz" "x" := (java.time.OffsetTime/parse (str "12:00" offset)))))))

(deftest existence-projections-keep-presence-and-omit-payload-size
  (let [d (spi/-source-info (pg/source accepted profile))
        clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:ignore]]}]
        choice (first (plan/choices d (request clauses) true))
        sql (plan/sql accepted choice {} 128)]
    (is (= #{"title"} (:presence-columns sql)))
    (is (.contains (:sql sql) "r.\"title\" IS NOT NULL"))
    (is (not (.contains (:sql sql) "octet_length(r.\"title\"")))
    (is (< (plan/fetch-charge accepted choice 128 16384) 1048576))
    (let [value-clause {:id 1 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:var ?title]]}
          choice (first (plan/choices d (request (conj clauses value-clause)) true))
          sql (plan/sql accepted choice {} 128)]
      (is (empty? (:presence-columns sql)))
      (is (.contains (:sql sql) "octet_length(r.\"title\""))))
  (let [reference (get-in accepted [:identity :type])
        accepted (pg/accept-mapping catalog
                   (assoc declaration :attributes {:order/parent {:column "id" :reference
                                                                 {:domain (get-in reference [:parameters :identity-domain-id])
                                                                  :type reference}}}))
        d (spi/-source-info (pg/source accepted profile))
        clauses [{:id 0 :kind :pattern :terms '[[:ignore] [:const :order/parent] [:ignore]]}]
        choice (first (plan/choices d (request clauses) true))]
    (is (= [] (plan/match-row accepted choice {} {"id" 1})))))

(deftest fused-repeated-variables-use-join-key-equality
  (let [catalog (assoc-in catalog [:columns "other"] (get-in catalog [:columns "amount"]))
        accepted (pg/accept-mapping catalog
                   (assoc declaration :attributes {:order/a "amount" :order/b "other"}))
        descriptor (spi/-source-info (pg/source accepted profile))
        entity (ref/entity-ref (get-in accepted [:identity :type]) [1])]
    (doseq [attributes [[:order/a :order/b] [:order/b :order/a]]
            :let [clauses (mapv (fn [id a] {:id id :kind :pattern :terms [[:var '?e] [:const a] [:var '?v]]})
                               (range) attributes)
                  candidate (first (plan/choices descriptor (request clauses) true))]]
      (is (nil? (plan/match-row accepted candidate {} {"id" 1 "amount" 1.0M "other" 1.00M})))
      (is (nil? (plan/match-row accepted candidate {} {"id" 1 "amount" 1.00M "other" 1.0M})))
      (doseq [x [1.0M 1.00M]
              bindings [{} {'?e entity} {'?v x}]]
        (is (= [entity x] (plan/match-row accepted candidate bindings {"id" 1 "amount" x "other" x}))))
      (is (nil? (plan/match-row accepted candidate {'?v 1.0M} {"id" 1 "amount" 1.00M "other" 1.00M}))))
    (let [clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/a] [:const 1.0M]]}
                   {:id 1 :kind :pattern :terms '[[:var ?e] [:const :order/b] [:const 1.0M]]}]
          candidate (first (plan/choices descriptor (request clauses) true))]
      (is (= [entity] (plan/match-row accepted candidate {} {"id" 1 "amount" 1.0M "other" 1.00M}))))))
