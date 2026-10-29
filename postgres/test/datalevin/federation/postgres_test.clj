(ns datalevin.federation.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.source :as spi]))

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
