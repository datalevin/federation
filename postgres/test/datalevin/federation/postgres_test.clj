(ns datalevin.federation.postgres-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.federation.postgres.mapping :as pm]
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

(deftest identifiers-and-values-cannot-become-sql
  (is (= "\"odd\"\"name\"" (jdbc/identifier "odd\"name")))
  (let [d (spi/-source-info (pg/source accepted profile))
        input "'; DROP TABLE orders; --"
        clauses (assoc-in patterns [0 :terms 2] [:const input])
        choice (first (plan/choices d (request clauses) true))
        q (plan/sql accepted choice {} 4096)]
    (is (= [input] (:params q)))
    (is (not (.contains (:sql q) input)))))
