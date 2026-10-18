(ns datalevin.federation.mapping-v2-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.federation.contract-test :as old]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.mapping :as mapping])
  (:import [datalevin.utl QueryKey$KeySet]))

(def order-domain [old/source-id :order 0])
(def customer-domain [old/source-id :customer 0])
(defn entity-type [domain key-types] (mapping/entity-type ref/type-contract domain key-types))

(def accepted
  {:representation-version 2 :relation-id :sales/orders :source-instance-id old/source-id
   :identity {:type (entity-type order-domain [:long]) :attribute :order/id
              :extraction {:shape :key-tuple :expressions [[:field "id"]]} :scope :durable}
   :attributes
   {:order/amount {:extraction {:shape :value :expression [:field "amount"]}
                   :type :bigdec :cardinality :one :null :absent :conversion {:version 1}}
    :order/tag {:extraction {:shape :elements :expression [:field "tags"]}
                :type :string :cardinality :many :null :absent :conversion {:version 1}}
    :order/customer {:extraction {:shape :value :expression [:field "customer-id"]}
                     :reference {:domain customer-domain :type (entity-type customer-domain [:long])}
                     :cardinality :one :null :absent :conversion {:version 1}}
    :order/assignee {:extraction {:shape :elements :expression [:path "assignees" "id"]}
                     :reference {:domain customer-domain :type (entity-type customer-domain [:long])}
                     :cardinality :many :null :absent :conversion {:version 1}}}
   :scope {:id "scope:tenant-a" :kind :restricted :parameters {:tenant "a"}}
   :completion {:domain :finite :extent :all-matches}
   :requirements {:fields ["id" "amount" "tags" "customer-id" "assignees"]}
   :dependencies {:provider {:name :api :version 1} :conversion-version 1}})

(deftest complete-logical-schema-without-provider-execution
  (let [compiled (mapping/compile-logical-mapping accepted)
        schema (:logical-schema compiled)]
    (is (= #{:domains :attributes} (set (keys schema))))
    (is (= {order-domain {:entity-type (entity-type order-domain [:long])}
            customer-domain {:entity-type (entity-type customer-domain [:long])}}
           (:domains schema)))
    (is (= (set (keys (:attributes schema)))
           (set (keys (get-in compiled [:pattern-contract :attributes])))))
    (is (= {:entity-domain order-domain :cardinality :one :absence :no-fact :reference-domain nil}
           (get-in schema [:attributes :order/id])))
    (is (= {:entity-domain order-domain :cardinality :many :absence :no-fact
            :reference-domain customer-domain}
           (get-in schema [:attributes :order/assignee])))
    (is (= :federation/v2 (:fingerprint-scheme compiled)))
    (is (.startsWith (:definition-fingerprint compiled) "federation-definition:v2:sha256:"))
    (is (not (contains? compiled :source-info)))
    (is (not (contains? compiled :spi-versions)))))

(defn facts-set [facts] (doto (QueryKey$KeySet.) (.addAll facts)))

(deftest no-fact-absence-many-values-and-references
  (let [compiled (mapping/compile-logical-mapping accepted)
        e (ref/entity-ref (entity-type order-domain [:long]) [7])
        customer (ref/entity-ref (entity-type customer-domain [:long]) [10])
        facts (mapping/project-facts compiled [7]
                                     {:order/amount nil :order/tag ["red" "red" nil "blue"]
                                      :order/customer 10 :order/assignee [10 10 nil 11]})]
    (is (= 6 (count facts)))
    (is (.equals (facts-set [[e :order/id 7] [e :order/tag "red"] [e :order/tag "blue"]
                            [e :order/customer customer] [e :order/assignee customer]
                            [e :order/assignee (ref/entity-ref (entity-type customer-domain [:long]) [11])]])
                 (facts-set facts)))
    (doseq [values [{} {:order/tag nil} {:order/tag []} {:order/assignee []}]]
      (is (= [[e :order/id 7]] (mapping/project-facts compiled [7] values))))
    (is (= 1 (count (mapping/project-facts compiled [8] {:order/customer nil}))))
    (doseq [values [{:order/tag "red"} {:order/tag 1} {:order/tag [1]}
                     {:order/amount 1.0} {:order/customer "10"}
                     {:order/assignee [10 "11"]} {:unknown/value 10}]]
      (is (thrown? clojure.lang.ExceptionInfo (mapping/project-facts compiled [7] values))))))

(deftest scalar-collections-and-composite-reference-elements
  (let [type {:kind :tuple :elements [:long :long]}
        accepted (-> accepted
                     (assoc-in [:attributes :order/pair]
                               {:extraction {:shape :value :expression [:field "pair"]}
                                :type type :cardinality :one :null :absent :conversion {:version 1}})
                     (assoc-in [:attributes :order/assignee :reference :type]
                               (entity-type customer-domain [:string :long]))
                     (update :attributes dissoc :order/customer))
        compiled (mapping/compile-logical-mapping accepted)
        facts (mapping/project-facts compiled [7] {:order/pair [1 2]
                                                   :order/assignee [["tenant" 1] ["tenant" 1] ["tenant" 2]]})]
    (is (= 4 (count facts)))
    (is (= #{[1 2]} (set (map #(nth % 2) (filter #(= :order/pair (second %)) facts)))))
    (doseq [bad [[["tenant" nil]] [[nil 1]] ["tenant" 1]]]
      (is (thrown? clojure.lang.ExceptionInfo
                   (mapping/project-facts compiled [7] {:order/assignee bad}))))))

(deftest guarantees-apply-to-the-combined-relation
  (let [single (mapping/compile-logical-mapping accepted)
        other (mapping/compile-logical-mapping (-> accepted
                                                   (assoc :relation-id :sales/archived-orders)
                                                   (assoc-in [:attributes :order/amount :extraction :expression]
                                                             [:field "archived-amount"])))
        combined (mapping/combine-logical-mappings [single other])]
    (is (= :unknown (get-in combined [:logical-schema :attributes :order/amount :cardinality])))
    (is (= :one (get-in combined [:logical-schema :attributes :order/id :cardinality])))
    (is (= :many (get-in combined [:logical-schema :attributes :order/tag :cardinality])))
    (is (= :one (get-in (mapping/combine-logical-mappings [single single])
                         [:logical-schema :attributes :order/amount :cardinality])))
    (is (= (:definition-fingerprint combined)
           (:definition-fingerprint (mapping/combine-logical-mappings [other single]))))
    (doseq [change [(assoc-in accepted [:scope :parameters :tenant] "b")
                    (assoc-in accepted [:identity :type :version] 2)
                    (assoc-in accepted [:attributes :order/amount :type] :long)]]
      (let [other (mapping/compile-logical-mapping (assoc change :relation-id :sales/other))]
        (is (thrown? clojure.lang.ExceptionInfo (mapping/combine-logical-mappings [single other])))))))

(deftest accepted-fingerprints-cover-complete-guarantees-and-scope
  (let [compile mapping/compile-logical-mapping
        original (compile accepted)]
    (doseq [mapping [(assoc-in accepted [:attributes :order/amount :cardinality] :many)
                     (assoc-in accepted [:attributes :order/amount :extraction :expression] [:field "new"])
                     (assoc-in accepted [:scope :parameters :tenant] "b")
                     (assoc-in accepted [:scope :id] "scope:tenant-b")
                     (assoc-in accepted [:identity :scope] :export)
                     (assoc-in accepted [:dependencies :conversion-version] 2)
                     (assoc-in accepted [:attributes :order/tag :conversion :version] 2)]]
      (if (= :export (get-in mapping [:identity :scope]))
        (is (thrown? clojure.lang.ExceptionInfo (compile mapping)))
        (let [changed (compile mapping)]
          (is (= (:schema-fingerprint original) (:schema-fingerprint changed)))
          (is (not= (:definition-fingerprint original) (:definition-fingerprint changed))))))
    (is (= (:definition-fingerprint original)
           (:definition-fingerprint (compile (into (array-map) (reverse (seq accepted)))))))))

(deftest rejects-incomplete-ambiguous-or-inconsistent-metadata
  (doseq [bad [(dissoc accepted :scope)
               (assoc accepted :representation-version 1)
               (assoc-in accepted [:scope :kind] :tenant)
               (assoc-in accepted [:completion :domain] :infinite)
               (assoc-in accepted [:completion :extent] :declared-view)
               (assoc-in accepted [:attributes :order/amount :null] :nullable)
               (assoc-in accepted [:attributes :order/amount :extraction :shape] :elements)
               (assoc-in accepted [:attributes :order/amount :cardinality] :unknown)
               (assoc-in accepted [:attributes :order/customer :reference] :customer)
               (assoc-in accepted [:attributes :order/customer :reference :domain] :other)
               (assoc-in accepted [:attributes :order/customer :reference :type :parameters :key-types] [:string])
               (assoc-in accepted [:attributes :order/customer :type] :long)
               (assoc-in accepted [:attributes :order/id] (get-in accepted [:attributes :order/amount]))
               (assoc accepted :domains {order-domain {:entity-type (entity-type order-domain [:string])}})]]
    (is (thrown? clojure.lang.ExceptionInfo (mapping/compile-logical-mapping bad)))))

(deftest column-normalization-joins-the-common-logical-compiler
  (let [schema (-> old/schema
                   (assoc-in (conj old/relation-path :scope) (:scope accepted))
                   (assoc-in (conj old/relation-path :completion) (:completion accepted))
                   (assoc-in (conj old/relation-path :entity :identity-scope) :durable)
                   (assoc-in (conj old/relation-path :attributes :order/amount)
                             {:extraction {:shape :value :expression [:column "amount"]}
                              :type :bigdec :cardinality :one :null :absent :conversion {:version 1}}))
        context (assoc-in old/context [:dependencies :entity-ref-type] ref/type-contract)
        normalized (mapping/normalize-column-mapping schema :sales/orders context)
        compiled (mapping/compile-logical-mapping normalized)]
    (is (= (select-keys (get-in (mapping/compile-logical-mapping accepted) [:logical-schema :attributes])
                        [:order/id :order/amount])
           (get-in compiled [:logical-schema :attributes])))
    (is (= 2 (:representation-version normalized)))
    (is (= [:column "amount"] (get-in normalized [:attributes :order/amount :extraction :expression])))
    (is (= [{:column "id" :type :long}]
           (get-in schema (conj old/relation-path :entity :key))))
    (is (= compiled (mapping/normalize-provider-mapping
                      (fn [s captured] (mapping/normalize-column-mapping s :sales/orders captured))
                      schema context)))
    (doseq [s [(assoc-in schema (conj old/relation-path :physical-schema :columns 0 :nullable?) true)
               (assoc-in schema (conj old/relation-path :entity :constraint-source) :untrusted)
               (assoc-in schema (conj old/relation-path :attributes :order/amount :extraction :expression)
                         [:column "missing"])]]
      (is (thrown? clojure.lang.ExceptionInfo (mapping/normalize-column-mapping s :sales/orders context))))))

(deftest export-identity-is-an-explicit-accepted-key-construction
  (let [type (entity-type [old/source-id :export-row 0]
                          [:uuid {:kind :tuple :elements [:string :long]}])
        accepted (-> accepted
                     (assoc :identity {:type type :attribute :row/id :scope :export
                                       :extraction {:shape :key-tuple
                                                    :expressions [[:export-token] [:row-locator]]}})
                     (assoc :attributes {}))
        compiled (mapping/compile-logical-mapping accepted)
        key [#uuid "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" ["part-1" 42]]
        facts (mapping/project-facts compiled key {})]
    (is (= [[(ref/entity-ref type key) :row/id key]] facts))
    (is (= {:kind :tuple :elements [:uuid {:kind :tuple :elements [:string :long]}]}
           (get-in compiled [:pattern-contract :attributes :row/id :value-type])))
    (is (not= (:definition-fingerprint compiled)
              (:definition-fingerprint (mapping/compile-logical-mapping
                                         (assoc-in accepted [:identity :extraction :expressions 1] [:ordinal])))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (mapping/compile-logical-mapping (assoc-in accepted [:identity :extraction :expressions] [[:ordinal]]))))))

(deftest fingerprint-covers-all-portable-type-contract-extras
  (let [original (mapping/compile-logical-mapping accepted)
        changed (mapping/compile-logical-mapping
                  (assoc-in accepted [:identity :type :fixture/semantic] {:character \a :ratio 1/2}))]
    (is (not= (:definition-fingerprint original) (:definition-fingerprint changed)))
    (is (= (:schema-fingerprint original) (:schema-fingerprint changed)))))
