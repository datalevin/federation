(ns datalevin.federation.contract-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.federation.fingerprint :as fp]
            [datalevin.federation.mapping :as mapping]
            [datalevin.query.source :as source]))

(def source-id #uuid "11111111-1111-4111-8111-111111111111")
(def body-key :datalevin.federation/definition)
(def relation-path [:sales/orders body-key])
(def schema
  {:sales/source
   {:datalevin/definition-kind :source
    :datalevin.federation/definition
    {:version 1 :id :sales :source-instance-id source-id :provider :postgres
     :profile :profiles/sales-readonly}}
   :sales/orders
   {:datalevin/definition-kind :relation
    :datalevin.federation/definition
    {:version 1 :source :sales/source :from ["public" "orders"]
     :entity {:domain :order :identity-epoch 0 :key [{:column "id" :type :long}]
              :identity-attribute :order/id :constraint-source :provider}
     :attributes {:order/amount {:expression [:column "amount"] :type :bigdec
                                 :cardinality :one :null :absent}}
     :physical-schema {:columns [{:name "id" :physical-type {:kind :int64} :nullable? false}
                                  {:name "amount" :physical-type {:kind :decimal :precision 19 :scale 4}
                                   :nullable? true}]
                       :primary-key ["id"]}}}})

(def context
  {:compute-context-id "sales/read-profile-1"
   :dependencies {:provider {:name :postgres :version 1} :conversion-version 1
                  ;; F0 accepted type fixture, not a registered runtime codec.
                  :entity-ref-type {:name :datalevin.federation/entity-ref :version 1
                                    :fingerprint "fixture-entity-ref:v1:nominal-typed-key"}}})

(defn compile-schema [s] (mapping/compile-relation s :sales/orders context))
(defn digests [compiled]
  (select-keys (:definition compiled) [:schema-fingerprint :definition-fingerprint]))

(deftest canonical-v1-independent-byte-and-digest-fixtures
  (doseq [[input bytes digest]
          [[nil "6e303a" "5e2cc5130f232a437c79d1363dbf012379ee5d000cf6842e5036af99fd1eafc8"]
           [[1 "1"] "76383a6c313a3174313a31" "a95d8785d58e3dfc9b3fb3644a87efae5e950b7db0afa1ae73329bc1993c01d4"]
           [{:b [2 1] :a #{:y :x}}
            "6d35353a6b373a6e303a74313a617332303a6b373a6e303a74313a786b373a6e303a74313a796b373a6e303a74313a6276383a6c313a326c313a31"
            "876020de1c582057f069af4da2e81b139d77b40de99baeade89a11d480d469fa"]]]
    (is (= bytes (fp/hex (fp/canonical-bytes input))))
    (is (= (str "federation-definition:v1:sha256:" digest) (fp/fingerprint :definition input))))
  (is (= (fp/fingerprint :definition (array-map :a 1 :b 2))
         (fp/fingerprint :definition (array-map :b 2 :a 1))))
  (is (= 6 (count (set (map #(fp/fingerprint :definition %)
                            [1 1N "1" :one 'one 1M])))))
  (is (not= (fp/fingerprint :definition [1 2]) (fp/fingerprint :definition [2 1])))
  (is (not= (fp/fingerprint :definition 1) (fp/fingerprint :schema 1)))
  (is (thrown? clojure.lang.ExceptionInfo (fp/canonical-bytes (str (char 0xd800)))))
  (is (thrown? clojure.lang.ExceptionInfo (fp/canonical-bytes (Object.)))))

(deftest nominal-domains-do-not-depend-on-aliases-or-relation-identifiers
  (let [compiled (compile-schema schema)
        entity-type (get-in compiled [:source-info :pattern-contract :attributes :order/id :entity-type])
        renamed (mapping/compile-relation
                  (assoc schema :sales/another-orders (:sales/orders schema)) :sales/another-orders context)
        other-source (compile-schema (assoc-in schema [:sales/source body-key :source-instance-id]
                                               #uuid "22222222-2222-4222-8222-222222222222"))
        other-type #(get-in % [:source-info :pattern-contract :attributes :order/id :entity-type])]
    (is (= entity-type (other-type renamed)))
    (is (not= entity-type (other-type other-source)))
    (is (= entity-type (other-type (compile-schema (assoc-in schema [:sales/source body-key :id] :alias)))))))

(deftest normalized-example-produces-complete-spi-descriptor
  (let [{:keys [source-info definition]} (compile-schema schema)
        attributes (get-in source-info [:pattern-contract :attributes])
        entity-type (get-in attributes [:order/id :entity-type])]
    (is (= source-info (source/validate-source-info! source-info)))
    (is (= #{:order/id :order/amount} (set (keys attributes))))
    (is (= :long (get-in attributes [:order/id :value-type])))
    (is (= :bigdec (get-in attributes [:order/amount :value-type])))
    (is (= entity-type (get-in attributes [:order/amount :entity-type])))
    (is (= {:identity-domain-id [source-id :order 0] :key-types [:long]} (:parameters entity-type)))
    (is (= (:definition-fingerprint definition) (:definition-fingerprint source-info)))
    (is (= (assoc (:dependencies context) :source-instance-id source-id) (:dependencies definition)))
    (is (= #{} (:features source-info)))
    (is (= #{} (:required-features source-info)))))

(deftest composite-identities-and-reference-variants
  (let [s (-> schema
              (assoc-in (conj relation-path :entity :key)
                        [{:column "tenant" :type :string} {:column "id" :type :long}])
              (assoc-in (conj relation-path :physical-schema :primary-key) ["tenant" "id"])
              (update-in (conj relation-path :physical-schema :columns) conj
                         {:name "tenant" :physical-type {:kind :text} :nullable? false}
                         {:name "customer_id" :physical-type {:kind :int64} :nullable? true})
              (assoc-in (conj relation-path :attributes :order/customer)
                        {:expression [:tuple [:column "tenant"] [:column "customer_id"]]
                         :reference {:identity-domain-id [source-id :customer 0]
                                     :key-types [:string :long]}
                         :cardinality :one :null :absent}))
        attributes (get-in (compile-schema s) [:source-info :pattern-contract :attributes])]
    (is (= {:kind :tuple :elements [:string :long]} (get-in attributes [:order/id :value-type])))
    (is (= [:string :long] (get-in attributes [:order/id :entity-type :parameters :key-types])))
    (is (= [source-id :customer 0]
           (get-in attributes [:order/customer :value-type :parameters :identity-domain-id])))
    (is (not= (get-in attributes [:order/customer :entity-type])
              (get-in attributes [:order/customer :value-type]))))
  (let [s (-> schema
              (update-in (conj relation-path :physical-schema :columns) conj
                         {:name "customer_id" :physical-type {:kind :int64} :nullable? true})
              (assoc-in (conj relation-path :attributes :order/customer)
                        {:expression [:column "customer_id"]
                         :reference {:identity-domain-id [source-id :customer 0] :key-types [:long]}
                         :cardinality :one :null :absent}))]
    (is (= [:long] (get-in (compile-schema s)
                           [:source-info :pattern-contract :attributes :order/customer
                            :value-type :parameters :key-types])))))

(deftest fingerprint-invalidation-matrix
  (let [original (digests (compile-schema schema))]
    (doseq [s [(assoc schema :unrelated {:db/valueType :db.type/string})
               (update-in schema relation-path assoc :version 99 :description "new docs" :rows [[999 100M]]
                          :definition-fingerprint "caller-controlled" :schema-fingerprint "ignored")
               (update-in schema (conj relation-path :physical-schema :columns) #(vec (reverse %)))
               (update-in schema (conj relation-path :physical-schema :columns) conj
                          {:name "unused" :physical-type {:kind :text} :nullable? true})]]
      (is (= original (digests (compile-schema s)))))
    (doseq [s [(assoc-in schema (conj relation-path :entity :identity-epoch) 1)
               (assoc-in schema (conj relation-path :entity :identity-attribute) :order/key)]]
      (let [changed (digests (compile-schema s))]
        (is (= (:schema-fingerprint original) (:schema-fingerprint changed)))
        (is (not= (:definition-fingerprint original) (:definition-fingerprint changed)))))
    (let [changed (digests (compile-schema (assoc-in schema
                                                    (conj relation-path :physical-schema :columns 1 :physical-type :scale)
                                                    5)))]
      (is (not= (:schema-fingerprint original) (:schema-fingerprint changed)))
      (is (not= (:definition-fingerprint original) (:definition-fingerprint changed))))
    (doseq [path [[:dependencies :conversion-version] [:dependencies :provider :version]
                  [:dependencies :entity-ref-type :version]]]
      (let [changed (digests (mapping/compile-relation schema :sales/orders (update-in context path inc)))]
        (is (= (:schema-fingerprint original) (:schema-fingerprint changed)))
        (is (not= (:definition-fingerprint original) (:definition-fingerprint changed)))))
    (let [compiled (compile-schema schema)
          reopened (assoc-in schema relation-path (:definition compiled))]
      (is (= original (digests (mapping/compile-relation reopened :sales/orders
                                                       (assoc context :dependencies
                                                              (get-in compiled [:definition :dependencies])))))))))

(deftest reject-unresolved-or-unsafe-accepted-mappings
  (doseq [s [(assoc-in schema (conj relation-path :entity :constraint-source) :untrusted)
             (assoc-in schema (conj relation-path :entity :key) [])
             (assoc-in schema (conj relation-path :physical-schema :columns 0 :nullable?) true)
             (assoc-in schema (conj relation-path :attributes :order/amount :cardinality) :many)
             (assoc-in schema (conj relation-path :attributes :order/amount :expression) [:column "missing"])
             (update-in schema (conj relation-path :attributes :order/amount)
                        #(-> % (dissoc :type) (assoc :reference :customer)))
             (assoc-in schema [:sales/source :datalevin/definition-kind] :relation)]]
    (is (thrown? clojure.lang.ExceptionInfo (compile-schema s)))))

(deftest reference-type-identity-fields-survive-compilation
  (let [s (-> schema
              (update-in (conj relation-path :physical-schema :columns) conj
                         {:name "parent_id" :physical-type {:kind :int64} :nullable? true})
              (assoc-in (conj relation-path :attributes :order/parent)
                        {:expression [:column "parent_id"]
                         :reference {:identity-domain-id [source-id :order 0] :key-types [:long]}
                         :cardinality :one :null :absent}))
        contract (get-in context [:dependencies :entity-ref-type])
        compile-type #(mapping/compile-relation s :sales/orders
                                               (assoc-in context [:dependencies :entity-ref-type] %))
        original (compile-type contract)
        type-of #(get-in % [:source-info :pattern-contract :attributes :order/id :entity-type])
        original-type (type-of original)
        variants [[:same contract true]
                  [:map-order (into (array-map) (reverse (seq contract))) true]
                  [:metadata (with-meta contract {:fixture/note "docs"}) true]
                  [:version (update contract :version inc) false]
                  [:fingerprint (assoc contract :fingerprint "fixture-entity-ref:v2") false]
                  [:extra (assoc contract :fixture/extra {:meaning "v2"}) false]
                  [:extra-value (assoc contract :fixture/extra {:meaning "v3"}) false]]]
    (doseq [[label accepted same?] variants]
      (testing (str label)
        (let [compiled (compile-type accepted)
              type (type-of compiled)]
          (is (= same? (= original-type type)))
          (is (= (assoc accepted :kind :custom :parameters (:parameters original-type)) type))
          (is (= type (get-in compiled [:source-info :pattern-contract :attributes :order/parent :value-type])))
          (is (= (:parameters original-type) (:parameters type)))
          (is (= (get-in original [:definition :schema-fingerprint])
                 (get-in compiled [:definition :schema-fingerprint])))
          (is (= same? (= (get-in original [:definition :definition-fingerprint])
                          (get-in compiled [:definition :definition-fingerprint]))))
          (when same? (is (= (hash original-type) (hash type)))))))
    (is (= 5 (count (set (map #(type-of (compile-type (second %))) variants)))))
    (doseq [invalid [(assoc contract :extra :v2)
                     (assoc contract :version 0)
                     (assoc contract :fingerprint "")]]
      (is (thrown? clojure.lang.ExceptionInfo (compile-type invalid))))))
