(ns datalevin.federation.install-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.walk :as walk]
            [datalevin.client.request :as request]
            [datalevin.core :as d]
            [datalevin.federation.contract-test :as contract]
            [datalevin.federation.install :as install]
            [datalevin.federation.mapping :as mapping]
            [datalevin.federation.mapping-v2-test :as fixtures]
            [datalevin.util :as u])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private owner :datalevin.federation/definition)
(def ^:private membership :datalevin.federation/relations)
(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "federation-install-" (random-uuid)))]
      (try (f) (finally (u/delete-files *dir*))))))

(defn- with-local [schema f]
  (let [conn (d/create-conn *dir* schema {:wal? false :background-sampling? false})]
    (try (f conn) (finally (d/close conn)))))

(def source-definition
  {:datalevin/definition-kind :source
   :datalevin.federation/definition
   {:format-version 1 :source-instance-id contract/source-id :provider :api
    :profile :profiles/sales-readonly}})

(def relation-definition
  {:datalevin/definition-kind :relation
   :datalevin.federation/definition
   {:format-version 1 :source :sales :accepted fixtures/accepted}})

(def definitions {:sales source-definition :sales/orders relation-definition})
(def versions {:sales nil :sales/orders nil})
(defn- definition [snapshot id] (get-in snapshot [:schema id owner]))
(defn- install-base! [conn]
  (install/install! conn definitions {:expected-versions versions}))
(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(deftest installation-derives-digests-and-native-memberships
  (let [foreign {:datalevin/definition-kind :future/view :future/definition {:opaque [1 2]}}]
    (with-local {:future/view foreign :native/value {:db/valueType :db.type/string}}
      (fn [conn]
        (let [input (assoc-in definitions [:sales owner :definition-fingerprint] "untrusted")
              result (install/install! conn input {:expected-versions versions})
              schema (:schema result)
              relation (definition result :sales/orders)
              source (definition result :sales)]
          (is (= 1 (:version source) (:version relation)))
          (is (= :federation/v2 (:fingerprint-scheme relation)))
          (is (not= "untrusted" (:definition-fingerprint source)))
          (is (= {:sales (:definition-fingerprint source)}
                 (get-in relation [:accepted :dependencies :definition-contracts])))
          (is (= #{:sales/orders} (get-in schema [:order/amount membership])))
          (is (= :db.type/bigdec (get-in schema [:order/amount :db/valueType])))
          (is (= :db.cardinality/many (get-in schema [:order/tag :db/cardinality])))
          (is (nil? (get-in schema [:order/customer :db/valueType]))
              "Nominal references do not become native long entity IDs")
          (is (nil? (get-in schema [:sales :db/aid])))
          (is (pos-int? (get-in schema [:order/id :db/aid])))
          (is (= foreign (:future/view schema)))
          (is (= result (install/load-definitions conn)))
          (is (= result (install/install! conn {} {:expected-versions {}})))
          (is (= (:compiled result)
                 (assoc (:compiled result) :sales/orders
                        (mapping/compile-logical-mapping (:accepted relation))))))))))

(deftest refresh-distinguishes-versions-from-semantic-fingerprints
  (with-local {}
    (fn [conn]
      (let [first (install-base! conn)
            documented (install/install! conn
                         {:sales (assoc-in source-definition [owner :documentation] "Audited profile")}
                         {:expected-versions {:sales 1}})
            refreshed (install/install! conn
                        {:sales (assoc-in source-definition [owner :profile] :profiles/sales-v2)}
                        {:expected-versions {:sales 2}})]
        (is (= 2 (:version (definition documented :sales))))
        (is (= 1 (:version (definition documented :sales/orders))))
        (is (= (:definition-fingerprint (definition first :sales))
               (:definition-fingerprint (definition documented :sales))))
        (is (= 3 (:version (definition refreshed :sales))))
        (is (= 2 (:version (definition refreshed :sales/orders))))
        (is (not= (:definition-fingerprint (definition first :sales/orders))
                  (:definition-fingerprint (definition refreshed :sales/orders))))
        (is (= (:schema-fingerprint (definition first :sales/orders))
               (:schema-fingerprint (definition refreshed :sales/orders))))
        (is (= contract/source-id (:source-instance-id (definition refreshed :sales))))
        (is (= refreshed (install/load-definitions conn)))))))

(deftest dependency-refresh-is-transitive-and-atomic
  (with-local {}
    (fn [conn]
      (install-base! conn)
      (let [dependent (-> relation-definition
                          (assoc-in [owner :accepted :relation-id] :sales/summary)
                          (assoc-in [owner :depends-on] #{:sales/orders}))]
        (install/install! conn {:sales/summary dependent} {:expected-versions {:sales/summary nil}})
        (let [before (install/load-definitions conn)
              changed (assoc-in relation-definition [owner :accepted :requirements :fields]
                                ["id" "amount" "tags" "customer-id" "assignees" "new-proof"])
              result (install/install! conn {:sales/orders changed} {:expected-versions {:sales/orders 1}})]
          (is (= 2 (:version (definition result :sales/summary))))
          (is (not= (:definition-fingerprint (definition before :sales/summary))
                    (:definition-fingerprint (definition result :sales/summary))))
          (is (= :federation/missing-dependency
                 (error #(install/remove! conn {:sales/orders 2}))))
          (is (= result (install/load-definitions conn)))
          (let [cyclic (assoc-in changed [owner :depends-on] #{:sales/summary})]
            (is (= :federation/dependency-cycle
                   (error #(install/install! conn {:sales/orders cyclic}
                                             {:expected-versions {:sales/orders 2}})))))
          (is (= result (install/load-definitions conn)))
          (let [removed (install/remove! conn {:sales 1 :sales/orders 2 :sales/summary 2})]
            (is (empty? (:compiled removed)))
            (is (not (contains? (:schema removed) :sales)))
            (is (not (contains? (:order/id (:schema removed)) membership)))))))))

(deftest rejected-candidates-leave-the-entire-schema-unchanged
  (with-local {}
    (fn [conn]
      (install-base! conn)
      (let [before (d/schema-snapshot conn)]
        (doseq [[expected patches]
                [[:federation/source-mismatch
                  {:sales (assoc-in source-definition [owner :source-instance-id] (random-uuid))}]
                 [:federation/definition {:sales (assoc-in source-definition [owner :profile] {:password "secret"})}]
                 [:federation/definition {:sales (assoc-in source-definition [owner :password] "secret")}]
                 [:federation/missing-dependency {:sales/orders (assoc-in relation-definition [owner :source] :missing)}]
                 [:federation/dependency-cycle {:sales/orders (assoc-in relation-definition [owner :source] :sales/orders)}]]]
          (testing (str expected)
            (is (= expected (error #(install/install! conn patches
                                    {:expected-versions (zipmap (keys patches) (repeat 1))
                                     :schema-update {:native/new {:db/valueType :db.type/string}}}))))
            (is (= before (d/schema-snapshot conn)))))))))

(deftest native-edits-and-accepted-refresh-share-one-commit
  (with-local {:order/amount {:db/valueType :db.type/bigdec :db/doc "retain me"}}
    (fn [conn]
      (install-base! conn)
      (let [before (d/schema-snapshot conn)
            changed (assoc-in relation-definition [owner :accepted :attributes :order/amount :cardinality] :many)]
        (is (= :federation/schema-mismatch
               (error #(install/install! conn {:sales/orders changed}
                                         {:expected-versions {:sales/orders 1}}))))
        (is (= before (d/schema-snapshot conn)))
        (let [result (install/install! conn {:sales/orders changed}
                       {:expected-versions {:sales/orders 1}
                        :schema-update {:order/amount {:db/cardinality :db.cardinality/many}}})]
          (is (= :db.cardinality/many (get-in result [:schema :order/amount :db/cardinality])))
          (is (= "retain me" (get-in result [:schema :order/amount :db/doc])))
          (d/transact! conn [{:db/id 1 :order/amount [1M 2M]}])
          (install/remove! conn {:sales/orders 2 :sales 1})
          (is (= #{[1M] [2M]} (set (d/q '[:find ?v :where [1 :order/amount ?v]] @conn))))
          (is (= "retain me" (get-in (d/schema conn) [:order/amount :db/doc])))
          (is (nil? (get-in (d/schema conn) [:order/amount membership]))))))))

(deftest incompatible-shared-attributes-and-domain-contracts-reject
  (with-local {}
    (fn [conn]
      (install-base! conn)
      (let [before (d/schema-snapshot conn)
            other (assoc-in relation-definition [owner :accepted :relation-id] :sales/other)]
        (doseq [candidate [(assoc-in other [owner :accepted :attributes :order/amount :type] :long)
                           (assoc-in other [owner :accepted :attributes :order/amount :cardinality] :many)
                           (assoc-in other [owner :accepted :identity :type :version] 2)]]
          (is (#{:federation/attribute-conflict :federation/domain-conflict}
                (error #(install/install! conn {:sales/other candidate}
                                          {:expected-versions {:sales/other nil}}))))
          (is (= before (d/schema-snapshot conn))))
        (is (= #{:sales/orders :sales/other}
               (get-in (install/install! conn {:sales/other other}
                                         {:expected-versions {:sales/other nil}})
                       [:schema :order/amount membership])))))))

(deftest native-revision-conflicts-reprepare-from-a-fresh-snapshot
  (with-local {}
    (fn [conn]
      (let [seen (atom [])
            result (install/install! conn definitions
                     {:expected-versions versions
                      :prepare (fn [{:keys [schema definitions]} {:keys [check-active! remaining-ms]}]
                                 (check-active!)
                                 (is (pos? (remaining-ms)))
                                 (swap! seen conj (get-in schema [:native/value :db/doc]))
                                 (when (= 1 (count @seen))
                                   (is (not= ::blocked
                                             (deref (future (d/update-schema conn
                                                              {:native/value {:db/valueType :db.type/string
                                                                              :db/doc "concurrent"}}))
                                                    3000 ::blocked))))
                                 definitions)})]
        (is (= [nil "concurrent"] @seen))
        (is (= "concurrent" (get-in result [:schema :native/value :db/doc])))
        (is (= 1 (:version (definition result :sales/orders))))
        (is (= result (install/load-definitions conn)))))))

(deftest definition-version-conflicts-never-overwrite-the-winner
  (with-local {}
    (fn [conn]
      (let [ready (CountDownLatch. 2)
            release (CountDownLatch. 1)
            run (fn [] (error #(install/install! conn definitions
                               {:expected-versions versions
                                :prepare (fn [{:keys [definitions]} _]
                                           (.countDown ready)
                                           (when-not (.await release 3 TimeUnit/SECONDS)
                                             (throw (ex-info "test barrier timeout" {})))
                                           definitions)})))
            writers [(future (run)) (future (run))]]
        (try
          (is (.await ready 3 TimeUnit/SECONDS))
          (finally (.countDown release)))
        (is (= {nil 1 :federation/version-conflict 1}
               (frequencies (mapv #(deref % 5000 ::timeout) writers))))
        (is (= 1 (:version (definition (install/load-definitions conn) :sales))))
        (is (= :federation/version-conflict
               (error #(install/install! conn definitions {:expected-versions versions}))))))))

(deftest retries-preparation-cancellation-and-deadlines-are-bounded
  (with-local {}
    (fn [conn]
      (let [calls (atom 0)
            prepare (fn [{:keys [definitions]} _]
                      (let [n (swap! calls inc)]
                        (deref (future (d/update-schema conn {:native/value {:db/doc (str n)}})) 3000 nil))
                      definitions)]
        (is (= :federation/retry-limit
               (error #(install/install! conn definitions
                         {:expected-versions versions :max-attempts 2 :prepare prepare}))))
        (is (= 2 @calls))
        (is (not (contains? (d/schema conn) :sales))))
      (let [before (d/schema-snapshot conn)]
        (is (= :federation/cancelled
               (error #(install/install! conn definitions
                         {:expected-versions versions :cancelled? (constantly true)}))))
        (is (= :preparation/failed
               (error #(install/install! conn definitions
                         {:expected-versions versions
                          :prepare (fn [_ _] (throw (ex-info "preparation failed" {:error :preparation/failed})))}))))
        (is (= :socket/timeout
               (error #(install/install! conn definitions
                         {:expected-versions versions :timeout-ms 20
                          :prepare (fn [{:keys [definitions]} _] (Thread/sleep 35) definitions)}))))
        (binding [request/*request* (request/budget 20 0 nil nil)]
          (let [outer (request/current)]
            (is (= :socket/timeout
                   (error #(install/install! conn definitions
                             {:expected-versions versions :timeout-ms 5000
                              :prepare (fn [{:keys [definitions]} _] (Thread/sleep 35) definitions)}))))
            (is (identical? outer (request/current)))))
        (is (= before (d/schema-snapshot conn)))))))

(deftest identity-changes-require-explicit-compatible-accepted-mappings
  (with-local {}
    (fn [conn]
      (install-base! conn)
      (let [new-id (random-uuid)
            accepted (walk/postwalk #(if (= contract/source-id %) new-id %) fixtures/accepted)
            result (install/install! conn
                     {:sales (assoc-in source-definition [owner :source-instance-id] new-id)
                      :sales/orders (assoc-in relation-definition [owner :accepted] accepted)}
                     {:expected-versions {:sales 1 :sales/orders 1}})]
        (is (= new-id (:source-instance-id (definition result :sales))))
        (is (= new-id (get-in result [:compiled :sales/orders :accepted :source-instance-id])))
        (is (= 2 (:version (definition result :sales)) (:version (definition result :sales/orders)))))
      (is (= (d/schema conn) (:schema (install/load-definitions conn)))))))

(deftest native-types-reject-incompatible-mappings-before-writing
  (with-local {:order/id {:db/valueType :db.type/string}}
    (fn [conn]
      (let [before (d/schema-snapshot conn)]
        (is (= :federation/schema-mismatch (error #(install-base! conn))))
        (is (= before (d/schema-snapshot conn)))))))

(deftest tuple-types-and-removed-attributes-retain-native-data
  (with-local {}
    (fn [conn]
      (install-base! conn)
      (d/transact! conn [{:db/id 1 :order/amount 2M}])
      (let [relation (-> relation-definition
                         (update-in [owner :accepted :attributes] dissoc :order/amount)
                         (assoc-in [owner :accepted :attributes :order/pair]
                                   {:extraction {:shape :value :expression [:field "pair"]}
                                    :type {:kind :tuple :elements [:long :string]}
                                    :cardinality :one :null :absent :conversion {:version 1}}))
            result (install/install! conn {:sales/orders relation}
                     {:expected-versions {:sales/orders 1}})]
        (is (= :db.type/tuple (get-in result [:schema :order/pair :db/valueType])))
        (is (= [:db.type/long :db.type/string] (get-in result [:schema :order/pair :db/tupleTypes])))
        (is (nil? (get-in result [:schema :order/amount membership])))
        (is (= #{[2M]} (set (d/q '[:find ?v :where [1 :order/amount ?v]] @conn))))
        (is (= result (install/load-definitions conn)))))))

(deftest callbacks-cannot-run-inside-an-explicit-writer
  (with-local {}
    (fn [conn]
      (let [called (atom false)]
        (d/with-transaction [tx conn]
          (is (= :federation/inside-transaction
                 (error #(install/install! tx definitions
                           {:expected-versions versions
                            :prepare (fn [{:keys [definitions]} _] (reset! called true) definitions)}))))
          (is (= :federation/inside-transaction
                 (error #(install/install! conn definitions {:expected-versions versions})))))
        (is (false? @called))
        (is (not (contains? (d/schema conn) :sales)))))))

(deftest reopen-verifies-recorded-metadata-without-discovery
  (let [saved (with-local {} install-base!)
        conn (d/create-conn *dir*)]
    (try
      (is (= saved (install/load-definitions conn)))
      (d/update-schema conn {:sales/orders {owner (assoc (definition saved :sales/orders)
                                                        :definition-fingerprint "stale")}})
      (is (= :federation/stale-definition (error #(install/load-definitions conn))))
      (is (= :federation/stale-definition
             (error #(install/install! conn {} {:expected-versions {}}))))
      (let [revalidated (install/install! conn {:sales/orders relation-definition}
                          {:expected-versions {:sales/orders 1}})]
        (is (= 2 (:version (definition revalidated :sales/orders))))
        (is (= (:definition-fingerprint (definition saved :sales/orders))
               (:definition-fingerprint (definition revalidated :sales/orders))))
        (is (= revalidated (install/load-definitions conn))))
      (finally (d/close conn)))))

(deftest memberships-and-unknown-fingerprint-schemes-require-explicit-revalidation
  (with-local {}
    (fn [conn]
      (let [saved (install-base! conn)]
        (d/update-schema conn {:sales/orders {owner (assoc (definition saved :sales/orders)
                                                          :fingerprint-scheme :future/v3)}})
        (is (= :federation/stale-definition (error #(install/load-definitions conn))))
        (install/install! conn {:sales/orders relation-definition} {:expected-versions {:sales/orders 1}})
        (d/update-schema conn {:native/other {membership #{:sales}}})
        (let [before (d/schema-snapshot conn)]
          (is (= :federation/membership (error #(install/load-definitions conn))))
          (is (= :federation/membership
                 (error #(install/install! conn {} {:expected-versions {}}))))
          (is (= :federation/membership
                 (error #(install/install! conn {:sales source-definition}
                                           {:expected-versions {:sales 1}}))))
          (is (= before (d/schema-snapshot conn))))
        (d/update-schema conn {:native/other {membership :db/retract}})
        (is (= :federation/schema-patch
               (error #(install/install! conn {} {:expected-versions {}
                                                  :schema-update {:order/id {membership #{}}}}))))))))
