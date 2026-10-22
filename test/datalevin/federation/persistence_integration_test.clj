(ns datalevin.federation.persistence-integration-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.dump :as dump]
            [datalevin.federation.install :as install]
            [datalevin.federation.install-test :as fixture]
            [datalevin.federation.local :as local]
            [datalevin.federation.mapping :as mapping]
            [datalevin.server :as server]
            [datalevin.util :as u]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream
            DataOutputStream PushbackReader StringReader StringWriter]
           [java.nio.channels ServerSocketChannel]))

(def ^:dynamic *dir* nil)
(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "federation-persistence-integration-" (random-uuid)))]
      (try (log/with-min-level :report (f)) (finally (u/delete-files *dir*))))))

(defn- definition [snapshot id]
  (get-in snapshot [:schema id :datalevin.federation/definition]))

(defn- query-loaded [conn loaded]
  (let [compiled (get-in loaded [:compiled :sales/orders])
        info (mapping/source-info (:accepted compiled) :persisted-test/runtime)
        facts (into [] (mapcat (fn [[key attributes]] (mapping/project-facts compiled [key] attributes)))
                    [[7 {:order/tag ["red" "blue"] :order/amount 2M}]
                     [8 {:order/tag ["red"] :order/amount 3M}]])
        source (local/source info facts)
        query '[:find ?tag ?amount :in $ $orders :where
                [?native :native/order-id ?id]
                [$orders ?order :order/id ?id]
                [$orders ?order :order/tag ?tag]
                [$orders ?order :order/amount ?amount]]]
    (is (= (:definition-fingerprint compiled) (:definition-fingerprint info)))
    (is (= #{["red" 2M] ["blue" 2M]} (d/q query @conn source)))
    (is (= (d/q query @conn facts) (d/q query @conn source)))))

(deftest persisted-mappings-rebuild-query-sources-after-reopen-and-dump-restore
  (let [path (str *dir* "/original")
        conn (d/create-conn path {:native/order-id {:db/valueType :db.type/long}}
                            {:wal? false :background-sampling? false})
        text (StringWriter.)
        binary (ByteArrayOutputStream.)
        saved (try
                (install/install! conn fixture/definitions {:expected-versions fixture/versions})
                (d/transact! conn [{:db/id 1 :native/order-id 7}])
                (query-loaded conn (install/load-definitions conn))
                (binding [*out* text] (dump/dump-datalog conn))
                (dump/dump-datalog conn (DataOutputStream. binary))
                (install/load-definitions conn)
                (finally (d/close conn)))]
    (dump/load-datalog (str *dir* "/text") (PushbackReader. (StringReader. (str text)))
                       nil {:wal? false})
    (dump/load-datalog (str *dir* "/binary")
                       (DataInputStream. (ByteArrayInputStream. (.toByteArray binary)))
                       nil {:wal? false} true)
    (doseq [name ["original" "text" "binary"]]
      (let [conn (d/create-conn (str *dir* "/" name))]
        (try
          (let [loaded (install/load-definitions conn)]
            (is (= (:compiled saved) (:compiled loaded)))
            (is (= (definition saved :sales) (definition loaded :sales)))
            (query-loaded conn loaded)
            (let [refreshed (install/install! conn
                              {:sales/orders (assoc-in fixture/relation-definition
                                              [:datalevin.federation/definition :documentation] "Reviewed")}
                              {:expected-versions {:sales/orders 1}})]
              (is (= 2 (:version (definition refreshed :sales/orders))))
              (query-loaded conn refreshed)))
          (finally (d/close conn)))))))

(deftest remote-installation-revalidates-revision-conflicts-and-dependents
  (let [srv (server/create {:port 0 :root (str *dir* "/server")})
        port (.getLocalPort (.socket ^ServerSocketChannel (.-server-socket srv)))]
    (try
      (server/start srv)
      (let [uri (str "dtlv://datalevin:datalevin@127.0.0.1:" port "/persisted")
            conn (d/create-conn uri)
            competing (d/create-conn uri)]
        (try
          (let [attempts (atom 0)
                installed (install/install! conn fixture/definitions
                            {:expected-versions fixture/versions
                             :prepare (fn [{:keys [definitions]} _]
                                        (when (= 1 (swap! attempts inc))
                                          (d/update-schema competing {:native/order-id {:db/valueType :db.type/long}}))
                                        definitions)})]
            (is (= 2 @attempts))
            (is (= installed (install/load-definitions competing)))
            (is (= :db.type/long (get-in installed [:schema :native/order-id :db/valueType])))
            (let [refreshed (install/install! competing
                              {:sales (assoc-in fixture/source-definition
                                                [:datalevin.federation/definition :profile] :profiles/remote-v2)}
                              {:expected-versions {:sales 1}})]
              (is (= 2 (:version (definition refreshed :sales/orders))))
              (is (= refreshed (install/load-definitions conn)))
              (d/transact! conn [{:db/id 1 :native/order-id 7}])
              (is (= #{[7]} (set (d/q '[:find ?id :where [_ :native/order-id ?id]] @conn))))
              (let [compiled (get-in refreshed [:compiled :sales/orders])
                    source (local/source (mapping/source-info (:accepted compiled) :remote-metadata/runtime)
                                         (mapping/project-facts compiled [7] {:order/tag ["red"]}))]
                (is (= #{["red"]} (d/q '[:find ?tag :in $orders :where [$orders _ :order/tag ?tag]] source))))
              (is (= :federation/version-conflict
                     (try (install/remove! conn {:sales 1}) nil
                          (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
              (is (empty? (:compiled (install/remove! conn {:sales 2 :sales/orders 2}))))))
          (finally (d/close competing) (d/close conn))))
      (finally (server/stop srv)))))
