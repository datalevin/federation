(ns datalevin.federation.identity-storage-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.mapping :as mapping]
            [datalevin.federation.mapping-v2-test :as fixtures]
            [datalevin.udf :as udf]
            [datalevin.util :as u]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "datalevin-entity-ref-" (random-uuid)))]
      (u/file *dir*)
      (try (f) (finally (u/delete-files *dir*))))))

(deftest registered-references-survive-native-storage-and-reconnect
  (let [dir *dir*
        type (mapping/entity-type ref/type-contract :shared/order [:string :long])
        values (mapv #(ref/entity-ref type ["tenant-a" %]) [7 8 9])
        registry #(ref/bind-codec! (udf/create-registry))]
    (let [kv (d/open-kv dir {:runtime-opts {:udf-registry (registry)}})]
      (try
        (is (= ref/type-name (d/register-type kv ref/type-name ref/storage-definition)))
        (d/open-dbi kv "references" {:key-type ref/type-name})
        (d/transact-kv kv "references" (mapv (fn [x] [:put x :owned]) values))
        (is (= :owned (d/get-value kv "references" (ref/entity-ref type ["tenant-a" 7]))))
        (is (= (sort values) (mapv first (d/get-range kv "references" [:all]))))
        (finally (d/close-kv kv))))
    (let [kv (d/open-kv dir {:runtime-opts {:udf-registry (registry)}})]
      (try
        (d/open-dbi kv "references")
        (is (= :owned (d/get-value kv "references" (ref/entity-ref type ["tenant-a" 7]))))
        (is (= (sort values) (mapv first (d/get-range kv "references" [:all]))))
        (is (nil? (d/get-value kv "references" (ref/entity-ref type ["tenant-b" 7]))))
        (finally (d/close-kv kv))))))

(deftest projected-many-valued-facts-use-ordinary-engine-semantics
  (let [compiled (mapping/compile-logical-mapping fixtures/accepted)
        facts (mapping/project-facts compiled [7]
                                     {:order/tag ["red" "red" nil "blue"]
                                      :order/assignee [10 10 nil 11 12]})
        people (mapv #(ref/entity-ref (fixtures/entity-type fixtures/customer-domain [:long]) [%]) [10 11 12])]
    (is (= (set (for [tag ["red" "blue"] person people] [tag person]))
           (d/q '[:find ?tag ?person :in $ :where
                  [?e :order/tag ?tag] [?e :order/assignee ?person]] facts)))
    (is (= #{["red" "red"] ["red" "blue"] ["blue" "red"] ["blue" "blue"]}
           (d/q '[:find ?a ?b :in $ :where [?e :order/tag ?a] [?e :order/tag ?b]] facts)))
    (is (= #{[6]} (set (d/q '[:find (count ?e) :with ?tag ?person :in $ :where
                              [?e :order/tag ?tag] [?e :order/assignee ?person]] facts))))
    (is (= #{} (d/q '[:find ?e :in $ :where [?e :order/amount _]] facts)))
    (let [other (mapping/project-facts compiled [8] {:order/tag ["red"]})]
      (is (= #{[2]} (set (d/q '[:find (count ?e) :in $ :where [?e :order/tag "red"]]
                               (into facts other))))))))
