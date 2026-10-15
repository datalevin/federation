(ns datalevin.federation.local-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.federation.local :as local]
            [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]
            [datalevin.util :as u]))

(def descriptor
  {:spi-versions #{1} :source-instance-id :test-local :compute-context-id :embedded
   :definition-id :pairs :definition-fingerprint "local:v1:pairs"
   :features #{} :required-features #{}
   :pattern-contract {:kind :tuple :columns [{:type :long :nullable? false}
                                            {:type :string :nullable? true}]}})

(defn category [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(deftest local-queries-match-eager-relations
  (let [rows [[1 "a"] [2 nil] [3 "b"] [4 "a"] [4 "a"]]
        s (local/source descriptor rows)]
    (doseq [query ['[:find ?id ?v :in $s :where [$s ?id ?v]]
                   '[:find (count ?v) . :in $s :where [$s _ ?v]]
                   '[:find (count ?v) . :with ?id :in $s :where [$s ?id ?v]]
                   '[:find ?a ?b :in $s :where [$s ?a ?v] [$s ?b ?v] [(< ?a ?b)]]
                   '[:find ?id :in $s :where [$s ?id _] [(> ?id 2)]]
                   '{:find [?id ?v] :keys [id value] :in [$s] :where [[$s ?id ?v]]}]]
      (is (= (d/q query rows) (d/q query s)) (pr-str query)))
    (let [query '[:find [?v ...] :in $s :where [$s _ ?v] [(not= ?v nil)]]]
      (is (= (frequencies (d/q query rows)) (frequencies (d/q query s)))))
    (is (= 2 (count (get-in (d/explain {:run? true}
                                     '[:find ?a ?b :in $s :where [$s ?a ?v] [$s ?b ?v]] s)
                            [:sources 0 :selected]))))
    (local/replace-rows! s [[9 "new"]])
    (is (= #{[9]} (d/q '[:find ?id :in $s :where [$s ?id _]] s)))
    (is (= descriptor (spi/-source-info s)))))

(deftest pinned-aliases-and-native-views
  (let [dir (u/tmp-dir (str "federation-local-" (random-uuid)))
        conn (d/get-conn dir {:id {:db/valueType :db.type/long}})
        s (local/source descriptor [[1 "old"]])
        changed (atom false)
        wrapped (reify spi/IQuerySource
                  (-source-info [_] (spi/-source-info s))
                  (-open-session [_ request] (spi/-open-session s request))
                  (-plan-source [_ session request]
                    (when (compare-and-set! changed false true)
                      (local/replace-rows! s [[2 "new"]])
                      (d/transact! conn [[:db/retract 1 :id 1] [:db/add 1 :id 2]]))
                    (spi/-plan-source s session request)))
        query '[:find ?id ?v :in $ $a $b
                :where [?e :id ?id] [$a ?id ?v] [$b ?id ?v]]]
    (try
      (d/transact! conn [{:db/id 1 :id 1}])
      (is (= #{[1 "old"]} (d/q query @conn wrapped wrapped)))
      (is (= #{[2 "new"]} (d/q query @conn wrapped wrapped)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest local-bounds-and-large-selective-scan
  (let [s (local/source descriptor (mapv #(vector % "a") (range 20000)))
        query '{:find [?id] :in [$s] :where [[$s ?id _] [(> ?id 19998)]]
                :query-source {:limits {:retained-bytes 16777216 :spill-bytes 0}}}
        explain (d/explain {:run? true} query s)]
    (is (= #{[19999]} (d/q query s)))
    (is (= 20000 (get-in explain [:cursors 0 :metrics :read-rows])))
    (is (= 1 (:result-rows explain)))
    (is (= :query-source/limit
           (category #(d/q (assoc-in query [:query-source :limits :retained-bytes] 1024) s))))
    (is (= :query-source/limit
           (category #(d/q (assoc-in query [:query-source :limits :batch-bytes] 1) s)))))
  (let [s (local/source descriptor [[1 "a"] [2 "b"] [3 "c"]])
        query '{:find [?id] :in [$s] :where [[$s ?id _]] :limit 1}
        explain (d/explain {:run? true} query s)]
    (is (= 1 (get-in explain [:cursors 0 :metrics :read-rows])))
    (is (= :query-source/limit
           (category #(d/q (-> query (dissoc :limit)
                                (assoc :query-source {:limits {:result-rows 1}})) s))))))

(deftest decimal-join-and-predicate-semantics
  (let [info (assoc-in descriptor [:pattern-contract :columns 1] {:type :bigdec :nullable? false})
        rows [[1 1.0M] [2 1.00M] [3 2M]]
        s (local/source info rows)]
    (doseq [query ['[:find ?n :in $s :where [$s _ ?n]]
                   '[:find ?id :in $s :where [$s ?id ?n] [(= ?n 1M)]]
                   '[:find ?a ?b :in $s :where [$s ?a ?n] [$s ?b ?n]]
                   '[:find (sum ?n) . :with ?id :in $s :where [$s ?id ?n]]]]
      (is (= (d/q query rows) (d/q query s)) (pr-str query)))))

(defn ref-type [domain]
  {:kind :custom :name :test/entity-ref :version 1 :fingerprint "test-ref:v1"
   :parameters {:domain domain :key-types [:long]}})

(deftype Ref [type key]
  value/IQuerySourceValue
  (-logical-type [_] type)
  (-retained-bytes [_] (+ 128 (value/value-charge type)))
  Object
  (equals [_ other] (and (instance? Ref other) (= type (.-type ^Ref other)) (= key (.-key ^Ref other))))
  (hashCode [_] (hash [type key])))

(deftest nominal-reference-valued-eav
  (let [order-type (ref-type :orders) customer-type (ref-type :customers)
        order (Ref. order-type 1) customer (Ref. customer-type 1)
        info (assoc descriptor :pattern-contract
                    {:kind :eav :attributes {:order/id {:entity-type order-type :value-type :long}
                                            :order/customer {:entity-type order-type :value-type customer-type}
                                            :customer/name {:entity-type customer-type :value-type :string}}})
        rows [[order :order/id 1] [order :order/customer customer]
              [customer :customer/name "Ada"]]
        s (local/source info (conj rows [(Ref. order-type 2) :order/customer nil]))]
    (is (not= order customer))
    (doseq [query ['[:find ?id ?name :in $s :where [$s ?o :order/id ?id]
                   [$s ?o :order/customer ?c] [$s ?c :customer/name ?name]]
                   '[:find ?o :in $s :where [$s ?o :order/customer _]]]]
      (is (= (d/q query rows) (d/q query s))))
    (is (= :query-source/invalid
           (category #(d/q '[:find ?o :in $s ?c :where [$s ?o :order/customer ?c]] s order))))))

(deftest immutable-owned-bytes-and-query-equality-guard
  (let [info (assoc-in descriptor [:pattern-contract :columns 1] {:type :bytes :nullable? false})
        original (byte-array [1 2])
        s (local/source info [[1 original]])
        fetch (fn []
                (let [control (control/create)
                      request {:control control :spi-version 1 :features #{} :consistency :snapshot
                               :source-descriptors [info]}
                      session (spi/-open-session s request)]
                  (try
                    (let [response (spi/-plan-source s session
                                     (merge request {:source-descriptor info :source-symbol '$s :bindings {}
                                                     :clauses [{:id 0 :kind :pattern
                                                                :terms [[:ignore] [:var '?v]]}]}))
                          alternative (first (:alternatives response))
                          cursor (spi/-open-cursor session
                                   (merge request {:definition-fingerprint (:definition-fingerprint info)
                                                   :alternative-id (:id alternative) :plan (:plan alternative)
                                                   :source-symbol '$s}))
                          batch (spi/-next-batch! cursor {:control control :max-rows 1 :max-bytes 10000})]
                      (try (ffirst (:rows batch))
                           (finally (spi/release! (:reservation batch)) (spi/-close-cursor! cursor))))
                    (finally (spi/-close-session! session) (control/close! control)))))]
    (aset-byte original 0 (byte 9))
    (let [first-result (fetch)]
      (is (= [1 2] (vec first-result)))
      (aset-byte first-result 0 (byte 8))
      (is (= [1 2] (vec (fetch)))))
    (is (= :query-source/unsupported
           (category #(d/q '[:find ?v . :in $s :where [$s _ ?v]] s))))))
