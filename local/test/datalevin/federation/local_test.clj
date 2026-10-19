(ns datalevin.federation.local-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.federation.local :as local]
            [datalevin.query.source :as spi]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.value :as value]
            [datalevin.util :as u])
  (:import [java.time Duration Instant Period]))

(def descriptor (fake/row-info))
(defn row-facts [rows] (with-meta (fake/row-facts rows) (meta rows)))
(defn pair-source [info rows] (local/source info (row-facts rows)))

(defn category [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(deftest local-queries-match-eager-relations
  (let [rows [[1 "a"] [2 nil] [3 "b"] [4 "a"] [4 "a"]]
        s (pair-source descriptor rows)]
    (doseq [query ['[:find ?id ?v :in $s :where [$s ?id :row/value ?v]]
                   '[:find (count ?v) . :in $s :where [$s _ :row/value ?v]]
                   '[:find (count ?v) . :with ?id :in $s :where [$s ?id :row/value ?v]]
                   '[:find ?a ?b :in $s :where [$s ?a :row/value ?v] [$s ?b :row/value ?v] [(< ?a ?b)]]
                   '[:find ?id :in $s :where [$s ?id :row/value _] [(> ?id 2)]]
                   '{:find [?id ?v] :keys [id value] :in [$s] :where [[$s ?id :row/value ?v]]}]]
      (is (= (d/q query (row-facts rows)) (d/q query s)) (pr-str query)))
    (let [query '[:find [?v ...] :in $s :where [$s _ :row/value ?v] [(not= ?v nil)]]]
      (is (= (frequencies (d/q query (row-facts rows))) (frequencies (d/q query s)))))
    (is (= 2 (count (get-in (d/explain {:run? true}
                                     '[:find ?a ?b :in $s :where [$s ?a :row/value ?v] [$s ?b :row/value ?v]] s)
                            [:regions 0 :steps]))))
    (local/replace-rows! s (row-facts [[9 "new"]]))
    (is (= #{[9]} (d/q '[:find ?id :in $s :where [$s ?id :row/value _]] s)))
    (is (= descriptor (spi/-source-info s)))))

(deftest pinned-aliases-and-native-views
  (let [dir (u/tmp-dir (str "federation-local-" (random-uuid)))
        conn (d/get-conn dir {:id {:db/valueType :db.type/long}})
        s (pair-source descriptor [[1 "old"]])
        changed (atom false)
        wrapped (reify spi/IQuerySource
                  (-source-info [_] (spi/-source-info s))
                  (-open-session [_ request] (spi/-open-session s request))
                  (-plan-source [_ session request]
                    (when (compare-and-set! changed false true)
                      (local/replace-rows! s (row-facts [[2 "new"]]))
                      (d/transact! conn [[:db/retract 1 :id 1] [:db/add 1 :id 2]]))
                    (spi/-plan-source s session request)))
        query '[:find ?id ?v :in $ $a $b
                :where [?e :id ?id] [$a ?id :row/value ?v] [$b ?id :row/value ?v]]]
    (try
      (d/transact! conn [{:db/id 1 :id 1}])
      (is (= #{[1 "old"]} (d/q query @conn wrapped wrapped)))
      (is (= #{[2 "new"]} (d/q query @conn wrapped wrapped)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest local-bounds-and-large-selective-scan
  (let [s (pair-source descriptor (mapv #(vector % "a") (range 20000)))
        query '{:find [?id] :in [$s] :where [[$s ?id :row/value _] [(> ?id 19998)]]
                :query-source {:limits {:retained-bytes 67108864 :spill-bytes 0}}}
        explain (d/explain {:run? true} query s)]
    (is (= #{[19999]} (d/q query s)))
    (is (= 40000 (get-in explain [:cursors 0 :metrics :read-rows])))
    (is (= 5 (get-in explain [:cursors 0 :metrics :batches])))
    (is (= 1 (:result-rows explain)))
    (is (= :query-source/limit
           (category #(d/q (assoc-in query [:query-source :limits :retained-bytes] 1024) s))))
    (is (= :query-source/limit
           (category #(d/q (assoc-in query [:query-source :limits :batch-bytes] 1) s)))))
  (let [s (pair-source descriptor [[1 "a"] [2 "b"] [3 "c"]])
        query '{:find [?id] :in [$s] :where [[$s ?id :row/value _]] :limit 1
                :query-source {:limits {:batch-rows 2}}}
        explain (d/explain {:run? true} query s)]
    (is (= 6 (get-in explain [:cursors 0 :metrics :read-rows])))
    (is (= 2 (get-in explain [:cursors 0 :metrics :batches])))
    (is (= :query-source/limit
           (category #(d/q (-> query (dissoc :limit)
                                (assoc :query-source {:limits {:result-rows 1}})) s))))))

(deftest decimal-join-and-predicate-semantics
  (let [info (fake/row-info :bigdec)
        rows [[1 1.0M] [2 1.00M] [3 2M]]
        s (pair-source info rows)]
    (doseq [query ['[:find ?n :in $s :where [$s _ :row/value ?n]]
                   '[:find ?id :in $s :where [$s ?id :row/value ?n] [(= ?n 1M)]]
                   '[:find ?a ?b :in $s :where [$s ?a :row/value ?n] [$s ?b :row/value ?n]]
                   '[:find (sum ?n) . :with ?id :in $s :where [$s ?id :row/value ?n]]]]
      (is (= (d/q query (row-facts rows)) (d/q query s)) (pr-str query)))))

(defn ref-type [domain]
  {:kind :custom :name :test/entity-ref :version 1 :fingerprint "test-ref:v1"
   :parameters {:domain domain :key-types [:long]}})

(deftest scalar-tuple-collection-and-relation-inputs-match-eager-keys
  (let [rows [[1 1.0M] [2 1.00M] [3 2M]]
        info (assoc-in descriptor [:pattern-contract :attributes :row/value :value-type] :bigdec)
        s (pair-source info rows)]
    (doseq [[binding input] [['?value 1.0M] ['[?value] [1.0M]]
                            ['[?value ...] [1.0M]] ['[[?value]] [[1.0M]]]]]
      (let [q {:find '[?id ?value] :in ['$s binding] :where '[[$s ?id :row/value ?value]]}]
        (is (= #{[1 1.0M]} (d/q q s input)))
        (is (= (d/q q (row-facts rows) input) (d/q q s input)))))))

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
        info (fake/info {:order/id {:entity-type order-type :value-type :long}
                                            :order/customer {:entity-type order-type :value-type customer-type}
                                            :customer/name {:entity-type customer-type :value-type :string}})
        rows [[order :order/id 1] [order :order/customer customer]
              [customer :customer/name "Ada"]]
        s (local/source info (conj rows [(Ref. order-type 2) :order/customer nil]))]
    (is (not= order customer))
    (doseq [query ['[:find ?id ?name :in $s :where [$s ?o :order/id ?id]
                   [$s ?o :order/customer ?c] [$s ?c :customer/name ?name]]
                   '[:find ?o :in $s :where [$s ?o :order/customer _]]]]
      (is (= (d/q query rows) (d/q query s))))
    (is (= :query-source/unsupported
           (category #(d/q '[:find ?o :in $s ?c :where [$s ?o :order/customer ?c]] s order))))))

(deftest immutable-owned-bytes-and-query-equality
  (let [info (fake/row-info :bytes)
        original (byte-array [1 2])
        s (pair-source info [[1 original]])
        fetch (fn []
                (let [control (control/create)
                      request {:control control :spi-version 2 :features #{} :consistency :snapshot
                               :query-id (random-uuid) :authorization {:principal-id :test}
                               :limits (control/limits control) :source-descriptors [info]
                               :region-id 0 :scope-id (get-in info [:scope :id])}
                      session (spi/-open-session s request)]
                  (try
                    (let [response (spi/-plan-source s session
                                     (merge request {:source-descriptor info :source-symbol '$s :bindings {} :available-inputs []
                                                     :clauses [{:id 0 :kind :pattern
                                                                :terms [[:ignore] [:const :row/value] [:var '?v]]}]}))
                          alternative (first (:alternatives response))
                          cursor (spi/-open-cursor session
                                   (contract/cursor-request request '$s
                                     (:definition-fingerprint info) alternative))
                          batch (spi/-next-batch! cursor {:control control :max-rows 1 :max-bytes 10000})]
                      (try (ffirst (:rows batch))
                           (finally (spi/release! (:reservation batch)) (spi/-close-cursor! cursor))))
                    (finally (spi/-close-session! session) (control/close! control)))))]
    (aset-byte original 0 (byte 9))
    (let [first-result (fetch)]
      (is (= [1 2] (vec first-result)))
      (aset-byte first-result 0 (byte 8))
      (is (= [1 2] (vec (fetch)))))
    (let [q '[:find ?v . :in $s :where [$s _ :row/value ?v]]
          result (d/q q s)]
      (is (= [1 2] (vec result)))
      (aset-byte result 0 (byte 7))
      (is (= [1 2] (vec (d/q q s)))))))


(deftest immutable-time-data-matches-eager-and-native-queries
  (let [info (assoc-in descriptor [:pattern-contract :attributes :row/value :value-type] :data)
        rows [[1 (Instant/ofEpochSecond -1 123456789)]
              [2 (Instant/ofEpochSecond -1 123456790)]
              [3 (Duration/ofSeconds -1 123456789)]
              [4 (Period/of 1 0 0)] [5 (Period/of 0 12 0)]]
        s (pair-source info (conj rows (first rows)))
        dir (u/tmp-dir (str "federation-time-" (random-uuid)))
        conn (d/create-conn dir {:event/time {}}
                            {:wal? false :background-sampling? false})]
    (try
      (doseq [q ['{:find [?id ?v] :in [$s] :where [[$s ?id :row/value ?v]]
                   :query-source {:limits {:batch-rows 2}}}
                 '[:find (count ?v) . :with ?id :in $s :where [$s ?id :row/value ?v]]
                 '[:find ?a ?b :in $s :where [$s ?a :row/value ?v] [$s ?b :row/value ?v]]]]
        (is (= (d/q q (row-facts (conj rows (first rows)))) (d/q q s))))
      (d/transact! conn (mapv (fn [[e v]] {:db/id e :event/time v}) rows))
      (let [q '[:find ?e ?v :in $ $s :where [?e :event/time ?v] [$s ?e :row/value ?v]]]
        (is (= (set rows) (d/q q @conn (row-facts rows)) (d/q q @conn s))))
      (is (= #{[1]} (d/q '[:find ?e :in $s ?v :where [$s ?e :row/value ?v]]
                         s (second (first rows)))))
      (local/replace-rows! s (row-facts [[1 Duration/ZERO]]))
      (is (= #{[1 Duration/ZERO]} (d/q '[:find ?e ?v :in $s :where [$s ?e :row/value ?v]] s)))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest local-byte-values-use-shared-query-equality
  (doseq [[type wrap] [[:bytes identity]
                       [:data #(vector % :tag)]
                       [:data #(hash-map :blob %)]
                       [:data #(hash-map % :value)]
                       [:data #(hash-set % :tag)]]
          :let [a (wrap (byte-array [0 31])) b (wrap (byte-array [1 0]))
                rows [[1 a] [2 (value/owned-copy a)] [3 b]]
                source (pair-source (assoc-in descriptor [:pattern-contract :attributes :row/value :value-type] type) rows)]]
    (doseq [q ['[:find ?id :in $s ?v :where [$s ?id :row/value ?v]]
               '[:find ?id :in $s ?v :where [$s ?id :row/value ?x] [(= ?x ?v)]]]
            :let [input (value/owned-copy a)]]
      (is (= #{[1] [2]} (d/q q source input)))
      (is (= (d/q q (row-facts rows) input) (d/q q source input))))
    (let [q '[:find ?a ?b :in $s :where [$s ?a :row/value ?v] [$s ?b :row/value ?v]]]
      (is (= (d/q q (row-facts rows)) (d/q q source))))
    (is (= 2 (d/q '[:find (count ?v) . :in $s :where [$s _ :row/value ?v]] source)))
    (is (= 3 (d/q '[:find (count ?v) . :with ?id :in $s :where [$s ?id :row/value ?v]] source)))
    (let [q {:find '[?id] :in '[$s] :where [['$s '?id :row/value (value/owned-copy a)]]}]
      (is (= #{[1] [2]} (d/q q source))))))
