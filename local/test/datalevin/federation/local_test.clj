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

(defn- clause-permutations [clauses]
  (if (empty? clauses) [[]]
      (mapcat (fn [i]
                (map #(into [(nth clauses i)] %)
                     (clause-permutations (into (subvec clauses 0 i) (subvec clauses (inc i))))))
              (range (count clauses)))))

(deftest mixed-source-clause-order-is-independent
  (let [facts (row-facts [[1 "one"] [2 "two"]])
        provider (local/source descriptor facts {:mode :lookup})
        seed [[1] [2] [99]]
        clauses '[[$seed ?raw] [(identity ?raw) ?first] [(identity ?first) ?second]
                  [(identity ?second) ?id] [$s ?id :row/value ?value]]]
    (doseq [where (clause-permutations clauses)]
      (let [query {:find '[?id ?value] :in '[$seed $s] :where where}]
        (is (= #{[1 "one"] [2 "two"]}
               (d/q query seed facts)
               (d/q query seed provider)) (pr-str where))))))

(deftest mixed-source-rule-and-union-clause-order-is-independent
  (let [facts (row-facts [[1 "one"] [2 "two"]])
        provider (local/source descriptor facts {:mode :lookup})
        seed [[1] [2] [99]]
        clauses '[[$seed ?raw] [(identity ?raw) ?first] [(ground ?first) ?second]
                  [(identity ?second) ?id] [?id :row/value ?value]]
        rule-query '[:find ?id ?value :in $seed $s % :where ($s named ?id ?value)]
        union-query '[:find ?id ?value :in $seed $s % :where ($s chosen ?id ?value)]]
    (doseq [where (clause-permutations clauses)]
      (let [rules [(into '[(named ?id ?value)] where)
                   '[(chosen ?id ?value)
                     (or-join [?id ?value] (named ?id ?value) (named ?id ?value))]]
            inline {:find '[?id ?value] :in '[$seed $s]
                    :where (mapv #(if (= '?id (first %)) (into ['$s] %) %) where)}
            expected (d/q inline seed facts)]
        (doseq [query [rule-query union-query]]
          (is (= #{[1 "one"] [2 "two"]}
                 expected
                 (d/q query seed provider rules)) (pr-str [query where])))))))

(deftest mixed-source-copy-cycles-retain-caller-types
  (let [facts (row-facts [[1 "one"] [2 "two"]])
        caller (local/source descriptor facts {:mode :scan})
        callee (local/source (assoc descriptor :source-instance-id (random-uuid)) facts {:mode :lookup})
        query '[:find ?copy ?value ?actual :in $caller $s % ?key
                :where ($caller copied ?key ?copy ?value) [$s ?copy :row/value ?actual]]]
    (doseq [copies ['[[(identity ?key) ?key] [(identity ?key) ?copy]]
                    '[[(identity ?key) ?copy] [(identity ?copy) ?key]]]
            where (clause-permutations (into '[[?e :row/value ?value]] copies))]
      (let [rules [(into '[(copied ?key ?copy ?value)] where)]]
        (is (= #{[1 "one" "one"] [1 "two" "one"]}
               (d/q query caller callee rules 1)) (pr-str where))))))

(deftest native-database-and-federated-clause-order-is-independent
  (let [dir (u/tmp-dir (str "federation-clause-order-" (random-uuid)))
        conn (d/get-conn dir {:native/key {:db/valueType :db.type/long}})
        facts (row-facts [[1 "one"] [2 "two"]])
        provider (local/source descriptor facts {:mode :lookup})
        clauses '[[?e :native/key ?raw] [(identity ?raw) ?first] [(identity ?first) ?second]
                  [(identity ?second) ?id] [$s ?id :row/value ?value]]]
    (try
      (d/transact! conn [{:native/key 1} {:native/key 2} {:native/key 99}])
      (doseq [where [clauses (vec (reverse clauses))]]
        (let [query {:find '[?id ?value] :in '[$ $s] :where where}]
          (is (= #{[1 "one"] [2 "two"]}
                 (d/q query @conn facts)
                 (d/q query @conn provider)) (pr-str where))))
      (finally (d/close conn) (u/delete-files dir)))))

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

(deftest rule-calls-receive-federated-caller-bindings
  (let [rows [[1 "one"] [2 "two"]]
        facts (row-facts rows)
        callee-info (assoc descriptor :source-instance-id (random-uuid))
        rules '[[(named ?entity ?label) [?entity :row/value ?label]]
                [(wrapped ?label ?entity) (named ?entity ?label)]]]
    (doseq [[caller-mode callee-mode] [[:scan :lookup] [:both :lookup]
                                     [:lookup :scan] [:lookup :both]]
            call '[($s named ?id ?value) ($s wrapped ?value ?id)
                   (or-join [?id ?value] ($s named ?id ?value) ($s wrapped ?value ?id))]
            caller-clause '[[$caller ?id :row/value _] ($caller named ?id _)]
            where [[caller-clause call] [call caller-clause]]]
      (let [caller (local/source descriptor facts {:mode caller-mode})
            callee (local/source callee-info facts {:mode callee-mode})
            query {:find '[?id ?value] :in '[$caller $s %] :where where}]
        (is (= #{[1 "one"] [2 "two"]}
               (d/q query facts facts rules)
               (d/q query caller callee rules))
            (pr-str [caller-mode callee-mode where]))))))

(deftest federated-rule-bindings-preserve-correlations-and-witnesses
  (let [facts (row-facts [[1 "one"] [2 "two"] [3 "three"]])
        caller (pair-source descriptor [[1 "one"] [2 "wrong"] [3 "three"]])
        callee (local/source (assoc descriptor :source-instance-id (random-uuid)) facts {:mode :lookup})
        rules '[[(named ?entity ?label) [?entity :row/value ?label]]
                [(named ?other ?text) [?other :row/value ?text]]]
        query '{:find [?id ?value ?tag] :in [$caller $s % [[?tag]]]
                :where [[$caller ?id :row/value ?value] ($s named ?id ?value)]}
        tags [[:a] [:b] [:a]]]
    (is (= #{[1 "one" :a] [1 "one" :b] [3 "three" :a] [3 "three" :b]}
           (d/q query caller callee rules tags)))
    (is (= 4 (:result-rows (d/explain {:run? true} query caller callee rules tags))))
    (let [count-query (assoc query :find '[(count ?id) .] :with '[?tag])]
      (is (= (d/q count-query (row-facts [[1 "one"] [2 "wrong"] [3 "three"]]) facts rules tags)
             (d/q count-query caller callee rules tags))))
    (is (= #{[1 "one" :a] [1 "one" :b]}
           (d/q (update query :where conj '(not [$s ?id :row/value "three"]))
                caller callee rules tags)))))

(deftest federated-rules-require-a-real-caller-producer
  (let [facts (row-facts [[1 "one"]])
        caller (local/source descriptor facts {:mode :lookup})
        callee (local/source (assoc descriptor :source-instance-id (random-uuid)) facts {:mode :lookup})
        query '[:find ?id ?value :in $caller $s %
                :where [$caller ?id :row/value _] ($s named ?id ?value)]]
    (is (= :query-source/unsupported
           (category #(d/q query caller callee '[[(named ?entity ?label) [?entity :row/value ?label]]]))))))

(deftest federated-rules-can-bind-only-part-of-their-head
  (let [caller (local/source descriptor (row-facts [[1 "one"]]) {:mode :scan})
        callee (local/source (assoc descriptor :source-instance-id (random-uuid))
                            (row-facts [[1 "one"] [2 "two"]]) {:mode :lookup})
        consumer (local/source (assoc descriptor :source-instance-id (random-uuid))
                              (row-facts [[10 "one"]]) {:mode :value})
        query '[:find ?id ?other ?value :in $caller $s $consumer %
                :where [$caller ?id :row/value _] ($s named ?id ?value)
                [$consumer ?other :row/value ?value]]]
    (is (= #{[1 10 "one"]}
           (d/q query caller callee consumer '[[(named ?entity ?label) [?entity :row/value ?label]]])))))

(deftest federated-rule-inputs-survive-caller-projection
  (let [facts (row-facts [[1 "one"] [2 "two"]])
        caller (local/source descriptor facts {:mode :scan})
        requests (atom [])
        observed (reify spi/IQuerySource
                   (-source-info [_] (spi/-source-info caller))
                   (-open-session [_ request] (spi/-open-session caller request))
                   (-plan-source [_ session request]
                     (swap! requests conj (select-keys request [:required-vars]))
                     (spi/-plan-source caller session request)))
        callee (local/source (assoc descriptor :source-instance-id (random-uuid)) facts {:mode :lookup})
        query '[:find ?value :in $caller $s %
                :where [$caller ?id :row/value _] ($s named ?id ?value)]]
    (is (= #{["one"] ["two"]}
           (d/q query observed callee '[[(named ?entity ?label) [?entity :row/value ?label]]])))
    (is (= [#{'?id}] (mapv :required-vars @requests)))))

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

(deftest bound-absence-keeps-missing-entities-and-private-locals
  (let [rows [[1 "present"] [2 nil] [3 ""]]
        provider (pair-source descriptor rows)
        q '[:find ?e ?private :in $s [[?e ?private]]
            :where (not-join [?e] [$s ?e :row/value ?private])]
        inputs [[1 :outer] [2 :outer] [2 :outer] [3 :outer] [999 :missing]]]
    (is (= #{[2 :outer] [999 :missing]}
           (d/q q (row-facts rows) inputs) (d/q q provider inputs)))))
