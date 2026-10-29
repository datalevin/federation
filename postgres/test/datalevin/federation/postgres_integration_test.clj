(ns datalevin.federation.postgres-integration-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.install :as install]
            [datalevin.federation.mapping :as mapping]
            [datalevin.federation.postgres :as pg]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres-test :as unit]
            [datalevin.query.source :as spi]
            [datalevin.query.source.batch :as batch]
            [datalevin.query.source.conformance :as conformance]
            [datalevin.query.source.control :as control]
            [datalevin.util :as u])
  (:import [java.lang.reflect Proxy InvocationHandler InvocationTargetException Method]
           [java.sql Connection DriverManager]
           [java.time Instant]
           [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]
           [javax.sql DataSource ConnectionEventListener]
           [org.postgresql.ds PGConnectionPoolDataSource]
           [datalevin.utl QueryKey]))

(def ^:dynamic *db* nil)
(def ^:dynamic *schema* nil)
(def ^:dynamic *profile* nil)
(def ^:dynamic *accepted* nil)

(defn sql! [sql]
  (with-open [stmt (.createStatement ^Connection *db*)] (.execute stmt sql)))
(defn table [name] (jdbc/relation [*schema* name]))
(defn error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn category [f] (:error (error f)))
(defn source
  ([] (source {}))
  ([options] (pg/source *accepted* (merge *profile* options))))

(def records
  [{:order/id 1 :order/title "Alpha" :order/status "paid" :order/amount 10.2500M :order/owner 7
    :order/stamp (Instant/parse "2026-10-20T12:34:56.123456Z")}
   {:order/id 2 :order/title "βeta" :order/status "paid" :order/owner 8 :order/note "present"}
   {:order/id 3 :order/title "x'; DROP TABLE orders; --" :order/status "held" :order/amount 30.0000M}
   {:order/id 4 :order/status "paid" :order/amount 10.2500M :order/owner 7 :order/note "present"}])

(defn eager []
  (vec (mapcat #(mapping/project-facts (mapping/compile-logical-mapping *accepted*)
                                     [(:order/id %)] (dissoc % :order/id)) records)))

(defn with-database [f]
  (let [url (or (System/getenv "POSTGRES_TEST_URL")
                (throw (ex-info "Set POSTGRES_TEST_URL to a disposable PostgreSQL database" {})))
        schema (str "federation_" (.replace (str (UUID/randomUUID)) "-" ""))
        profile {:jdbc-url url :compute-context-id :tests/postgres :fetch-size 2}]
    (with-open [conn (DriverManager/getConnection url)]
      (binding [*db* conn *schema* schema *profile* profile]
        (sql! (str "CREATE SCHEMA " (jdbc/identifier schema)))
        (try
          (sql! (str "CREATE TABLE " (table "orders")
                     " (id bigint PRIMARY KEY, title text, status text, amount numeric(18,4), "
                     "owner bigint, note text, stamp timestamptz, unsupported jsonb)"))
          (sql! (str "INSERT INTO " (table "orders") " (id,title,status,amount,owner,note,stamp) VALUES "
                     "(1,'Alpha','paid',10.25,7,NULL,'2026-10-20 12:34:56.123456+00'),"
                     "(2,'βeta','paid',NULL,8,'present',NULL),"
                     "(3,'x''; DROP TABLE orders; --','held',30,NULL,NULL,NULL),"
                     "(4,NULL,'paid',10.25,7,'present',NULL)"))
          (let [catalog (pg/discover profile [schema "orders"])
                accepted (pg/accept-mapping catalog
                           {:source-instance-id unit/source-id :relation-id :sales/orders
                            :identity-attribute :order/id
                            :attributes {:order/title "title" :order/status "status" :order/amount "amount"
                                         :order/owner "owner" :order/note "note" :order/stamp "stamp"}})]
            (binding [*accepted* accepted] (f)))
          (finally (sql! (str "DROP SCHEMA " (jdbc/identifier schema) " CASCADE"))))))))

(use-fixtures :each with-database)

(defn query [find where]
  {:find find :in '[$p] :where where :timeout 10000
   :query-source {:limits {:batch-rows 1 :binding-rows 1}}})

(defn read-rows [explanation]
  (reduce + (map #(get-in % [:metrics :read-rows]) (:cursors explanation))))
(defn relational-plan? [explanation]
  (some #(= :relational-v1 (:policy %)) (:regions explanation)))

(deftest decimal-and-temporal-filters-reduce-transfer-in-ordinary-queries
  (sql! (str "CREATE TABLE " (table "filtered") " (id bigint PRIMARY KEY, amount numeric(18,4), stamp timestamptz)"))
  (sql! (str "INSERT INTO " (table "filtered")
             " SELECT i,i::numeric/10,timestamptz '2026-01-01 00:00:00+00' + i * interval '1 microsecond'"
             " FROM generate_series(1,1000) i"))
  (let [accepted (pg/accept-mapping (pg/discover *profile* [*schema* "filtered"])
                   {:source-instance-id unit/source-id :relation-id :test/filtered :identity-attribute :row/id
                    :attributes {:row/amount "amount" :row/stamp "stamp"}})
        compiled (mapping/compile-logical-mapping accepted)
        base (Instant/parse "2026-01-01T00:00:00Z")
        facts (vec (mapcat #(mapping/project-facts compiled [%]
                             {:row/amount (.setScale (.divide (bigdec %) 10M) 4)
                              :row/stamp (.plusNanos base (* 1000 %))}) (range 1 1001)))
        provider (pg/source accepted (dissoc *profile* :fetch-size))
        upper (.plusNanos base 10500) lower (.plusNanos base 990500)
        cases [[:row/id :> 990 10] [:row/amount :> 99M 10]
               [:row/stamp :> lower 10] [:row/stamp :>= lower 10]
               [:row/stamp :< upper 10] [:row/stamp :<= upper 10]
               [:row/stamp := upper 0]]]
    (doseq [[attribute operator cutoff n] cases
            reverse? [false true]]
      (let [symbol ({:> '> :>= '>= :< '< :<= '<= := '=} operator)
            predicate (if reverse? (list ({'> '< '>= '<= '< '> '<= '>= '= '=} symbol) cutoff '?value)
                          (list symbol '?value cutoff))
            q {:find '[?id] :in '[$p] :timeout 30000
               :where [['$p '?e :row/id '?id] ['$p '?e attribute '?value] [predicate]]}
            explained (d/explain {:run? true} q provider)]
        (is (= (set (d/q q facts)) (set (d/q q provider))) (pr-str [attribute operator reverse?]))
        (is (= n (:result-rows explained)))
        (is (= n (read-rows explained)))
        (is (some #(seq (:prefilters %)) (mapcat :steps (:regions explained))))))
    (let [q {:find '[?id] :in '[$p ?cutoff] :timeout 30000
             :where '[[$p ?e :row/id ?id] [$p ?e :row/amount ?amount] [(> ?amount ?cutoff)]]}]
      (is (= (set (d/q q facts 99M)) (set (d/q q provider 99M)))))
    (let [q {:find '[?id] :in '[$p] :timeout 30000
             :where '[[$p ?e :row/id ?id] [$p ?e :row/amount ?amount] [(> ?amount 99.0)]]}
          explained (d/explain {:run? true} q provider)]
      (is (= (set (d/q q facts)) (set (d/q q provider))))
      (is (= 1000 (read-rows explained))))
    (is (zero? (:live-sessions (pg/statistics provider))))))

(deftest delegated-aggregates-preserve-find-with-and-empty-input-semantics
  (let [queries
        ['[:find (count ?status) :in $p :where [$p ?e :order/status ?status]]
         '[:find (count ?status) :with ?e :in $p :where [$p ?e :order/status ?status]]
         '[:find (count-distinct ?status) :with ?e :in $p :where [$p ?e :order/status ?status]]
         '[:find ?status (count ?e) :in $p :where [$p ?e :order/status ?status]]
         '[:find (sum ?amount) :in $p :where [$p ?e :order/amount ?amount]]
         '[:find (sum ?amount) :with ?e :in $p :where [$p ?e :order/amount ?amount]]
         '[:find (min ?amount) (max ?amount) :in $p :where [$p ?e :order/amount ?amount]]
         '[:find (sum ?id) :in $p :where [$p ?e :order/id ?id] [(>= ?id 0)]]
         '[:find (count ?e) :in $p :where [$p ?e :order/id ?id] [(> ?id 99)]]
         '[:find (count ?e) . :in $p :where [$p ?e :order/id ?id]]
         '[:find [(count ?status) ...] :in $p :where [$p ?e :order/status ?status]]
         '[:find [(count ?e) (sum ?amount)] :in $p :where [$p ?e :order/amount ?amount]]
         {:find '[?status (count ?e)] :in '[$p] :order-by '[1 :desc ?status :asc] :limit 1
          :where '[[$p ?e :order/status ?status]]}
         {:find '[?status (count ?e)] :keys '[status total] :in '[$p]
          :where '[[$p ?e :order/status ?status]]}]]
    (doseq [q queries]
      (let [provider (source) explained (d/explain {:run? true} q provider)
            actual (d/q q provider) expected (d/q q (eager))]
        (is (if (or (set? expected) (seq? expected))
              (and (= (count expected) (count actual))
                   (every? (fn [row] (some #(QueryKey/equals row %) actual)) expected))
              (QueryKey/equals expected actual)) (pr-str [q expected actual]))
        (is (relational-plan? explained) (pr-str q))
        (is (<= (read-rows explained) 2))
        (is (zero? (:live-sessions (pg/statistics provider))))))))

(deftest relational-distinctness-preserves-decimal-scale-and-repeated-variables
  (sql! (str "CREATE TABLE " (table "decimal_keys") " (id bigint PRIMARY KEY, a numeric, b numeric)"))
  (sql! (str "INSERT INTO " (table "decimal_keys") " VALUES (1,1.0,1.00),(2,1.00,1.0),(3,2.0,2.0)"))
  (let [accepted (pg/accept-mapping (pg/discover *profile* [*schema* "decimal_keys"])
                   {:source-instance-id unit/source-id :relation-id :test/decimal-keys :identity-attribute :row/id
                    :attributes {:row/a "a" :row/b "b"}})
        compiled (mapping/compile-logical-mapping accepted)
        facts (vec (mapcat (fn [[id a b]] (mapping/project-facts compiled [id] {:row/a a :row/b b}))
                           [[1 1.0M 1.00M] [2 1.00M 1.0M] [3 2.0M 2.0M]]))
        queries ['[:find ?a (count ?e) :in $p :where [$p ?e :row/a ?a]]
                 '[:find (count ?a) (count-distinct ?a) (sum ?a) :in $p :where [$p ?e :row/a ?a]]
                 '[:find ?a ?x ?y :in $p :where [$p ?x :row/a ?a] [$p ?y :row/b ?a]]
                 '[:find ?a ?b :in $p :where [$p ?x :row/a ?a] [$p ?y :row/b ?b] [(= ?a ?b)]]]]
    (doseq [q queries]
      (let [provider (pg/source accepted *profile*) expected (d/q q facts) actual (d/q q provider)]
        (is (= (count expected) (count actual)))
        (is (every? (fn [row] (some #(QueryKey/equals row %) actual)) expected))
        (is (relational-plan? (d/explain {:run? false} q provider)))))
    (let [q '[:find (min ?a) :in $p :where [$p ?e :row/a ?a]]]
      (is (not (relational-plan? (d/explain {:run? false} q (pg/source accepted *profile*))))))))

(deftest delegated-aggregates-enforce-input-row-and-conversion-limits
  (sql! (str "UPDATE " (table "orders") " SET title=repeat('x',100000) WHERE id=1"))
  (let [provider (source {:max-row-bytes 128})
        q '[:find (count ?title) :in $p :where [$p ?e :order/title ?title]]]
    (is (= :query-source/limit (category #(d/q q provider))))
    (is (zero? (:live-sessions (pg/statistics provider)))))
  (sql! (str "UPDATE " (table "orders") " SET amount='NaN'::numeric WHERE id=1"))
  (let [provider (source) q '[:find (sum ?amount) :in $p :where [$p ?e :order/amount ?amount]]]
    (is (= :query-source/invalid (category #(d/q q provider))))
    (is (zero? (:live-sessions (pg/statistics provider))))))

(deftest cross-table-joins-and-aggregates-share-the-acquired-snapshot
  (sql! (str "CREATE TABLE " (table "customers") " (id bigint PRIMARY KEY, name text)"))
  (sql! (str "INSERT INTO " (table "customers") " VALUES (7,'Ada'),(8,'Bo')"))
  (let [customers (pg/accept-mapping (pg/discover *profile* [*schema* "customers"])
                    {:source-instance-id unit/source-id :relation-id :sales/customers :identity-attribute :customer/id
                     :attributes {:customer/name "name"}})
        type (get-in customers [:identity :type])
        orders (pg/accept-mapping (pg/discover *profile* [*schema* "orders"])
                 {:source-instance-id unit/source-id :relation-id :sales/linked :identity-attribute :order/id
                  :attributes {:order/customer {:column "owner" :reference {:type type :domain (get-in type [:parameters :identity-domain-id])}}}})
        order-facts (vec (mapcat #(mapping/project-facts (mapping/compile-logical-mapping orders) [(:order/id %)]
                                  {:order/customer (when (:order/owner %) (ref/entity-ref type [(:order/owner %)]))}) records))
        customer-facts (vec (mapcat (fn [[id name]]
                                     (mapping/project-facts (mapping/compile-logical-mapping customers) [id] {:customer/name name}))
                                   [[7 "Ada"] [8 "Bo"]]))
        where '[[$o ?order :order/id ?id] [$o ?order :order/customer ?customer]
                [$c ?customer :customer/name ?name]]]
    (doseq [find ['[?id ?customer ?name] '[?name (count ?order)]]]
      (let [q {:find find :in '[$o $c] :where where}
            orders (pg/source orders *profile*) customers (pg/source customers *profile*)
            explained (d/explain {:run? true} q orders customers)]
        (is (= (set (d/q q order-facts customer-facts)) (set (d/q q orders customers))))
        (is (relational-plan? explained))
        (is (= 1 (count (set (map :session-id (:sources explained))))))
        (is (= 1 (count (:cursors explained))))
        (is (<= (read-rows explained) 3))
        (is (zero? (:live-sessions (pg/statistics orders))))))))

(deftest fused-separate-and-eager-queries-have-the-same-complete-results
  (let [queries
        [(query '[?id ?title ?amount] '[[$p ?e :order/id ?id] [$p ?e :order/title ?title] [$p ?e :order/amount ?amount]])
         (query '[?id] '[[$p ?e :order/id ?id] [$p ?e :order/title _] [$p ?e :order/note _]])
         (query '[?title] '[[$p ?e :order/status "paid"] [$p ?e :order/title ?title]])
         (query '[?amount] '[[$p ?e :order/title "Alpha"] [$p ?e :order/amount ?amount]])
         (query '[?id ?stamp] '[[$p ?e :order/id ?id] [$p ?e :order/stamp ?stamp]])
         (query '[?a ?b] '[[$p ?x :order/owner ?owner] [$p ?y :order/owner ?owner]
                            [$p ?x :order/id ?a] [$p ?y :order/id ?b]])
         (query '[?id ?v] '[[$p ?e :order/id ?id] [$p ?e :order/title ?v] [$p ?e :order/status ?v]])
         (query '[?id] '[[$p ?e :order/id ?id] [$p ?e :order/amount 10.25M]])
         (query '[?title] '[[$p ?e :order/id 1N] [$p ?e :order/title ?title]])
         (query '[?title] '[[$p ?e :order/id 1.5] [$p ?e :order/title ?title]])
         (query '[?id] '[[$p ?e :order/id ?id] [(> ?id 2)] [$p ?e :order/status "paid"]])
         (assoc (query '[(count ?status)] '[[$p ?e :order/status ?status] [$p ?e :order/amount _]]) :with '[?e])]]
    (doseq [q queries fusion? [false true] fetch-size [1 3]]
      (let [provider (source {:fusion? fusion? :fetch-size fetch-size})]
        (is (= (set (d/q (dissoc q :query-source) (eager))) (set (d/q q provider))) (pr-str [fusion? q]))
        (is (zero? (:live-sessions (pg/statistics provider))))))))

(deftest explain-shows-fusion-and-fewer-round-trips
  (let [q (query '[?id ?title ?amount] '[[$p ?e :order/id ?id] [$p ?e :order/title ?title] [$p ?e :order/amount ?amount]])
        fused (d/explain {:run? true} q (source))
        separate (d/explain {:run? true} q (source {:fusion? false}))
        steps (mapcat :steps (:regions fused))
        calls #(reduce + (map (comp :external-requests :metrics) (:cursors %)))]
    (is (some #(= 3 (count (:covers %))) steps))
    (is (< (calls fused) (calls separate)))
    (is (= (:result-rows fused) (:result-rows separate) 2))
    (is (every? #(= :snapshot (:consistency %)) (:sources fused)))
    (is (not (.contains (pr-str fused) "DROP TABLE")))
    (is (not (.contains (pr-str fused) "jdbc:postgresql")))))

(deftest wide-two-entity-queries-keep-complete-plans-under-the-candidate-cap
  (let [attributes (into {} (map #(vector (keyword "row" (str "value" %)) "title")) (range 11))
        accepted (pg/accept-mapping (pg/discover *profile* [*schema* "orders"])
                   {:source-instance-id unit/source-id :relation-id :test/wide
                    :identity-attribute :row/id :attributes attributes})
        compiled (mapping/compile-logical-mapping accepted)
        facts (vec (mapcat #(mapping/project-facts compiled [(:order/id %)]
                             (zipmap (keys attributes) (repeat (:order/title %)))) records))
        entities (mapv #(ref/entity-ref (get-in accepted [:identity :type]) [%]) [1 2 4 99])
        where (conj (mapv (fn [i] ['$p '?e (keyword "row" (str "value" i))
                                  (symbol (str "?v" i))]) (range 11))
                    '[$p ?other :row/id ?other-id])
        q (assoc (query '[?v0 ?other-id] where) :in '[$p [?e ...]])
        expected (set (d/q (dissoc q :query-source) facts entities))]
    (is (= 8 (count expected)))
    (doseq [fusion? [false true]
            cap [nil (if fusion? 2 12)]]
      (let [provider (pg/source accepted (assoc *profile* :fusion? fusion?))
            q (cond-> q cap (assoc-in [:query-source :planning :max-alternatives] cap))
            explained (d/explain {:run? true} q provider entities)]
        (is (= expected (set (d/q q provider entities))) (pr-str [fusion? cap]))
        (is (= (count expected) (:result-rows explained)))
        (is (= 12 (count (set (mapcat :covers (mapcat :steps (:regions explained)))))))
        (is (zero? (:live-sessions (pg/statistics provider))))))
    (let [provider (pg/source accepted *profile*)]
      (is (= :query-source/limit
             (category #(d/q (assoc-in q [:query-source :planning :max-alternatives] 1) provider entities))))
      (is (zero? (:live-sessions (pg/statistics provider)))))))

(deftest dependent-inputs-entities-native-joins-and-rules
  (let [q (assoc (query '[?id ?title ?witness]
                        '[[$p ?e :order/id ?id] [$p ?e :order/title ?title]])
                 :in '[$p [[?id ?witness]]])
        inputs [[1 :a] [1 :b] [2 :c] [99 :d]]]
    (doseq [fusion? [false true]]
      (is (= (set (d/q (dissoc q :query-source) (eager) inputs))
             (set (d/q q (source {:fusion? fusion?}) inputs)))))
    (let [type (get-in *accepted* [:identity :type])
          entity-query '[:find ?title :in $p ?e :where [$p ?e :order/title ?title]]]
      (is (= #{["Alpha"]} (set (d/q entity-query (source) (ref/entity-ref type [1]))))))
    (let [conn (d/create-conn nil {:triage/id {:db/valueType :db.type/long}
                                  :triage/priority {:db/valueType :db.type/long}}
                              {:kv-opts {:inmemory? true} :background-sampling? false})]
      (try
        (d/transact! conn [{:triage/id 1 :triage/priority 10} {:triage/id 2 :triage/priority 1}])
        (is (= #{["Alpha"]}
               (set (d/q '[:find ?title :in $ $p :where
                           [?t :triage/id ?id] [?t :triage/priority ?priority] [(> ?priority 9)]
                           [$p ?e :order/id ?id] [$p ?e :order/title ?title]] @conn (source)))))
        (finally (d/close conn))))
    (let [rules '[[(paid ?e ?title) [$p ?e :order/status "paid"] [$p ?e :order/title ?title]]]
          q '[:find ?title :in $p % :where (paid ?e ?title)]]
      (is (= (set (d/q q (eager) rules)) (set (d/q q (source) rules)))))))

(defn request [provider ctl]
  {:control ctl :query-id (random-uuid) :spi-version 2 :features #{} :consistency :snapshot
   :limits (control/limits ctl) :authorization nil :source-descriptors [(spi/-source-info provider)]})
(defn alternatives [provider session request clauses available]
  (:alternatives
    (spi/-plan-source provider session
      (merge request {:source-symbol '$p :region-id 0 :scope-id (get-in (spi/-source-info provider) [:scope :id])
                      :source-descriptor (spi/-source-info provider) :bindings {} :available-inputs available
                      :max-alternatives 32 :clauses clauses}))))
(defn open [provider session request alternative bindings]
  (spi/-open-cursor session
    (cond-> (merge (select-keys request [:control :query-id :spi-version :features :authorization :consistency :limits])
                   {:source-symbol '$p :region-id 0 :scope-id (get-in (spi/-source-info provider) [:scope :id])
                    :definition-fingerprint (:definition-fingerprint (spi/-source-info provider))
                    :alternative-id (:id alternative) :plan (:plan alternative)})
      bindings (assoc :bindings bindings))))
(defn consume [cursor ctl alternative dependent?]
  (loop [all [] ids []]
    (let [fetch {:control ctl :max-rows 1 :max-bytes 1048576}
          raw (spi/-next-batch! cursor fetch)
          accepted (batch/accept! {:control ctl} (:columns alternative) dependent? fetch raw identity)]
      (spi/release! (:reservation accepted))
      (if (:exhausted? accepted)
        {:rows (into all (:rows accepted)) :ids (into ids (:binding-ids accepted))}
        (recur (into all (:rows accepted)) (into ids (:binding-ids accepted)))))))
(defn released? [ctl] (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))

(deftest snapshot-replay-planning-and-binding-ids
  (let [provider (source) ctl (control/create) request (request provider ctl)
        session (spi/-open-session provider request)
        type (get-in *accepted* [:identity :type])
        clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:var ?title]]}]
        available [{:variable '?e :type type :nullable? false}]
        inputs [{:binding-id 91 :values [(ref/entity-ref type [1])]}
                {:binding-id 92 :values [(ref/entity-ref type [2])]}
                {:binding-id 93 :values [(ref/entity-ref type [99])]}]]
    (try
      (let [before (:external-requests (control/usage ctl))
            alternative (first (alternatives provider session request clauses available))]
        (is (= before (:external-requests (control/usage ctl))))
        (sql! (str "UPDATE " (table "orders") " SET title='changed' WHERE id=1"))
        (dotimes [_ 2]
          (let [cursor (open provider session request alternative inputs)]
            (try
              (let [{:keys [rows ids]} (consume cursor ctl alternative true)]
                (is (= [91 92] ids))
                (is (= ["Alpha" "βeta"] (mapv second rows))))
              (finally (spi/-close-cursor! cursor) (spi/-close-cursor! cursor)))))
        (is (= :query-source/stale-definition
               (category #(spi/-open-cursor session {:definition-fingerprint "stale"})))))
      (finally (spi/-close-session! session) (spi/-close-session! session)))
    (is (released? ctl))
    (is (zero? (:live-sessions (pg/statistics provider))))
    (control/close! ctl)))

(deftest batched-lookups-preserve-binding-ids-and-equality
  (let [entity-type (get-in *accepted* [:identity :type])
        entity #(ref/entity-ref entity-type [%])
        stamp (:order/stamp (first records))]
    (doseq [[attribute variable type values]
            [[:order/title '?value :string ["βeta" "Alpha" "missing"]]
             [:order/status '?value :string ["paid" "held" "missing"]]
             [:order/amount '?value :bigdec [10.2500M 10.25M 99M 30.0000M]]
             [:order/stamp '?value :data [stamp (.plusNanos ^Instant stamp 1000)]]
             [:order/title '?e entity-type [(entity 2) (entity 1) (entity 99)]]]]
      (let [provider (source {:fetch-size 2}) ctl (control/create) request (request provider ctl)
            session (spi/-open-session provider request)
            clauses [{:id 0 :kind :pattern :terms [[:var '?e] [:const attribute] [:var '?value]]}
                     {:id 1 :kind :pattern :terms '[[:var ?e] [:const :order/id] [:var ?id]]}]
            available [{:variable variable :type type :nullable? false}]
            alternative (first (alternatives provider session request clauses available))
            inputs (mapv (fn [i x] {:binding-id (+ 90 i) :values [x]}) (range) values)
            q {:find '[?witness ?id] :in ['$p [[variable '?witness]]]
               :where [['$p '?e attribute '?value] '[$p ?e :order/id ?id]]}
            expected (set (d/q q (eager) (mapv #(vector (first (:values %)) (:binding-id %)) inputs)))]
        (try
          (let [cursor (open provider session request alternative inputs)]
            (try
              (let [{:keys [rows ids]} (consume cursor ctl alternative true)
                    columns (map :variable (:columns alternative))
                    actual (set (map (fn [id row] [id (get (zipmap columns row) '?id)]) ids rows))]
                (is (= expected actual) (pr-str attribute))
                (is (<= (:external-requests (spi/-cursor-metrics cursor)) 10)))
              (finally (spi/-close-cursor! cursor))))
          (finally (spi/-close-session! session)))
        (is (released? ctl))
        (is (zero? (:live-sessions (pg/statistics provider))))
        (control/close! ctl)))))

(deftest thousands-of-dependent-keys-fit-the-default-request-budget
  (sql! (str "CREATE TABLE " (table "bulk") " (id bigint PRIMARY KEY, value bigint)"))
  (sql! (str "INSERT INTO " (table "bulk") " SELECT i, i*10 FROM generate_series(1,1200) i"))
  (let [accepted (pg/accept-mapping (pg/discover *profile* [*schema* "bulk"])
                   {:source-instance-id unit/source-id :relation-id :test/bulk :identity-attribute :row/id
                    :attributes {:row/value "value"}})
        entity #(ref/entity-ref (get-in accepted [:identity :type]) [%])
        inputs (conj (mapv #(vector (entity %) :original) (range 1 1201))
                     [(entity 1) :duplicate] [(entity 99999) :missing])
        q {:find '[?id ?value ?witness] :in '[$p [[?e ?witness]]]
           :where '[[$p ?e :row/id ?id] [$p ?e :row/value ?value]] :timeout 30000
           :query-source {:limits {:batch-rows 7}}}
        expected (conj (set (map #(vector % (* 10 %) :original) (range 1 1201))) [1 10 :duplicate])]
    (doseq [fusion? [false true] variable '[?e ?id]]
      (let [inputs (if (= variable '?e) inputs
                        (mapv (fn [[entity witness]] [(first (ref/key-tuple entity)) witness]) inputs))
            q (assoc q :in ['$p [[variable '?witness]]])
            provider (pg/source accepted (assoc (dissoc *profile* :fetch-size) :fusion? fusion?))
            explained (d/explain {:run? true} q provider inputs)
            requests (reduce + (map #(get-in % [:metrics :external-requests]) (:cursors explained)))]
        (is (= expected (set (d/q q provider inputs))))
        (is (= (count expected) (:result-rows explained)))
        ;; Entity references reach the binding-byte cap before the row cap;
        ;; separate accesses also collect entity keys for their second step.
        (is (< requests (if (and fusion? (= variable '?id)) 32 128))
            (pr-str {:lookup variable :fusion? fusion? :requests requests}))
        (is (zero? (:live-sessions (pg/statistics provider))))))))

(deftest larger-default-fetches-stream-a-hundred-thousand-rows-within-budget
  (sql! (str "CREATE TABLE " (table "scan") " (id bigint PRIMARY KEY)"))
  (sql! (str "INSERT INTO " (table "scan") " SELECT i FROM generate_series(1,100000) i"))
  (let [accepted (pg/accept-mapping (pg/discover *profile* [*schema* "scan"])
                   {:source-instance-id unit/source-id :relation-id :test/scan :identity-attribute :row/id
                    :attributes {}})
        provider (pg/source accepted (dissoc *profile* :fetch-size))
        ctl (control/create) request (request provider ctl)
        session (spi/-open-session provider request)]
    (try
      (let [clauses [{:id 0 :kind :pattern :terms '[[:ignore] [:const :row/id] [:var ?id]]}]
            alternative (first (alternatives provider session request clauses []))
            cursor (open provider session request alternative nil)]
        (try
          ;; Stream and release pages; core result materialization has separate
          ;; memory charges and is not part of this adapter request-budget check.
          (let [[rows total]
                (loop [rows 0 total 0]
                  (let [fetch {:control ctl :max-rows 4096 :max-bytes 1048576}
                        batch (batch/accept! {:control ctl} (:columns alternative) false fetch
                                (spi/-next-batch! cursor fetch) identity)
                        rows (+ rows (:row-count batch))
                        total (+ total (reduce + (map first (:rows batch))))]
                    (spi/release! (:reservation batch))
                    (if (:exhausted? batch) [rows total] (recur rows total))))]
            (is (= [100000 5000050000] [rows total]))
            (is (< (:external-requests (control/usage ctl)) 800)))
          (finally (spi/-close-cursor! cursor))))
      (finally (spi/-close-session! session)))
    (is (released? ctl))
    (is (zero? (:live-sessions (pg/statistics provider))))
    (let [q '[:find (count ?id) . :in $p :where [$p _ :row/id ?id]]
          explained (d/explain {:run? true} q provider)]
      (is (= 100000 (d/q q provider)))
      (is (relational-plan? explained))
      (is (= 1 (read-rows explained))))
    (control/close! ctl)))

(deftest fetch-pages-shrink-to-fit-the-shared-memory-budget
  (doseq [max-row-bytes [16384 1048576]]
    (let [provider (pg/source *accepted* (assoc (dissoc *profile* :fetch-size) :max-row-bytes max-row-bytes))
          ctl (control/create {:limits {:retained-bytes (* 16 1024 1024)}})
          request (request provider ctl)
          session (spi/-open-session provider request)]
      (try
        (let [clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:var ?title]]}]
              alternative (first (alternatives provider session request clauses []))]
          (if (= max-row-bytes 16384)
            (let [cursor (open provider session request alternative nil)]
              (try
                (is (= #{"Alpha" "βeta" "x'; DROP TABLE orders; --"}
                       (set (map second (:rows (consume cursor ctl alternative false))))))
                (finally (spi/-close-cursor! cursor))))
            (is (= :query-source/limit (category #(open provider session request alternative nil))))))
        (finally (spi/-close-session! session)))
      (is (released? ctl))
      (is (zero? (:live-sessions (pg/statistics provider))))
      (control/close! ctl))))

(deftest used-schema-drift-fails-unrelated-additions-do-not
  (let [provider (source) q '[:find ?title :in $p :where [$p ?e :order/title ?title]]]
    (sql! (str "ALTER TABLE " (table "orders") " ADD COLUMN unused integer"))
    (is (= 3 (count (d/q q provider))))
    (sql! (str "ALTER TABLE " (table "orders") " ALTER COLUMN title TYPE varchar(100)"))
    (is (= :query-source/stale-definition (category #(d/q q provider))))
    (is (zero? (:live-sessions (pg/statistics provider))))))

(deftest composite-keys-quoted-identifiers-and-typed-references
  (sql! (str "CREATE TABLE " (table "odd\"table")
             " (tenant text, id bigint, parent bigint, \"odd\"\"name\" text, PRIMARY KEY (tenant,id))"))
  (sql! (str "INSERT INTO " (table "odd\"table") " VALUES ('a',1,7,'left'),('b',1,8,'right')"))
  (let [catalog (pg/discover *profile* [*schema* "odd\"table"])
        domain [unit/source-id :people 0]
        type (mapping/entity-type ref/type-contract domain [:long])
        accepted (pg/accept-mapping catalog
                   {:source-instance-id unit/source-id :relation-id :test/quoted :identity-attribute :row/key
                    :attributes {:row/title "odd\"name" :row/parent {:column "parent" :reference {:domain domain :type type}}}})
        provider (pg/source accepted *profile*)]
    (is (= #{[["a" 1] "left" (ref/entity-ref type [7])] [["b" 1] "right" (ref/entity-ref type [8])]}
           (set (d/q '[:find ?key ?title ?parent :in $p :where
                       [$p ?e :row/key ?key] [$p ?e :row/title ?title] [$p ?e :row/parent ?parent]] provider))))
    (is (= #{["right"]}
           (set (d/q '[:find ?title :in $p ?key :where [$p ?e :row/key ?key] [$p ?e :row/title ?title]]
                     provider ["b" 1]))))
    (is (= #{["left" :a] ["right" :b] ["left" :duplicate]}
           (set (d/q '[:find ?title ?witness :in $p [[?key ?witness]] :where
                       [$p ?e :row/key ?key] [$p ?e :row/title ?title]]
                     provider [[["a" 1] :a] [["b" 1] :b] [["a" 1] :duplicate] [["missing" 1] :missing]]))))
    (is (= #{["left"] ["right"]}
           (set (d/q '[:find ?title :in $p [?parent ...] :where
                       [$p ?e :row/parent ?parent] [$p ?e :row/title ?title]]
                     provider (mapv #(ref/entity-ref type [%]) [8 7 99])))))))

(deftest aliases-and-relations-share-one-session
  (let [provider (source)
        other (pg/source (pg/accept-mapping (pg/discover *profile* [*schema* "orders"])
                           {:source-instance-id unit/source-id :relation-id :sales/owners
                            :identity-attribute :other/id :attributes {:other/owner "owner"}}) *profile*)
        q '[:find ?id ?title ?owner :in $a $b :where
            [$a ?e :order/id ?id] [$a ?e :order/title ?title] [$b ?e :other/owner ?owner]]
        result (d/explain {:run? true} q provider other)]
    (is (= 2 (:result-rows result)))
    (is (= 1 (count (set (map :session-id (:sources result))))))
    (is (zero? (:live-sessions (pg/statistics provider))))
    (is (zero? (:live-sessions (pg/statistics other))))))

(deftest persisted-mappings-reopen-with-runtime-profile
  (let [path (u/tmp-dir (str "federation-postgres-reopen-" (random-uuid)))
        conn (d/get-conn path {} {:background-sampling? false})]
    (try
      (install/install! conn
        {:postgres {:datalevin/definition-kind :source
                    :datalevin.federation/definition {:format-version 1 :source-instance-id unit/source-id
                                                     :provider :postgres :profile :tests/postgres}}
         :sales/orders {:datalevin/definition-kind :relation
                        :datalevin.federation/definition {:format-version 1 :source :postgres :accepted *accepted*}}}
        {:expected-versions {:postgres nil :sales/orders nil}})
      (d/close conn)
      (let [reopened (d/get-conn path)]
        (try
          (let [loaded (install/load-definitions reopened)
                accepted (get-in loaded [:compiled :sales/orders :accepted])]
            (is (= 4 (count (d/q '[:find ?id :in $p :where [$p ?e :order/id ?id]]
                                (pg/source accepted *profile*))))))
          (finally (d/close reopened))))
      (finally (d/close conn) (u/delete-files path)))))

(deftest budgets-and-large-rows-fail-without-partial-success
  (let [q (query '[?title] '[[$p ?e :order/title ?title]])]
    (doseq [limits [{:external-requests 1} {:retained-bytes 1024} {:batch-bytes 1}]]
      (let [provider (source)]
        (is (= :query-source/limit (category #(d/q (assoc-in q [:query-source :limits] limits) provider))))
        (is (zero? (:live-sessions (pg/statistics provider))))))
    (sql! (str "UPDATE " (table "orders") " SET title=repeat('x',100000) WHERE id=1"))
    (is (= :query-source/limit (category #(d/q q (source {:max-row-bytes 128})))))))

(deftest blocked-acquisition-observes-deadline-and-releases-everything
  (let [provider (source) ctl (control/create {:timeout-ms 150})
        request (request provider ctl)]
    (.setAutoCommit ^Connection *db* false)
    (try
      (sql! (str "LOCK TABLE " (table "orders") " IN ACCESS EXCLUSIVE MODE"))
      (let [start (System/nanoTime)]
        (is (= :query/timeout (category #(spi/-open-session provider request))))
        (is (< (/ (- (System/nanoTime) start) 1e6) 3000)))
      (finally (.rollback ^Connection *db*) (.setAutoCommit ^Connection *db* true)))
    (is (released? ctl))
    (is (zero? (:live-sessions (pg/statistics provider))))
    (control/close! ctl)))

(deftest shared-scan-and-lookup-conformance
  ;; Scalar attributes are the supported subset of the shared gate's :many role.
  (doseq [fusion? [false true]]
    (let [provider (source {:fusion? fusion?})
          roles {:identity :order/id :value :order/title :many :order/status}]
      (conformance/mapped-scan-semantics! provider (eager) roles)
      (conformance/mapped-lookup-semantics! provider (eager) roles))))

(deftest generated-scalar-relations-preserve-fusion-products-and-aggregates
  (sql! (str "CREATE TABLE " (table "generated") " (id bigint PRIMARY KEY, a bigint, b bigint, label text)"))
  (sql! (str "INSERT INTO " (table "generated")
             " SELECT i, CASE WHEN i%5<>0 THEN i%7 END, CASE WHEN i%3<>0 THEN i%7 END, "
             "CASE WHEN i%4<>0 THEN 'label-' || (i%9)::text END FROM generate_series(1,64) i"))
  (let [accepted (pg/accept-mapping (pg/discover *profile* [*schema* "generated"])
                   {:source-instance-id unit/source-id :relation-id :test/generated :identity-attribute :row/id
                    :attributes {:row/a "a" :row/b "b" :row/label "label"}})
        compiled (mapping/compile-logical-mapping accepted)
        eager (vec (mapcat (fn [i]
                            (mapping/project-facts compiled [i]
                              {:row/a (when-not (zero? (mod i 5)) (mod i 7))
                               :row/b (when-not (zero? (mod i 3)) (mod i 7))
                               :row/label (when-not (zero? (mod i 4)) (str "label-" (mod i 9)))})) (range 1 65)))
        queries [(query '[?id ?a ?b] '[[$p ?e :row/id ?id] [$p ?e :row/a ?a] [$p ?e :row/b ?b]])
                 (query '[?id ?x] '[[$p ?e :row/id ?id] [$p ?e :row/a ?x] [$p ?e :row/b ?x]])
                 (query '[?label] '[[$p ?e :row/a _] [$p ?e :row/b _] [$p ?e :row/label ?label]])
                 (assoc (query '[(count ?label)] '[[$p ?e :row/a _] [$p ?e :row/label ?label]]) :with '[?e])
                 (query '[?id] '[[$p ?e :row/id ?id] [(>= ?id 32)] [$p ?e :row/a 3]])]]
    (doseq [fusion? [false true] q queries]
      (is (= (set (d/q (dissoc q :query-source) eager))
             (set (d/q q (pg/source accepted (assoc *profile* :fusion? fusion? :fetch-size 7)))))))))

(deftest exact-physical-types-and-unsupported-catalog-columns
  (sql! (str "CREATE TABLE " (table "types")
             " (id uuid PRIMARY KEY, small smallint, n integer, big bigint, f real, d double precision, "
             "b boolean, bytes bytea, amount numeric, stamp timestamptz, nope jsonb)"))
  (sql! (str "INSERT INTO " (table "types")
             " VALUES ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',-4,42,9007199254740993,1.25,2.5,false,"
             "decode('0001ff','hex'),1.2500,'2026-10-20 12:34:56.123456+00','{}')"))
  (let [catalog (pg/discover *profile* [*schema* "types"])
        declaration {:source-instance-id unit/source-id :relation-id :test/types :identity-attribute :row/id
                     :attributes {:row/small "small" :row/n "n" :row/big "big" :row/f "f" :row/d "d"
                                  :row/b "b" :row/bytes "bytes" :row/amount "amount" :row/stamp "stamp"}}
        accepted (pg/accept-mapping catalog declaration)
        provider (pg/source accepted *profile*)
        q '[:find ?id ?small ?n ?big ?f ?d ?b ?bytes ?amount ?stamp :in $p :where
            [$p ?e :row/id ?id] [$p ?e :row/small ?small] [$p ?e :row/n ?n] [$p ?e :row/big ?big]
            [$p ?e :row/f ?f] [$p ?e :row/d ?d] [$p ?e :row/b ?b] [$p ?e :row/bytes ?bytes]
            [$p ?e :row/amount ?amount] [$p ?e :row/stamp ?stamp]]
        expected [(UUID/fromString "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") -4 42 9007199254740993
                  (float 1.25) 2.5 false (byte-array [0 1 -1]) 1.2500M (Instant/parse "2026-10-20T12:34:56.123456Z")]]
    (is (= "jsonb" (get-in catalog [:columns "nope" :type])))
    (is (thrown? clojure.lang.ExceptionInfo
                 (pg/accept-mapping catalog (assoc-in declaration [:attributes :row/nope] "nope"))))
    (is (QueryKey/equals expected (first (d/q q provider))))
    (doseq [column '[[:row/bytes ?bytes] [:row/amount ?amount] [:row/big ?big]]]
      (let [[a v] column expected (get (zipmap '[:row/bytes :row/amount :row/big]
                                                            [(byte-array [0 1 -1]) 1.2500M 9007199254740993]) a)
            q {:find '[?id] :in ['$p [v '...]] :where [['$p '?e a v] ['$p '?e :row/id '?id]]}]
        (is (= 1 (count (d/q q provider [expected]))))))
    (doseq [[attribute values] [[:row/small [-4 40000]] [:row/n [42 3000000000]]]]
      (let [q {:find '[?id] :in '[$p [?value ...]]
               :where [['$p '?e attribute '?value] '[$p ?e :row/id ?id]]}]
        (is (= 1 (count (d/q q provider values))))))
    (sql! (str "UPDATE " (table "types") " SET stamp='infinity'"))
    (is (= :query-source/invalid (category #(d/q q provider))))))

(defn data-source [checkout]
  (reify DataSource
    (getConnection [_] (checkout))
    (getConnection [_ _ _] (checkout))
    (getLoginTimeout [_] 1)
    (setLoginTimeout [_ _])
    (getLogWriter [_] nil)
    (setLogWriter [_ _])
    (getParentLogger [_] (java.util.logging.Logger/getGlobal))
    (unwrap [_ _] (throw (java.sql.SQLException. "Not a wrapper")))
    (isWrapperFor [_ _] false)))

(deftest pooled-connections-are-returned-without-closing-the-pool
  (let [pool (doto (PGConnectionPoolDataSource.) (.setURL (:jdbc-url *profile*)))
        pooled (.getPooledConnection pool)
        returns (atom 0)
        listener (reify ConnectionEventListener
                   (connectionClosed [_ _] (swap! returns inc))
                   (connectionErrorOccurred [_ _]))
        _ (.addConnectionEventListener pooled listener)
        profile (-> *profile* (dissoc :jdbc-url) (assoc :data-source (data-source #(.getConnection pooled))))
        provider (pg/source *accepted* profile)]
    (try
      (dotimes [_ 2]
        (is (= 4 (count (d/q '[:find ?id :in $p :where [$p ?e :order/id ?id]] provider)))))
      (is (= 2 @returns))
      (with-open [connection (.getConnection pooled)] (is (.isValid connection 1)))
      (finally (.close pooled)))))

(deftest timed-out-checkout-closes-a-late-connection
  (let [release (CountDownLatch. 1) entered (CountDownLatch. 1) late (promise) url (:jdbc-url *profile*)
        pool (data-source
               #(do (.countDown entered)
                    (loop [] (when-not (try (.await release 20 TimeUnit/MILLISECONDS)
                                           (catch InterruptedException _ false)) (recur)))
                    (let [conn (DriverManager/getConnection url)]
                      (deliver late conn) conn)))
        provider (pg/source *accepted* (-> *profile* (dissoc :jdbc-url) (assoc :data-source pool)))
        ctl (control/create {:timeout-ms 150}) request (request provider ctl)]
    (try
      (is (= :query/timeout (category #(spi/-open-session provider request))))
      (is (zero? (.getCount entered)))
      (is (released? ctl))
      (.countDown release)
      (let [connection (deref late 3000 nil)]
        (is (some? connection))
        (when connection
          (loop [attempt 0]
            (when (and (< attempt 100) (not (.isClosed ^Connection connection)))
              (Thread/sleep 10) (recur (inc attempt))))
          (is (.isClosed ^Connection connection))))
      (finally (.countDown release) (control/close! ctl)))))

(defn intercepted-connection [url prepare]
  (let [connection (DriverManager/getConnection url)]
    (Proxy/newProxyInstance (.getClassLoader Connection) (into-array Class [Connection])
      (reify InvocationHandler
        (invoke [_ _ method args]
          (let [^Method method method
                args (if (= "prepareStatement" (.getName method))
                       (object-array [(prepare (aget ^objects args 0))]) args)]
            (try (.invoke method connection args)
                 (catch InvocationTargetException e (throw (.getCause e))))))))))

(deftest cancellation-unblocks-real-jdbc-fetch
  (let [entered (CountDownLatch. 1) url (:jdbc-url *profile*)
        pool (data-source #(intercepted-connection url
                             (fn [sql]
                               (if (.startsWith ^String sql "FETCH FORWARD")
                                 (do (.countDown entered) "SELECT pg_sleep(30)") sql))))
        provider (pg/source *accepted* (-> *profile* (dissoc :jdbc-url) (assoc :data-source pool)))
        ctl (control/create) request (request provider ctl)
        session (spi/-open-session provider request)
        clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:var ?title]]}]
        alternative (first (alternatives provider session request clauses []))
        cursor (open provider session request alternative nil)
        result (future (category #(spi/-next-batch! cursor {:control ctl :max-rows 1 :max-bytes 1048576})))]
    (try
      (is (.await entered 3 TimeUnit/SECONDS))
      (control/cancel! ctl {:error :query-source/cancelled :phase :execution})
      (is (= :query-source/cancelled (deref result 3000 :blocked)))
      (finally (spi/-close-cursor! cursor) (spi/-close-session! session)))
    (is (released? ctl))
    (control/close! ctl)))

(deftest ddl-before-lock-cannot-hide-behind-an-older-catalog-snapshot
  (let [url (:jdbc-url *profile*) changed? (atom false)
        pool (data-source
               #(intercepted-connection url
                  (fn [sql]
                    (when (and (.startsWith ^String sql "LOCK TABLE ONLY")
                               (compare-and-set! changed? false true))
                      (sql! (str "ALTER TABLE " (table "orders") " ALTER COLUMN title TYPE varchar(100)")))
                    sql)))
        provider (pg/source *accepted* (-> *profile* (dissoc :jdbc-url) (assoc :data-source pool)))]
    (is (= :query-source/stale-definition
           (category #(d/q '[:find ?title :in $p :where [$p ?e :order/title ?title]] provider))))
    (is @changed?)
    (is (zero? (:live-sessions (pg/statistics provider))))))

(deftest live-catalog-rejects-duplicate-rows-and-deferrable-keys
  (sql! (str "CREATE TABLE " (table "duplicates") " (id bigint, title text)"))
  (sql! (str "INSERT INTO " (table "duplicates") " VALUES (1,'first'),(1,'second')"))
  (sql! (str "CREATE TABLE " (table "deferred") " (id bigint PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, title text)"))
  (doseq [name ["duplicates" "deferred"]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (pg/accept-mapping (pg/discover *profile* [*schema* name])
                   {:source-instance-id unit/source-id :relation-id :test/invalid
                    :identity-attribute :row/id :attributes {:row/title "title"}})))))

(deftest snapshot-loss-does-not-reconnect-or-return-a-prefix
  (let [provider (source {:fetch-size 1}) ctl (control/create) request (request provider ctl)
        session (spi/-open-session provider request)
        clauses [{:id 0 :kind :pattern :terms '[[:var ?e] [:const :order/title] [:var ?title]]}]
        alternative (first (alternatives provider session request clauses []))
        cursor (open provider session request alternative nil)]
    (try
      (let [batch (spi/-next-batch! cursor {:control ctl :max-rows 1 :max-bytes 1048576})
            pid (first (jdbc/query! (:context session) :execution "SELECT pg_backend_pid()" [] #(.getInt ^java.sql.ResultSet % 1)))]
        (is (= 1 (:row-count batch)))
        (spi/release! (:reservation batch))
        (sql! (str "SELECT pg_terminate_backend(" pid ")"))
        (is (= :query-source/snapshot-lost
               (category #(spi/-next-batch! cursor {:control ctl :max-rows 1 :max-bytes 1048576})))))
      (finally (spi/-close-cursor! cursor) (spi/-close-session! session)))
    (is (released? ctl))
    (control/close! ctl)))
