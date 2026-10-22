(ns datalevin.federation.file-test
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.file :as file]
            [datalevin.federation.install :as install]
            [datalevin.federation.mapping :as mapping]
            [datalevin.query.source :as spi]
            [datalevin.query.source.conformance :as conformance]
            [datalevin.query.source.control :as control]
            [datalevin.util :as u])
  (:import [java.io ByteArrayInputStream InputStream]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util HexFormat]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:dynamic *dir* nil)
(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "federation-file-" (random-uuid)))]
      (.mkdirs (io/file *dir* "spools"))
      (try (f) (finally (u/delete-files *dir*))))))

(def source-id #uuid "12345678-1111-4111-8111-123456789abc")
(def records [{:name "Ada" :tags ["red" "red" nil "blue"] :amount 1.0M :friends [10 10 nil 11]}
              {:name "Grace" :tags ["red"] :amount 2M :nested {:score 7}}
              {:name "Ada" :tags []}])
(defn attribute [type expression cardinality]
  {:type type :extraction {:shape (if (= :many cardinality) :elements :value) :expression expression}
   :cardinality cardinality :null :absent :conversion {:version 1}})
(def declaration
  {:relation-id :exports/people :source-instance-id source-id :domain :person :identity-epoch 0
   :identity-attribute :export/row :scope {:id "people:all" :kind :full :parameters {}}
   :attributes
   {:person/name (attribute :string [:field :name] :one)
    :person/tag (attribute :string [:field :tags] :many)
    :person/amount (attribute :bigdec [:field :amount] :one)
    :person/score (attribute :long [:path [:nested :score]] :one)
    :person/friend {:extraction {:shape :elements :expression [:field :friends]}
                    :reference {:domain [source-id :customer 0]
                                :type (mapping/entity-type ref/type-contract [source-id :customer 0] [:long])}
                    :cardinality :many :null :absent :conversion {:version 1}}}})

(defn- export! [rows]
  (let [path (str *dir* "/people.ednl")]
    (with-open [out (io/writer path :encoding "UTF-8")]
      (doseq [row rows] (.write out (str (pr-str row) "\n"))))
    (file/accept-mapping declaration (file/manifest path (random-uuid)))))

(defn- source [accepted]
  (file/source accepted {:path (str *dir* "/people.ednl") :compute-context-id :test/export
                         :spool-directory (str *dir* "/spools")}))
(defn- clean? [] (empty? (seq (.listFiles (io/file *dir* "spools")))))
(defn- category [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))
(defn- eager [accepted rows]
  (let [project (mapping/fact-projector (mapping/compile-logical-mapping accepted))
        token (file/export-token (get-in accepted [:dependencies :export]))]
    (into [] (mapcat (fn [ordinal row]
                       (project [token ordinal]
                                {:person/name (:name row) :person/tag (:tags row) :person/amount (:amount row)
                                 :person/score (get-in row [:nested :score]) :person/friend (:friends row)}))
                     (iterate inc 1) rows))))

(deftest file-queries-match-eager-facts
  (let [accepted (export! records) provider (source accepted) facts (eager accepted records)]
    (doseq [query ['[:find ?e ?name :in $s :where [$s ?e :person/name ?name]]
                   '[:find ?row ?tag :in $s :where [$s ?e :export/row ?row] [$s ?e :person/tag ?tag]]
                   '[:find ?name ?friend :in $s :where [$s ?e :person/name ?name] [$s ?e :person/friend ?friend]]
                   '[:find (count ?e) . :in $s :where [$s ?e :person/name _]]
                   '[:find (count ?e) . :with ?tag :in $s :where [$s ?e :person/name _] [$s ?e :person/tag ?tag]]
                   '[:find ?name :in $s :where [$s ?e :person/name ?name] [$s ?e :person/score _]]
                   '[:find ?name :in $s :where [$s ?e :person/name ?name] [(= ?name "Ada")]]
                   '[:find ?name :in $s :where (or [$s _ :person/name ?name] [$s _ :person/tag ?name])]
                   '[:find [?amount ...] :in $s :where [$s _ :person/amount ?amount]]]]
      (let [expected (d/q query facts) actual (d/q query provider)]
        (is (= (if (sequential? expected) (frequencies expected) expected)
               (if (sequential? actual) (frequencies actual) actual)) (pr-str query)))
      (is (clean?)))
    (let [query '[:find (count ?e) . :in $s [?tag ...] :where [$s ?e :person/tag ?tag]]]
      (is (= (d/q query facts ["red" "red" "blue"]) (d/q query provider ["red" "red" "blue"]))))
    (is (zero? (:live-spools (file/statistics provider))))))

(deftest metadata-is-pure-and-actual-work-is-separated-from-estimates
  (let [accepted (export! records)
        opens (atom 0)
        provider (file/source accepted {:compute-context-id :test/observed
                                        :spool-directory (str *dir* "/spools")
                                        :open-input (fn [_] (swap! opens inc) (io/input-stream (str *dir* "/people.ednl")))})
        descriptor (spi/-source-info provider)
        _ (is (= descriptor (spi/-source-info provider)))
        _ (is (zero? @opens))
        explain (d/explain {:run? true} '[:find ?name :in $s :where [$s _ :person/name ?name]] provider)
        metrics (get-in explain [:cursors 0 :metrics])
        stats (file/statistics provider)
        export (get-in accepted [:dependencies :export])]
    (is (= 1 @opens (:acquisitions stats)))
    (is (= (:bytes export) (:read-bytes stats) (:read-bytes metrics)))
    (is (= 3 (:validated-records stats) (:read-rows metrics)))
    (is (zero? (:external-requests metrics)))
    (is (= :snapshot (get-in explain [:sources 0 :consistency])))
    (is (= (file/export-token export) (get-in explain [:sources 0 :data-version])))
    (is (every? #(= :attribute-enumeration (get-in % [:access :kind]))
                (mapcat :selected (:sources explain))))
    (is (clean?))))

(defn- session-request [provider ctl]
  {:control ctl :query-id (random-uuid) :spi-version 2 :features #{} :consistency :snapshot
   :limits (control/limits ctl) :authorization nil :source-descriptors [(spi/-source-info provider)]})

(defn- plan-request [provider request]
  (merge request {:source-symbol '$s :region-id 0 :scope-id "people:all" :bindings {} :available-inputs []
                  :source-descriptor (spi/-source-info provider)
                  :clauses [{:id 0 :kind :pattern :terms [[:var '?e] [:const :person/name] [:var '?name]]}]}))

(defn- open-request [provider request alternative]
  (merge (select-keys request [:control :query-id :spi-version :features :authorization :consistency :limits])
         {:source-symbol '$s :region-id 0 :scope-id "people:all"
          :definition-fingerprint (:definition-fingerprint (spi/-source-info provider))
          :alternative-id (:id alternative) :plan (:plan alternative)}))

(deftest cursor-replay-is-bounded-and-cleanup-is-idempotent
  (let [accepted (export! records) provider (source accepted) ctl (control/create)
        request (session-request provider ctl)
        session (spi/-open-session provider request)]
    (try
      (let [alternative (first (:alternatives (spi/-plan-source provider session (plan-request provider request))))
            open (open-request provider request alternative)
            read! (fn [max-rows max-bytes]
                    (let [cursor (spi/-open-cursor session open)]
                      (try
                        (loop [rows []]
                          (let [batch (spi/-next-batch! cursor {:control ctl :max-rows max-rows :max-bytes max-bytes})]
                            (is (<= (:row-count batch) max-rows))
                            (is (= (:byte-count batch) (spi/batch-charge-bytes (:columns batch) (:rows batch))))
                            (when-let [reservation (:reservation batch)] (spi/release! reservation))
                            (if (:exhausted? batch) (into rows (:rows batch))
                                (recur (into rows (:rows batch))))))
                        (finally (spi/-close-cursor! cursor) (spi/-close-cursor! cursor)))))
            first (read! 1 16384)
            narrow (apply max (map #(spi/batch-charge-bytes (:columns alternative) [%]) first))]
        (is (= first (read! 100 narrow)))
        (is (= 3 (count first)))
        (is (= 1 (:live-spools (file/statistics provider))))
        (is (= :query-source/stale-definition
               (category #(spi/-open-cursor session (assoc open :definition-fingerprint "stale")))))
        (is (= :query-source/invalid
               (category #(spi/-open-cursor session (assoc open :bindings [])))))
        (let [cursor (spi/-open-cursor session open)]
          (spi/-cancel-session! session {:error :query-source/cancelled})
          (is (= :query-source/cancelled
                 (category #(spi/-next-batch! cursor {:control ctl :max-rows 1 :max-bytes 16384}))))
          (spi/-close-cursor! cursor)
          (is (zero? (:read-rows (spi/-cursor-metrics cursor))))))
      (finally (spi/-close-session! session) (spi/-close-session! session)))
    (is (clean?))
    (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))
    (is (= 1 (:external-requests (control/usage ctl))))
    (control/close! ctl)))

(deftest cancellation-unblocks-acquisition-and-deletes-partial-copies
  (let [accepted (export! records) closed (CountDownLatch. 1) reading (CountDownLatch. 1)
        close-count (atom 0)
        blocked (proxy [InputStream] []
                  (read
                    ([] (throw (UnsupportedOperationException.)))
                    ([_ _ _]
                     (.countDown reading)
                     (when-not (.await closed 5 TimeUnit/SECONDS)
                       (throw (ex-info "cancellation failed to close the stream" {})))
                     (throw (java.io.IOException. "closed"))))
                  (close [] (swap! close-count inc) (.countDown closed)))
        provider (file/source accepted {:open-input (fn [_] blocked) :compute-context-id :test/blocked
                                        :spool-directory (str *dir* "/spools")})
        ctl (control/create {:timeout-ms 1000})
        task (future (category #(spi/-open-session provider (session-request provider ctl))))]
    (try
      (is (.await reading 2 TimeUnit/SECONDS))
      (control/cancel! ctl {:error :query-source/cancelled :phase :acquisition})
      (is (= :query-source/cancelled (deref task 3000 ::blocked)))
      (is (= 1 @close-count))
      (is (clean?))
      (is (zero? (:live-spools (file/statistics provider))))
      (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))
      (finally (.countDown closed) (future-cancel task) (control/close! ctl)))))

(deftest partial-open-and-read-failures-release-every-resource
  (let [accepted (export! records)]
    (doseq [mode [:open :read :invalid-result]]
      (let [closed (atom 0)
            provider (file/source accepted
                       {:compute-context-id mode :spool-directory (str *dir* "/spools")
                        :open-input (fn [_]
                                      (case mode
                                        :open (throw (java.io.IOException. "failed open"))
                                        :invalid-result (reify java.io.Closeable (close [_] (swap! closed inc)))
                                        :read (proxy [InputStream] []
                                          (read
                                            ([] (throw (java.io.IOException. "failed read")))
                                            ([_ _ _] (throw (java.io.IOException. "failed read"))))
                                          (close [] (swap! closed inc)))))})
            ctl (control/create)]
        (try
          (if (= mode :invalid-result)
            (is (= :query-source/invalid (category #(spi/-open-session provider (session-request provider ctl)))))
            (is (thrown? java.io.IOException (spi/-open-session provider (session-request provider ctl)))))
          (is (= (if (= mode :open) 0 1) @closed))
          (is (clean?))
          (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))
          (finally (control/close! ctl)))))))

(defn- raw-export [text options]
  (let [bytes (.getBytes ^String text StandardCharsets/UTF_8)]
    (spit (str *dir* "/people.ednl") text :encoding "UTF-8")
    (merge {:format :edn-lines/v1 :id (random-uuid)
            :sha256 (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes))
            :bytes (alength bytes) :records 1 :max-record-bytes 16384 :max-depth 32} options)))

(deftest malformed-framing-and-resource-amplification-fail-before-results
  (doseq [[text options expected]
          [["{:name \"a\"} {:name \"b\"}\n" {} :query-source/invalid]
           ["\n" {} :query-source/invalid]
           ["#unknown {:name \"a\"}\n" {} :query-source/invalid]
           ["#=(System/exit 0)\n" {} :query-source/invalid]
           [(str "{:name \"" (apply str (repeat 200 "x")) "\"}\n") {:max-record-bytes 32} :query-source/limit]
           ["{:ignored [[[[[[1]]]]]]}\n" {:max-depth 4} :query-source/limit]]]
    (testing (pr-str options)
      (let [export (raw-export text options)
            accepted (file/accept-mapping declaration export)
            query '{:find [?n] :in [$s] :where [[$s _ :person/name ?n]] :limit 1}]
        (is (= expected
               (category #(d/q query (source accepted)))))
        (is (clean?)))))
  (let [export (raw-export "{}\n" {})
        bytes (byte-array [(unchecked-byte 255) 10])
        _ (with-open [out (io/output-stream (str *dir* "/people.ednl"))] (.write out bytes))
        export (assoc export :bytes 2 :sha256 (.formatHex (HexFormat/of)
                                               (.digest (MessageDigest/getInstance "SHA-256") bytes)))]
    (is (= :query-source/invalid
           (category #(d/q '[:find ?n :in $s :where [$s _ :person/name ?n]]
                           (source (file/accept-mapping declaration export))))))
    (is (clean?))))

(deftest large-selective-export-fits-a-small-memory-budget
  (let [payload (apply str (repeat 180 "x"))
        rows (map #(hash-map :ignored payload :name (if (= % 19999) "selected" "other")) (range 20000))
        accepted (export! rows)
        export (assoc (get-in accepted [:dependencies :export]) :max-record-bytes 256)
        accepted (file/accept-mapping declaration export)
        provider (source accepted)
        result (d/explain {:run? true}
                 '{:find [?e] :in [$s] :where [[$s ?e :person/name "selected"]]
                   :query-source {:limits {:retained-bytes 1048576 :batch-bytes 67108864}}} provider)]
    (is (= 1 (:result-rows result)))
    (is (> (:bytes export) 1048576))
    (is (= 20000 (get-in result [:cursors 0 :metrics :read-rows])))
    (is (= (:bytes export) (:read-bytes (file/statistics provider))))
    (is (clean?))))

(deftest shared-scan-conformance-for-advertised-accesses
  (let [accepted (export! records)]
    (conformance/mapped-scan-semantics! (source accepted) (eager accepted records)
                                      {:identity :export/row :value :person/name :many :person/tag
                                       :reference :person/friend})
    (is (clean?))))

(deftest empty-exports-produce-complete-empty-results
  (let [accepted (export! [])
        provider (file/source accepted {:compute-context-id :test/empty :reopen :once
                                        :open-input (fn [_] (ByteArrayInputStream. (byte-array 0)))
                                        :spool-directory (str *dir* "/spools")})]
    (is (empty? (d/q '[:find ?name :in $s :where [$s _ :person/name ?name]] provider)))
    (is (zero? (:validated-records (file/statistics provider))))
    (is (clean?))))

(deftest identical-records-remain-distinct-and-absent-values-stay-absent
  (let [row {:name "Ada" :tags []}
        accepted (export! [row row {}]) provider (source accepted)
        token (file/export-token (get-in accepted [:dependencies :export]))]
    (is (= 2 (count (d/q '[:find ?e :in $s :where [$s ?e :person/name "Ada"]] provider))))
    (is (= #{[token 1] [token 2] [token 3]}
           (set (d/q '[:find [?row ...] :in $s :where [$s _ :export/row ?row]] provider))))
    (is (empty? (d/q '[:find ?v :in $s :where [$s _ :person/tag ?v]] provider)))
    (is (clean?))))

(deftest persisted-export-mappings-reopen-and-join-native-facts
  (let [accepted (export! records)
        path (str *dir* "/native")
        conn (d/create-conn path {:native/name {:db/valueType :db.type/string}}
                            {:wal? false :background-sampling? false})
        definitions {:exports
                     {:datalevin/definition-kind :source
                      :datalevin.federation/definition
                      {:format-version 1 :source-instance-id source-id :provider :file/export :profile :profiles/people}}
                     :exports/people
                     {:datalevin/definition-kind :relation
                      :datalevin.federation/definition {:format-version 1 :source :exports :accepted accepted}}}
        query '[:find ?tag :in $ $s :where [?n :native/name ?name] [$s ?e :person/name ?name] [$s ?e :person/tag ?tag]]
        before (try
                 (install/install! conn definitions {:expected-versions {:exports nil :exports/people nil}})
                 (d/transact! conn [{:native/name "Ada"}])
                 (let [loaded (install/load-definitions conn)
                       accepted (get-in loaded [:compiled :exports/people :accepted])]
                   (is (= #{["red"] ["blue"]} (d/q query @conn (source accepted))))
                   loaded)
                 (finally (d/close conn)))
        reopened (d/create-conn path)]
    (try
      (let [loaded (install/load-definitions reopened)
            accepted (get-in loaded [:compiled :exports/people :accepted])]
        (is (= before loaded))
        (is (= #{["red"] ["blue"]} (d/q query @reopened (source accepted))))
        (is (= (file/export-token (get-in accepted [:dependencies :export]))
               (first (nth (first (d/q '[:find ?row :in $s :where [$s _ :export/row ?row]] (source accepted))) 0)))))
      (finally (d/close reopened)))
    (is (clean?))))

(deftest same-export-replays-and-new-exports-have-distinct-identities
  (let [accepted (export! records)
        query '[:find [?e ...] :in $s :where [$s ?e :person/name "Ada"]]
        one (set (d/q query (source accepted)))
        again (set (d/q query (source accepted)))
        different (file/accept-mapping declaration (file/manifest (str *dir* "/people.ednl") (random-uuid)))
        two (set (d/q query (source different)))]
    (is (= 2 (count one)))
    (is (= one again))
    (is (empty? (set/intersection one two)))
    (is (= (get-in accepted [:identity :type]) (get-in different [:identity :type])))
    (is (clean?))))

(deftest pinned-snapshot-survives-origin-mutation-between-patterns
  (let [accepted (export! records) provider (source accepted)
        wrapped (reify spi/IQuerySource
                  (-source-info [_] (spi/-source-info provider))
                  (-open-session [_ request] (spi/-open-session provider request))
                  (-plan-source [_ session request]
                    (spit (str *dir* "/people.ednl") "changed underneath the pinned view")
                    (spi/-plan-source provider session request)))
        query '[:find ?name ?tag :in $a $b :where [$a ?e :person/name ?name] [$b ?e :person/tag ?tag]]]
    (is (= (d/q query (eager accepted records) (eager accepted records)) (d/q query wrapped wrapped)))
    (is (= 1 (:acquisitions (file/statistics provider))))
    (is (= :query-source/snapshot-lost
           (category #(d/q '[:find ?n :in $s :where [$s _ :person/name ?n]] provider))))
    (is (clean?))))

(deftest known-incomplete-or-invalid-exports-never-yield-partial-results
  (let [accepted (export! records)
        query '{:find [?name] :in [$s] :where [[$s _ :person/name ?name]] :limit 1}]
    (doseq [[key val expected] [[:sha256 (apply str (repeat 64 "0")) :query-source/snapshot-lost]
                                [:bytes 1 :query-source/snapshot-lost]
                                [:records 1 :query-source/incomplete]]]
      (let [export (assoc (get-in accepted [:dependencies :export]) key val)
            changed (file/accept-mapping declaration export)]
        (is (= expected (category #(d/q query (source changed)))))
        (is (clean?))))
    (let [bad (export! [{:name "valid"} {:name 17}])]
      (is (= :query-source/invalid (category #(d/q query (source bad)))))
      (is (clean?)))))

(deftest shared-resource-limits-admit-work-before-opening-the-export
  (let [accepted (export! records)
        query '{:find [?name] :in [$s] :where [[$s _ :person/name ?name]]}]
    (doseq [limits [{:retained-bytes 1024} {:spill-bytes 1} {:external-requests 0} {:batch-bytes 1}]]
      (let [provider (source accepted)]
        (is (= :query-source/limit (category #(d/q (assoc query :query-source {:limits limits}) provider))))
        (is (clean?))
        (is (zero? (:live-spools (file/statistics provider))))))))

(deftest one-shot-input-is-read-once-per-query-and-replayed-from-the-private-copy
  (let [accepted (export! records) opens (atom 0)
        provider (file/source accepted {:compute-context-id :test/once :reopen :once
                                        :spool-directory (str *dir* "/spools")
                                        :open-input (fn [_] (swap! opens inc) (io/input-stream (str *dir* "/people.ednl")))})
        query '[:find ?name ?tag :in $s :where [$s ?e :person/name ?name] [$s ?e :person/tag ?tag]]]
    (is (= (d/q query (eager accepted records)) (d/q query provider)))
    (is (= 1 @opens))
    (is (= :query-source/snapshot-lost (category #(d/q query provider))))
    (is (= 1 @opens))
    (is (clean?))))
