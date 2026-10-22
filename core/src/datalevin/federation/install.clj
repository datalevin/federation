(ns datalevin.federation.install
  "Revision-checked persistence of accepted federation metadata. Provider I/O
  belongs in explicit preparation callbacks, outside the schema writer."
  (:require [clojure.set :as set]
            [datalevin.client.request :as request]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.federation.fingerprint :as fp]
            [datalevin.federation.mapping :as mapping]
            [datalevin.lmdb :as l]
            [datalevin.query.source.value :as value]))

(def ^:private owner :datalevin.federation/definition)
(def ^:private membership :datalevin.federation/relations)
(def ^:private digest-keys
  [:schema-fingerprint :definition-fingerprint :fingerprint-scheme])

(defn- fail! [error data]
  (throw (ex-info "Invalid federation definition installation" (assoc data :error error))))

(defn- ensure! [condition error data]
  (when-not condition (fail! error data)))

(defn- owned? [entry] (contains? entry owner))
(defn- body [entry] (get entry owner))
(defn- envelope [kind definition]
  {:datalevin/definition-kind kind owner definition})

(defn- catalog [schema]
  (into {} (filter (fn [[_ entry]] (owned? entry))) schema))

(defn- validate-entry! [id entry]
  (let [kind (:datalevin/definition-kind entry)
        definition (body entry)
        fields (case kind
                 :source #{:source-instance-id :provider :profile}
                 :relation #{:source :accepted :depends-on}
                 nil)
        allowed (into (set digest-keys)
                      (concat fields [:format-version :version :documentation]))]
    (ensure! (and (keyword? id) fields
                  (= #{:datalevin/definition-kind owner} (set (keys entry)))
                  (value/plain-map? definition) (value/portable? definition)
                  (= 1 (:format-version definition))
                  (every? allowed (keys definition))
                  (or (not (contains? definition :documentation))
                      (string? (:documentation definition))))
             :federation/definition {:definition-id id})
    (case kind
      :source
      (ensure! (and (uuid? (:source-instance-id definition))
                    (keyword? (:provider definition))
                    (qualified-keyword? (:profile definition)))
               :federation/definition {:definition-id id})
      :relation
      (ensure! (and (keyword? (:source definition))
                    (set? (:depends-on definition))
                    (every? keyword? (:depends-on definition)))
               :federation/definition {:definition-id id}))))

(defn- dependencies [entry]
  (if (= :relation (:datalevin/definition-kind entry))
    (conj (:depends-on (body entry)) (:source (body entry)))
    #{}))

(defn- dependency-order [definitions check!]
  (doseq [[id entry] definitions]
    (check!)
    (validate-entry! id entry)
    (doseq [dependency (dependencies entry)]
      (ensure! (contains? definitions dependency) :federation/missing-dependency
               {:definition-id id :dependency-id dependency})))
  (loop [pending (set (keys definitions)), done #{}, order []]
    (check!)
    (if (empty? pending)
      order
      (let [ready (sort-by str (filter #(set/subset? (dependencies (definitions %)) done) pending))]
        (ensure! (seq ready) :federation/dependency-cycle {:definition-ids pending})
        (recur (apply disj pending ready) (into done ready) (into order ready))))))

(defn- compile-entry [id entry definitions compiled]
  (let [definition (body entry)]
    (case (:datalevin/definition-kind entry)
      :source
      {:definition-fingerprint
       (fp/accepted-fingerprint :definition
         {:definition-kind :source
          :source (select-keys definition [:format-version :source-instance-id :provider :profile])})
       :fingerprint-scheme :federation/v2}
      :relation
      (let [source-entry (definitions (:source definition))
            source (body source-entry)
            accepted (:accepted definition)]
        (ensure! (and (= :source (:datalevin/definition-kind source-entry))
                      (= id (:relation-id accepted))
                      (= (:source-instance-id source) (:source-instance-id accepted))
                      (= (:provider source) (get-in accepted [:dependencies :provider :name])))
                 :federation/source-mismatch {:definition-id id :source-id (:source definition)})
        (mapping/compile-logical-mapping
          (assoc-in accepted [:dependencies :definition-contracts]
                    (into {} (map (fn [dependency]
                                    [dependency (:definition-fingerprint (compiled dependency))]))
                          (dependencies entry))))))))

(defn- compile-catalog
  "Untouched definitions must verify, except for changes propagated from a
  semantic dependency replacement in this same candidate."
  [schema previous replaced check!]
  (let [definitions (catalog schema)]
    (reduce
      (fn [{:keys [schema compiled changed] :as state} id]
        (check!)
        (let [entry (definitions id)
              definition (body entry)
              old (body (previous id))
              artifact (compile-entry id entry definitions compiled)
              semantic-change? (not= (:definition-fingerprint old)
                                     (:definition-fingerprint artifact))
              propagated? (seq (set/intersection changed (dependencies entry)))
              replacing? (contains? replaced id)
              verified? (and (= :federation/v2 (:fingerprint-scheme definition))
                             (= (select-keys definition digest-keys)
                                (select-keys artifact digest-keys))
                             (or (= :source (:datalevin/definition-kind entry))
                                 (= (:accepted definition) (:accepted artifact))))
              version (:version old)]
          (ensure! (or replacing? (and (pos-int? version)
                                      (= :federation/v2 (:fingerprint-scheme definition))
                                      (or verified? propagated?)))
                   :federation/stale-definition {:definition-id id})
          (let [advance? (or replacing? (and propagated? semantic-change?))
                version (if advance? (inc (long (or version 0))) version)
                definition (cond-> (merge (apply dissoc definition digest-keys)
                                         (select-keys artifact digest-keys)
                                         {:version version})
                             (:accepted artifact) (assoc :accepted (:accepted artifact)))]
            (assoc state
                   :schema (assoc schema id (envelope (:datalevin/definition-kind entry) definition))
                   :compiled (assoc compiled id artifact)
                   :changed (cond-> changed semantic-change? (conj id))))))
      {:schema schema :compiled {} :changed #{}}
      (dependency-order definitions check!))))

(defn- attribute-contracts [compiled check!]
  (reduce-kv
    (fn [state id artifact]
      (check!)
      (let [state
            (reduce-kv
              (fn [state domain contract]
                (when-let [old (get-in state [:domains domain])]
                  (ensure! (= old contract) :federation/domain-conflict {:definition-id id}))
                (assoc-in state [:domains domain] contract))
              state (get-in artifact [:logical-schema :domains] {}))]
        (reduce-kv
          (fn [state attribute types]
            (let [contract {:types types :guarantees (get-in artifact [:logical-schema :attributes attribute])}]
              (when-let [old (get-in state [:attributes attribute :contract])]
                (ensure! (= old contract) :federation/attribute-conflict {:attribute attribute}))
              (-> state
                  (assoc-in [:attributes attribute :contract] contract)
                  (update-in [:attributes attribute :members] (fnil conj #{}) id))))
          state (get-in artifact [:pattern-contract :attributes] {}))))
    {:attributes {} :domains {}} compiled))

(defn- native-type [type]
  (cond
    (keyword? type)
    (let [native (keyword "db.type" (name type))]
      (when (contains? c/datalog-value-types native) {:db/valueType native}))

    (and (= :tuple (:kind type))
         (every? #(and (keyword? %)
                       (contains? c/tuple-value-types (keyword "db.type" (name %)))) (:elements type)))
    {:db/valueType :db.type/tuple
     :db/tupleTypes (mapv #(keyword "db.type" (name %)) (:elements type))}

    :else nil))

(defn- memberships [schema compiled check!]
  (let [attributes (:attributes (attribute-contracts compiled check!))
        schema (reduce-kv (fn [m id entry]
                            (check!)
                            (if (contains? entry membership)
                              (assoc m id (dissoc entry membership)) m)) schema schema)]
    (reduce-kv
      (fn [schema attribute {:keys [contract members]}]
        (check!)
        (let [entry (get schema attribute)
              type (get-in contract [:types :value-type])
              native (native-type type)
              cardinality (keyword "db.cardinality" (name (get-in contract [:guarantees :cardinality])))
              actual-type (:db/valueType entry)]
          (ensure! (not (contains? entry :datalevin/definition-kind))
                   :federation/schema-mismatch {:attribute attribute})
          (when entry
            (ensure! (and (= cardinality (get entry :db/cardinality :db.cardinality/one))
                          (or (nil? actual-type)
                              (and (= actual-type (:db/valueType native))
                                   (or (not= actual-type :db.type/tuple)
                                       (= (:db/tupleTypes entry) (:db/tupleTypes native))))))
                     :federation/schema-mismatch {:attribute attribute}))
          (assoc schema attribute
                 (assoc (or entry (assoc native :db/cardinality cardinality)) membership members))))
      schema attributes)))

(defn- validate-memberships! [schema definitions replaced check!]
  (doseq [[attribute entry] schema :when (contains? entry membership)]
    (check!)
    (let [members (get entry membership)]
      (ensure! (and (set? members) (every? keyword? members)
                    (not (contains? entry :datalevin/definition-kind)))
               :federation/membership {:attribute attribute})
      (doseq [id members]
        (let [entry (definitions id)
              accepted (:accepted (body entry))]
          (ensure! (and (= :relation (:datalevin/definition-kind entry))
                        (or (contains? replaced id)
                            (= attribute (get-in accepted [:identity :attribute]))
                            (contains? (:attributes accepted) attribute)))
                   :federation/membership {:attribute attribute :definition-id id}))))))

(defn- patch-schema [schema patches]
  (reduce-kv
    (fn [schema attribute patch]
      (ensure! (and (keyword? attribute) (map? patch)
                    (not (contains? (get schema attribute) :datalevin/definition-kind))
                    (not-any? #(contains? patch %) [:db/aid :datalevin/definition-kind owner membership]))
               :federation/schema-patch {:attribute attribute})
      (update schema attribute
              #(reduce-kv (fn [entry property value]
                            (if (= :db/retract value) (dissoc entry property)
                                (assoc entry property value))) (or % {}) patch)))
    schema patches))

(defn- schema-diff [before after]
  (into {}
        (keep (fn [[id entry]]
                (let [old (get before id)
                      patch (reduce (fn [patch key]
                                      (if (= (find old key) (find entry key)) patch
                                          (assoc patch key (get entry key :db/retract))))
                                    {} (set/union (set (keys old)) (set (keys entry))))]
                  (when (or (nil? old) (seq patch)) [id patch]))))
        after))

(defn- expected-versions! [schema touched expected]
  (ensure! (and (map? expected) (= touched (set (keys expected)))
                (every? #(or (nil? %) (pos-int? %)) (vals expected)))
           :federation/expected-versions {})
  (doseq [id touched]
    (let [entry (get schema id)
          actual (:version (body entry))]
      (ensure! (and (or (nil? entry) (owned? entry))
                    (= (get expected id) actual)
                    (or (nil? entry) (pos-int? actual)))
               :federation/version-conflict
               {:definition-id id :expected (expected id) :actual actual}))))

(defn load-definitions
  "Read and verify persisted format 1 definitions and their accepted v2 mapping
  contracts. Returns {:schema ... :revision ... :compiled {id artifacts}}.
  No discovery, profile resolution, identity generation, or provider I/O occurs."
  [conn]
  (let [{:keys [schema] :as snapshot} (d/schema-snapshot conn)
        check! #(request/check! :federation-load)
        definitions (catalog schema)
        _ (validate-memberships! schema definitions #{} check!)
        compiled (:compiled (compile-catalog schema definitions #{} check!))]
    (ensure! (= schema (memberships schema compiled check!)) :federation/membership {})
    (assoc snapshot :compiled compiled)))

(defn install!
  "Atomically install/refresh format 1 source/relation envelopes. Relation bodies
  hold :source, :accepted (representation 2), and optional :depends-on IDs;
  sources hold an explicit UUID, :provider, and qualified :profile reference.

  Options require :expected-versions for every submitted or removed ID (nil
  means absent). :remove is a set of IDs. :schema-update contains ordinary
  attribute property patches. Counters/digests are computed by the installer.

  Optional :prepare receives {:schema :revision :definitions} and control
  {:check-active! :remaining-ms}, and returns replacements for exactly the
  submitted IDs. It runs before acquiring the writer, and must bound I/O with
  the supplied control. Revision conflicts rerun it on a fresh snapshot.
  :max-attempts defaults to 8, :timeout-ms to 5000; :cancelled? is a predicate.
  Definition-version conflicts never retry. Returns committed schema/revision
  and compiled artifacts only after a successful commit."
  [conn definitions {:keys [expected-versions remove schema-update prepare cancelled? max-attempts timeout-ms]
                     :or {remove #{} schema-update {} max-attempts 8 timeout-ms 5000}}]
  (ensure! (and (map? definitions) (every? keyword? (keys definitions))
                (set? remove) (every? keyword? remove) (map? schema-update)
                (empty? (set/intersection (set (keys definitions)) remove))
                (pos-int? max-attempts) (pos-int? timeout-ms) (<= (long timeout-ms) 86400000)
                (or (nil? prepare) (ifn? prepare)) (or (nil? cancelled?) (ifn? cancelled?)))
           :federation/options {})
  (let [kv (d/datalog-kv conn)
        _ (ensure! (not (or (l/writing? kv) (Thread/holdsLock (l/write-txn kv))))
                   :federation/inside-transaction {})
        own-budget (request/budget timeout-ms 0 nil nil)
        outer (request/current)
        deadline (min (long (:deadline own-budget))
                      (if outer (request/deadline) (long (:deadline own-budget))))]
    (binding [request/*request* (or outer own-budget)
              request/*stage-deadline* deadline]
      (let [check! #(do (request/check! :federation-install)
                       (when (and cancelled? (cancelled?)) (fail! :federation/cancelled {})))
            touched (set/union (set (keys definitions)) remove)
            control {:check-active! check!
                     :remaining-ms #(do (check!) (request/remaining-ms timeout-ms))}]
        (loop [attempt 1]
          (check!)
          (let [{:keys [schema revision] :as snapshot} (d/schema-snapshot conn)
                _ (expected-versions! schema touched expected-versions)
                _ (doseq [id remove]
                    (ensure! (owned? (get schema id)) :federation/definition {:definition-id id}))
                prepared (if prepare (prepare (assoc snapshot :definitions definitions) control) definitions)
                _ (check!)
                _ (ensure! (and (map? prepared) (= (set (keys prepared)) (set (keys definitions))))
                           :federation/preparation {})
                prepared (into {} (map (fn [[id entry]]
                                         [id (if (= :relation (:datalevin/definition-kind entry))
                                               (update entry owner #(merge {:depends-on #{}} %)) entry)])) prepared)
                _ (doseq [[id entry] prepared]
                    (when-let [old (get schema id)]
                      (ensure! (= (:datalevin/definition-kind old) (:datalevin/definition-kind entry))
                               :federation/definition {:definition-id id})))
                _ (validate-memberships! schema (catalog schema) touched check!)
                candidate (merge (apply dissoc (patch-schema schema schema-update) remove) prepared)
                result (compile-catalog candidate (catalog schema) (set (keys prepared)) check!)
                candidate (memberships (:schema result) (:compiled result) check!)
                patch (schema-diff schema candidate)
                _ (check!)
                committed (try (d/update-schema-if-revision conn revision patch (vec remove))
                               (catch clojure.lang.ExceptionInfo e
                                 (if (= :schema/revision-conflict (:error (ex-data e)))
                                   ::conflict (throw e))))]
            (if (= ::conflict committed)
              (do (ensure! (< attempt (long max-attempts)) :federation/retry-limit {:attempts attempt})
                  (recur (inc attempt)))
              (assoc committed :compiled (:compiled result)))))))))

(defn remove!
  "Remove definitions with explicit current versions, rejecting surviving
  dependents. Ordinary native attributes and their data are retained."
  ([conn expected-versions] (remove! conn expected-versions {}))
  ([conn expected-versions options]
   (install! conn {} (assoc options :expected-versions expected-versions
                            :remove (set (keys expected-versions))))))
