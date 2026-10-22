(ns datalevin.federation.mapping
  "Compilation of accepted metadata. No discovery, installation, or row I/O."
  (:require [datalevin.federation.fingerprint :as fp]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.query.source :as source]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl QueryKey$KeySet]))

(defn- require! [condition]
  (when-not condition
    (throw (ex-info "Invalid accepted federation mapping" {:error :federation/mapping}))))

(defn- definition [schema id kind]
  (let [entry (get schema id)]
    (require! (and (= kind (:datalevin/definition-kind entry))
                   (= #{:datalevin/definition-kind :datalevin.federation/definition}
                      (set (keys entry)))
                   (map? (:datalevin.federation/definition entry))))
    (:datalevin.federation/definition entry)))

(defn entity-type
  "Build a nominal type from an accepted type contract and resolved domain/key."
  [type-contract identity-domain-id key-types]
  (require! (and (some? identity-domain-id) (vector? key-types) (seq key-types)))
  (run! source/validate-type! key-types)
  (source/validate-type!
    (assoc type-contract :kind :custom :parameters {:identity-domain-id identity-domain-id
                                                   :key-types key-types})))

(defn- expression-columns [expression]
  (require! (vector? expression))
  (case (first expression)
    :column (do (require! (and (= 2 (count expression)) (string? (second expression))))
                [(second expression)])
    :tuple (do (require! (> (count expression) 1))
               (vec (mapcat expression-columns (rest expression))))
    (require! false)))

(defn- entity-type-from-reference [type-contract reference]
  (entity-type type-contract (:identity-domain-id reference) (:key-types reference)))

(defn- accepted-fields! [m required optional]
  (require! (and (value/plain-map? m)
                 (every? #(contains? m %) required)
                 (every? #(or (contains? required %) (contains? optional %)
                              (qualified-keyword? %)) (keys m))
                 (value/portable? m))))

(defn- accepted-scope! [scope completion]
  (accepted-fields! scope #{:id :kind :parameters} #{})
  (accepted-fields! completion #{:domain :extent} #{})
  (require! (and (string? (:id scope)) (seq (:id scope))
                 (#{:full :restricted} (:kind scope))
                 (value/plain-map? (:parameters scope))
                 (= :finite (:domain completion))
                 (#{:all-matches :declared-view} (:extent completion)))))

(defn- add-domain [domains id type]
  (require! (some? id))
  (ref/validate-type! type)
  (require! (= id (get-in type [:parameters :identity-domain-id])))
  (when-let [previous (get domains id)]
    (require! (= (:entity-type previous) type)))
  (assoc domains id {:entity-type type}))

(defn normalize-column-mapping
  "Normalize a declared SQL/file column mapping to accepted representation 2.
  Attributes supply explicit :extraction {:shape ... :expression ...}; physical
  validation is local to this normalizer. API normalizers may use path/field
  extraction instead, returning the same common representation. No I/O occurs."
  [schema relation-id {:keys [dependencies] :as context}]
  (let [mapping (definition schema relation-id :relation)
        source-body (definition schema (:source mapping) :source)
        source-id (:source-instance-id source-body)
        entity (:entity mapping)
        key (:key entity)
        key-columns (mapv :column key)
        key-types (mapv :type key)
        physical (:physical-schema mapping)
        columns (into {} (map (juxt :name identity)) (:columns physical))
        attributes (:attributes mapping)
        type-contract (:entity-ref-type dependencies)
        domain (or (:identity-domain-id entity)
                   [source-id (:domain entity) (:identity-epoch entity)])]
    (require! (and (uuid? source-id)
                   (keyword? (:provider source-body))
                   (= (:provider source-body) (get-in dependencies [:provider :name]))
                   (pos-int? (get-in dependencies [:provider :version]))
                   (pos-int? (:conversion-version dependencies))
                   (vector? key) (seq key) (every? string? key-columns)
                   (= (count key-columns) (count (set key-columns)))
                   (keyword? (:domain entity)) (nat-int? (:identity-epoch entity))
                   (#{:durable :export} (:identity-scope entity))
                   (keyword? (:identity-attribute entity))
                   (#{:provider :validated :trusted} (:constraint-source entity))
                   (or (= :trusted (:constraint-source entity))
                       (= key-columns (:primary-key physical))
                       (some #{key-columns} (:unique-keys physical)))
                   (vector? (:from mapping)) (seq (:from mapping))
                   (every? string? (:from mapping))
                   (map? attributes)
                   (= (count columns) (count (:columns physical)))
                   (every? #(and (contains? columns %) (false? (:nullable? (columns %)))) key-columns)))
    (let [attributes
          (into {}
                (map (fn [[a body]]
                       (let [extraction (:extraction body)
                             used (expression-columns (:expression extraction))
                             reference (:reference body)]
                         (require! (every? #(contains? columns %) used))
                         (when reference
                           (require! (and (map? reference) (some? (:identity-domain-id reference))
                                          (vector? (:key-types reference)) (seq (:key-types reference))
                                          ;; Elements may themselves be composite keys in one
                                          ;; array column. The provider validates element shape.
                                          (or (= :elements (:shape extraction))
                                              (= (count used) (count (:key-types reference)))))))
                         [a (cond-> (select-keys body [:extraction :type :cardinality :null :conversion])
                              reference (assoc :reference
                                               {:domain (:identity-domain-id reference)
                                                :type (entity-type-from-reference type-contract reference)}))])))
                attributes)
          used-columns (into (set key-columns)
                             (mapcat #(expression-columns (get-in % [:extraction :expression])))
                             (vals attributes))
          requirements {:columns (mapv #(select-keys (columns %) [:name :physical-type :nullable?])
                                       (sort used-columns))
                        :key {:columns key-columns :constraint-source (:constraint-source entity)}}]
      (doseq [column (:columns requirements)]
        (require! (and (map? (:physical-type column)) (boolean? (:nullable? column)))))
      (cond-> {:representation-version 2 :relation-id relation-id :source-instance-id source-id
               :identity {:type (entity-type type-contract domain key-types)
                          :attribute (:identity-attribute entity)
                          :extraction {:shape :key-tuple :expressions (mapv #(vector :column %) key-columns)}
                          :scope (:identity-scope entity)}
               :attributes attributes :requirements requirements
               :scope (:scope mapping) :completion (:completion mapping)
               :dependencies (assoc dependencies :source-instance-id source-id
                                    :locator (:from mapping) :source (:source mapping))}
        (:domains context) (assoc :domains (:domains context))
        (:view mapping) (assoc :view (:view mapping))))))

(defn compile-logical-mapping
  "Compile provider-normalized accepted metadata, without provider execution.

  :representation-version is 2. :identity has :type, :attribute, :extraction,
  and :scope (:durable or :export). Export identities have two key components:
  the immutable export token and row locator. :attributes declare :extraction
  {:shape :value|:elements :expression portable-data}, :type or a resolved
  :reference {:domain ... :type ...}, :cardinality, :null, and :conversion.
  Provider columns, paths, and extraction proofs remain in accepted metadata;
  the returned logical schema contains only accepted logical guarantees.

  This returns preparation artifacts, never a live SPI descriptor."
  [accepted]
  (accepted-fields! accepted
                    #{:representation-version :relation-id :source-instance-id
                      :identity :attributes :scope :completion :dependencies :requirements}
                    #{:domains :view})
  (require! (and (= 2 (:representation-version accepted))
                 (keyword? (:relation-id accepted))
                 (uuid? (:source-instance-id accepted))
                 (value/plain-map? (:attributes accepted))
                 (value/plain-map? (:dependencies accepted))
                 (keyword? (get-in accepted [:dependencies :provider :name]))
                 (pos-int? (get-in accepted [:dependencies :provider :version]))
                 (pos-int? (get-in accepted [:dependencies :conversion-version]))))
  (accepted-scope! (:scope accepted) (:completion accepted))
  (when (= :declared-view (get-in accepted [:completion :extent]))
    (require! (and (value/plain-map? (:view accepted)) (seq (:view accepted)))))
  (let [{entity-type :type identity-attribute :attribute :as identity} (:identity accepted)
        _ (accepted-fields! identity #{:type :attribute :extraction :scope} #{})
        _ (ref/validate-type! entity-type)
        domain (get-in entity-type [:parameters :identity-domain-id])
        key-types (get-in entity-type [:parameters :key-types])
        _ (accepted-fields! (:extraction identity) #{:shape :expressions} #{})
        _ (require! (and (keyword? identity-attribute)
                         (#{:durable :export} (:scope identity))
                         (= :key-tuple (get-in identity [:extraction :shape]))
                         (vector? (get-in identity [:extraction :expressions]))
                         (= (count key-types) (count (get-in identity [:extraction :expressions])))
                         (every? some? (get-in identity [:extraction :expressions]))
                         (or (not= :export (:scope identity)) (= 2 (count key-types)))
                         (not (contains? (:attributes accepted) identity-attribute))))
        domains (reduce-kv (fn [m id entry]
                             (accepted-fields! entry #{:entity-type} #{})
                             (add-domain m id (:entity-type entry)))
                           {} (get accepted :domains {}))
        domains (add-domain domains domain entity-type)
        attributes (:attributes accepted)
        domains
        (reduce-kv
          (fn [domains attribute body]
            (require! (keyword? attribute))
            (accepted-fields! body #{:extraction :cardinality :null :conversion} #{:type :reference})
            (accepted-fields! (:extraction body) #{:shape :expression} #{})
            (require! (and (#{:one :many} (:cardinality body))
                           (= :absent (:null body))
                           (some? (get-in body [:extraction :expression]))
                           (#{:value :elements} (get-in body [:extraction :shape]))
                           (or (= :many (:cardinality body))
                               (= :value (get-in body [:extraction :shape])))
                           (value/plain-map? (:conversion body))
                           (pos-int? (get-in body [:conversion :version]))
                           (not= (contains? body :type) (contains? body :reference))))
            (if-let [reference (:reference body)]
              (do (accepted-fields! reference #{:domain :type} #{})
                  (add-domain domains (:domain reference) (:type reference)))
              (do (source/validate-type! (:type body)) domains)))
          domains attributes)
        identity-value-type (if (= 1 (count key-types)) (first key-types)
                                {:kind :tuple :elements key-types})
        pattern-contract
        {:kind :eav
         :attributes (into {identity-attribute {:entity-type entity-type :value-type identity-value-type}}
                           (map (fn [[a body]]
                                  [a {:entity-type entity-type
                                      :value-type (or (get-in body [:reference :type]) (:type body))}]))
                           attributes)}
        logical-schema
        {:domains domains
         :attributes
         (into {identity-attribute {:entity-domain domain :cardinality :one
                                   :absence :no-fact :reference-domain nil}}
               (map (fn [[a body]]
                      [a {:entity-domain domain :cardinality (:cardinality body)
                          :absence :no-fact :reference-domain (get-in body [:reference :domain])}]))
               attributes)}
        schema-fingerprint (fp/accepted-fingerprint :schema (:requirements accepted))
        payload {:representation-version 2 :definition-kind :relation
                 :mapping (dissoc accepted :requirements)
                 :schema-fingerprint schema-fingerprint
                 :pattern-contract pattern-contract :logical-schema logical-schema
                 :scope (:scope accepted) :completion (:completion accepted)}
        fingerprint (fp/accepted-fingerprint :definition payload)]
    {:accepted accepted :pattern-contract pattern-contract :logical-schema logical-schema
     :schema-fingerprint schema-fingerprint :definition-fingerprint fingerprint
     :fingerprint-scheme :federation/v2}))

(defn normalize-provider-mapping
  "Run an explicit provider normalizer, then compile its common accepted output.
  A normalizer is a pure function of the declared mapping and captured physical
  metadata; SQL columns, file fields, and API paths meet at the same boundary."
  [normalize mapping physical]
  (require! (ifn? normalize))
  (compile-logical-mapping (normalize mapping physical)))

(defn combine-logical-mappings
  "Validate the union of accepted relations. Distinct mappings contributing to
  one attribute cannot inherit :one; report :unknown unless any declares :many.
  Repeated identical definitions are one member. Scope and complete domain/type
  identity must agree; incompatible unions fail without reading provider data."
  [compiled]
  (require! (and (vector? compiled) (seq compiled)))
  (let [members (mapv compile-logical-mapping (map :accepted compiled))
        by-id (group-by #(get-in % [:accepted :relation-id]) members)
        _ (doseq [duplicates (vals by-id)]
            (require! (= 1 (count (set (map :definition-fingerprint duplicates))))))
        members (mapv first (vals by-id))
        scopes (set (map #(get-in % [:accepted :scope]) members))
        completions (set (map #(get-in % [:accepted :completion]) members))
        _ (require! (and (= 1 (count scopes)) (= 1 (count completions))))
        domains (reduce (fn [domains member]
                          (reduce-kv (fn [m id entry] (add-domain m id (:entity-type entry)))
                                     domains (get-in member [:logical-schema :domains]))) {} members)
        patterns (reduce (fn [patterns member]
                           (reduce-kv (fn [m a types]
                                        (when-let [previous (get m a)] (require! (= previous types)))
                                        (assoc m a types))
                                      patterns (get-in member [:pattern-contract :attributes]))) {} members)
        attributes (reduce (fn [attrs member]
                             (reduce-kv
                               (fn [m a body]
                                 (if-let [previous (get m a)]
                                   (do (require! (= (dissoc previous :cardinality) (dissoc body :cardinality)))
                                       (assoc m a (assoc body :cardinality
                                                         (cond
                                                           (every? #(= a (get-in % [:accepted :identity :attribute]))
                                                                   members) :one
                                                           (or (= :many (:cardinality previous))
                                                               (= :many (:cardinality body))) :many
                                                           :else :unknown))))
                                   (assoc m a body))) attrs (get-in member [:logical-schema :attributes])))
                           {} members)
        logical-schema {:domains domains :attributes attributes}
        pattern-contract {:kind :eav :attributes patterns}
        payload {:representation-version 2 :definition-kind :source
                 :members (vec (sort-by (comp str first)
                                        (mapv #(vector (get-in % [:accepted :relation-id])
                                                       (:definition-fingerprint %)) members)))
                 :pattern-contract pattern-contract :logical-schema logical-schema
                 :scope (first scopes) :completion (first completions)}]
    {:pattern-contract pattern-contract :logical-schema logical-schema
     :scope (first scopes) :completion (first completions)
     :definition-fingerprint (fp/accepted-fingerprint :definition payload)
     :fingerprint-scheme :federation/v2}))

(defn- project-record [accepted entity-key extracted]
  (require! (value/plain-map? extracted))
  (require! (every? #(contains? (:attributes accepted) %) (keys extracted)))
  (let [entity-type (get-in accepted [:identity :type])
        entity (ref/entity-ref entity-type entity-key)
        identity-attribute (get-in accepted [:identity :attribute])
        identity-value (if (= 1 (count entity-key)) (first entity-key) entity-key)
        facts (QueryKey$KeySet.)]
    (.add facts [entity identity-attribute (value/owned-copy identity-value)])
    (doseq [[a body] (:attributes accepted)]
      (let [raw (get extracted a)
            values (case (get-in body [:extraction :shape])
                     :value [raw]
                     :elements (do (require! (or (nil? raw) (vector? raw) (list? raw) (set? raw))) raw))]
        (doseq [x values :when (some? x)]
          (let [x (if-let [reference (:reference body)]
                    (let [t (:type reference)
                          composite? (> (count (get-in t [:parameters :key-types])) 1)]
                      (if (ref/entity-ref? x)
                        (do (require! (= t (value/-logical-type x))) x)
                        (ref/entity-ref t (if composite? x [x]))))
                    (do (require! (value/matches-type? (:type body) x)) (value/owned-copy x)))]
            (.add facts [entity a x])))))
    (vec facts)))

(defn fact-projector
  "Validate immutable compiled metadata once and return a record projector.
  Calls take [entity-key extracted] or [entity-key extracted attributes]. The
  latter projects only the selected declared attributes, plus identity, allowing
  streaming adapters to bound workspace to one attribute of one record.
  Extracted values must already have exact accepted types; no I/O occurs."
  [{:keys [accepted] :as compiled}]
  (require! (= compiled (compile-logical-mapping accepted)))
  (fn
    ([entity-key extracted] (project-record accepted entity-key extracted))
    ([entity-key extracted attributes]
     (require! (and (set? attributes) (every? #(contains? (:attributes accepted) %) attributes)))
     (project-record (update accepted :attributes select-keys attributes) entity-key extracted))))

(defn project-facts
  "Project one provider-normalized record to distinct, non-null EAV facts.
  extracted maps logical attribute names to their explicitly extracted values.
  Conversions precede this boundary and must already be exact. A composite
  reference is one key tuple; :elements supplies a collection of such keys.
  Repeated production reads should prepare fact-projector once."
  [compiled entity-key extracted]
  ((fact-projector compiled) entity-key extracted))

(defn- compiled-source-info [compiled compute-context-id]
  (require! (some? compute-context-id))
  (let [accepted (:accepted compiled)
        descriptor (merge (select-keys compiled [:pattern-contract :logical-schema :definition-fingerprint])
                          (select-keys accepted [:source-instance-id :scope :completion])
                          {:spi-versions #{2} :features #{} :required-features #{}
                           :compute-context-id compute-context-id :definition-id (:relation-id accepted)
                           :read-capabilities #{:snapshot :statement :best-effort}})]
    (source/validate-source-info! descriptor)
    descriptor))

(defn source-info
  "Compile accepted representation 2 into the common v2 EAV descriptor. Use
  persisted :accepted metadata after load-definitions, supplying a runtime
  compute-context ID. Adapters narrow advertised read capabilities to their
  actual guarantees. This function performs no discovery or provider I/O."
  [accepted compute-context-id]
  (compiled-source-info (compile-logical-mapping accepted) compute-context-id))

(defn compile-relation
  "Compile accepted provider metadata and emit a complete v2 EAV descriptor.
  No discovery, installation or provider execution occurs."
  [schema relation-id {:keys [compute-context-id] :as context}]
  (let [mapping (definition schema relation-id :relation)
        compiled (normalize-provider-mapping
                   (fn [declared captured]
                     (normalize-column-mapping declared relation-id captured)) schema context)
        accepted (:accepted compiled)
        descriptor (compiled-source-info compiled compute-context-id)]
    {:definition (merge mapping (select-keys compiled [:schema-fingerprint :definition-fingerprint :fingerprint-scheme])
                        {:dependencies (:dependencies accepted)})
     :source-info descriptor}))
