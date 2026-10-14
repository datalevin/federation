(ns datalevin.federation.mapping
  "F0 compilation of accepted metadata. No discovery, installation, or row I/O."
  (:require [datalevin.federation.fingerprint :as fp]
            [datalevin.query.source :as source]))

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

(defn compile-relation
  "Compile one accepted :relation and its :source entry to an EAV descriptor.

  Context supplies :compute-context-id and recorded :dependencies containing
  :provider, :conversion-version and :entity-ref-type. Resolved references use
  {:identity-domain-id ... :key-types [...]}; unresolved aliases are rejected.
  Returns {:definition accepted-body-with-digests :source-info descriptor}."
  [schema relation-id {:keys [compute-context-id dependencies]}]
  (let [mapping (definition schema relation-id :relation)
        source-body (definition schema (:source mapping) :source)
        source-id (:source-instance-id source-body)
        entity (:entity mapping)
        key (:key entity)
        key-columns (mapv :column key)
        key-types (mapv :type key)
        type-contract (:entity-ref-type dependencies)
        domain (or (:identity-domain-id entity)
                   [source-id (:domain entity) (:identity-epoch entity)])
        entity-type (entity-type type-contract domain key-types)
        identity-attribute (:identity-attribute entity)
        physical (:physical-schema mapping)
        columns (into {} (map (juxt :name identity)) (:columns physical))
        attributes (:attributes mapping)]
    (require! (and (uuid? source-id) (map? dependencies)
                   (keyword? (:provider source-body))
                   (= (:provider source-body) (get-in dependencies [:provider :name]))
                   (pos-int? (get-in dependencies [:provider :version]))
                   (pos-int? (:conversion-version dependencies))
                   (= :datalevin.federation/entity-ref (:name type-contract))
                   (vector? key) (seq key) (every? string? key-columns)
                   (= (count key-columns) (count (set key-columns)))
                   (keyword? (:domain entity)) (nat-int? (:identity-epoch entity))
                   (keyword? identity-attribute) (map? attributes)
                   (not (contains? attributes identity-attribute))
                   (#{:provider :validated :trusted} (:constraint-source entity))
                   (or (= :trusted (:constraint-source entity))
                       (= key-columns (:primary-key physical))
                       (some #{key-columns} (:unique-keys physical)))
                   (vector? (:from mapping)) (seq (:from mapping))
                   (every? string? (:from mapping))
                   (= (count columns) (count (:columns physical)))
                   (every? #(and (contains? columns %) (false? (:nullable? (columns %)))) key-columns)))
    (let [logical-attributes
          (into {}
                (map (fn [[attribute body]]
                       (require! (and (keyword? attribute) (= :one (:cardinality body))
                                      (= :absent (:null body))
                                      (every? #(contains? columns %) (expression-columns (:expression body)))
                                      (not= (contains? body :type) (contains? body :reference))))
                       (when-let [reference (:reference body)]
                         (require! (and (map? reference) (some? (:identity-domain-id reference))
                                        (vector? (:key-types reference)) (seq (:key-types reference))
                                        (= (count (expression-columns (:expression body)))
                                           (count (:key-types reference))))))
                       [attribute (select-keys body [:expression :type :reference :cardinality :null])]))
                attributes)
          pattern-contract
          {:kind :eav
           :attributes
           (into {identity-attribute
                  {:entity-type entity-type
                   :value-type (if (= 1 (count key-types)) (first key-types)
                                   {:kind :tuple :elements key-types})}}
                 (map (fn [[attribute body]]
                        [attribute {:entity-type entity-type
                                    :value-type (if-let [reference (:reference body)]
                                                  (entity-type-from-reference type-contract reference)
                                                  (:type body))}])) logical-attributes)}
          used-columns (into (set key-columns)
                             (mapcat #(expression-columns (:expression %)))
                             (vals logical-attributes))
          requirements {:columns (mapv #(select-keys (columns %) [:name :physical-type :nullable?])
                                        (sort used-columns))
                        :key {:columns key-columns :constraint-source (:constraint-source entity)}}
          _ (doseq [column (:columns requirements)]
              (require! (and (map? (:physical-type column)) (boolean? (:nullable? column)))))
          schema-fingerprint (fp/fingerprint :schema requirements)
          logical-mapping {:source (:source mapping) :from (:from mapping)
                           :entity (select-keys entity [:domain :identity-domain-id :identity-epoch
                                                        :key :identity-attribute])
                           :attributes logical-attributes}
          dependencies (assoc dependencies :source-instance-id source-id)
          fingerprint (fp/fingerprint :definition
                                      {:definition-kind :relation :mapping logical-mapping
                                       :schema-fingerprint schema-fingerprint
                                       :pattern-contract pattern-contract :dependencies dependencies})
          descriptor {:spi-versions #{1} :features #{} :required-features #{}
                      :source-instance-id source-id :compute-context-id compute-context-id
                      :definition-id relation-id :definition-fingerprint fingerprint
                      :pattern-contract pattern-contract}]
      (source/validate-source-info! descriptor)
      {:definition (assoc mapping :schema-fingerprint schema-fingerprint
                          :definition-fingerprint fingerprint :dependencies dependencies
                          :fingerprint-scheme :federation/v1)
       :source-info descriptor})))
