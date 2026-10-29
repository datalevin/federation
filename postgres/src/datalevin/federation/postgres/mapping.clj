(ns datalevin.federation.postgres.mapping
  (:require [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.fingerprint :as fp]
            [datalevin.federation.mapping :as mapping]
            [datalevin.federation.postgres.jdbc :as jdbc :refer [ensure! fail!]]))

(def types
  {"int2" :long "int4" :long "int8" :long "bool" :boolean
   "text" :string "varchar" :string "uuid" :uuid "bytea" :bytes
   "numeric" :bigdec "float4" :float "float8" :double "timestamptz" :data
   "date" :data "timestamp" :data "time" :data "timetz" :data "interval" :data})

(def ^:private type-ids
  {"int2" 21 "int4" 23 "int8" 20 "bool" 16 "text" 25 "varchar" 1043
   "uuid" 2950 "bytea" 17 "numeric" 1700 "float4" 700 "float8" 701 "timestamptz" 1184
   "date" 1082 "timestamp" 1114 "time" 1083 "timetz" 1266 "interval" 1186})

(defn storage-type [{:keys [type enum]}] (if enum "enum" type))
(defn logical-type [column] (if (:enum column) :string (types (:type column))))

(defn- supported? [{:keys [type type-id enum] :as column}]
  (if enum
    (and (= type (:name enum)) (pos-int? type-id) (string? (:schema enum))
         (vector? (:labels enum)) (seq (:labels enum)) (every? string? (:labels enum)))
    (and (contains? types type) (= type-id (type-ids type)) (some? (logical-type column)))))

(defn spooled? [accepted]
  (let [r (:requirements accepted)] (or (not= "r" (:kind r)) (:row-security? r))))

(defn- restricted? [catalog] (or (:row-security? catalog) (#{"v" "m"} (:kind catalog))))

(defn accept
  "Accept a catalog capture and explicit attribute/identity declarations."
  [catalog {:keys [relation-id source-instance-id identity-attribute attributes]
            :as declaration}]
  (ensure! (and (every? #{:relation-id :source-instance-id :identity-attribute :attributes :identity-version :key-columns}
                        (keys declaration))
                (keyword? relation-id) (uuid? source-instance-id) (keyword? identity-attribute)
                (map? attributes) (not (contains? attributes identity-attribute))
                (every? keyword? (keys attributes))
                (integer? (get declaration :identity-version 0))
                (<= 0 (get declaration :identity-version 0) Long/MAX_VALUE)
                (#{"r" "p" "v" "m"} (:kind catalog))
                (boolean? (:row-security? catalog))
                (or (not (:row-security? catalog)) (true? (get-in catalog [:security :active?])))
                (or (not (restricted? catalog)) (map? (get-in catalog [:security :authorization])))) :mapping)
  (jdbc/relation (:table catalog))
  (let [view? (boolean (#{"v" "m"} (:kind catalog)))
        key-cols (if view? (:key-columns declaration) (mapv :column (:primary-key catalog)))
        physical (:columns catalog)
        key-types (mapv #(logical-type (physical %)) key-cols)
        _ (ensure! (and (vector? key-cols) (seq key-cols) (<= (count key-cols) 16)
                        (= (count key-cols) (count (set key-cols))) (every? string? key-cols)
                        (if view? (and (map? (:view catalog))
                                      (or (= "v" (:kind catalog)) (true? (get-in catalog [:view :populated?]))))
                          (and (not (contains? declaration :key-columns))
                               (every? #(and (false? (:deferred? %)) (true? (:validated? %))) (:primary-key catalog))))
                        (every? #(and (or view? (true? (:not-null? (physical %)))) (supported? (physical %))
                                      (or (#{:long :string :uuid} (logical-type (physical %)))
                                          (#{"date" "timestamp" "time" "timetz" "timestamptz"} (storage-type (physical %))))) key-cols)) :mapping)
        scope-fingerprint (when (restricted? catalog)
                            (fp/fingerprint :definition (get-in catalog [:security :authorization])))
        domain (cond-> [source-instance-id (:table catalog) (get declaration :identity-version 0)]
                 scope-fingerprint (conj scope-fingerprint))
        attrs (into (sorted-map)
                (map (fn [[a spec]]
                       (let [spec (if (string? spec) {:column spec} spec)
                             _ (ensure! (and (map? spec)
                                             (every? #{:column :columns :reference} (keys spec))) :mapping)
                             columns (if (contains? spec :column) [(:column spec)] (:columns spec))
                             _ (ensure! (and (vector? columns) (seq columns)
                                             (every? #(and (string? %) (supported? (physical %))) columns)) :mapping)
                             ts (mapv #(logical-type (physical %)) columns)
                             reference (:reference spec)]
                         (if reference
                           (ensure! (and (= ts (get-in reference [:type :parameters :key-types]))
                                         (= (:domain reference)
                                            (get-in reference [:type :parameters :identity-domain-id]))) :mapping)
                           (ensure! (= 1 (count columns)) :mapping))
                         [a (merge {:extraction {:shape :value :expression [:columns columns]}
                                    :cardinality :one :null :absent :conversion {:version 1}}
                                   (if reference {:reference reference} {:type (first ts)}))])) attributes))
        used (into (set key-cols) (mapcat #(second (get-in % [:extraction :expression])) (vals attrs)))
        requirements (cond-> (assoc (select-keys catalog [:table :oid :kind :row-security? :database :primary-key :security :view])
                                   :columns (into (sorted-map) (select-keys physical used)))
                       view? (assoc :key-contract {:columns key-cols :proof :snapshot}))
        accepted {:representation-version 2 :relation-id relation-id :source-instance-id source-instance-id
                  :identity {:type (mapping/entity-type ref/type-contract domain key-types)
                             :attribute identity-attribute :scope :durable
                             :extraction {:shape :key-tuple :expressions (mapv #(vector :column %) key-cols)}}
                  :attributes attrs
                  :scope (if scope-fingerprint
                           {:id (str "postgres:" (:oid catalog) ":" scope-fingerprint) :kind :restricted
                            :parameters {:authorization (get-in catalog [:security :authorization])}}
                           {:id (str "postgres:" (:oid catalog) (if (= "p" (:kind catalog)) ":parent" ":only"))
                            :kind :full :parameters {}})
                  :completion {:domain :finite :extent :all-matches}
                  :requirements requirements
                  :dependencies {:provider {:name :postgres :version 1} :conversion-version 1
                                 :identity-version (get declaration :identity-version 0)}}]
    (ensure! (<= (count used) 128) :mapping)
    (mapping/compile-logical-mapping accepted)
    accepted))

(defn compiled! [accepted]
  (let [canonical (accept (:requirements accepted)
                    (cond-> {:relation-id (:relation-id accepted) :source-instance-id (:source-instance-id accepted)
                     :identity-attribute (get-in accepted [:identity :attribute])
                     :identity-version (get-in accepted [:dependencies :identity-version])
                     :attributes (into {} (map (fn [[a body]]
                                               [a (cond-> {:columns (second (get-in body [:extraction :expression]))}
                                                    (:reference body) (assoc :reference (:reference body)))]))
                                       (:attributes accepted))}
                      (get-in accepted [:requirements :key-contract])
                      (assoc :key-columns (get-in accepted [:requirements :key-contract :columns]))))]
    (ensure! (= canonical (update accepted :dependencies dissoc :definition-contracts)) :mapping)
    (mapping/compile-logical-mapping accepted)))

(defn verify! [accepted catalog]
  (let [expected (:requirements accepted)
        actual (cond-> (assoc (select-keys catalog (keys expected))
                               :columns (into (sorted-map) (select-keys (:columns catalog) (keys (:columns expected)))))
                 (:key-contract expected) (assoc :key-contract (:key-contract expected)))]
    (when (and (:security expected)
               (or (not= (get-in expected [:security :authorization]) (get-in actual [:security :authorization]))
                   (and (:row-security? expected) (not (true? (get-in actual [:security :active?]))))))
      (fail! :query-source/unauthorized :acquisition))
    (when-not (= expected actual) (fail! :query-source/stale-definition :acquisition))))

(defn key-columns [accepted]
  (mapv second (get-in accepted [:identity :extraction :expressions])))

(defn attribute-columns [accepted attribute]
  (if (= attribute (get-in accepted [:identity :attribute]))
    (key-columns accepted)
    (second (get-in accepted [:attributes attribute :extraction :expression]))))
