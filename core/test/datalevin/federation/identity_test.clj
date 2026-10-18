(ns datalevin.federation.identity-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.fingerprint :as fp]
            [datalevin.federation.mapping :as mapping]
            [datalevin.query.source.value :as value]
            [datalevin.udf :as udf]
            [taoensso.nippy :as nippy])
  (:import [java.math BigInteger]
           [java.util Arrays Date]
           [datalevin.utl QueryKey QueryKey$KeySet]))

(def source-id #uuid "11111111-1111-4111-8111-111111111111")
(defn type-for
  ([types] (type-for [source-id :order 0] types))
  ([domain types] (mapping/entity-type ref/type-contract domain types)))

(deftest nominal-type-and-key-identity
  (let [t (type-for [:string :long])
        x (ref/entity-ref t ["tenant-a" 7])
        equivalents [t (with-meta t {:note "irrelevant"})
                     (into (array-map) (reverse (seq t)))
                     (assoc-in t [:parameters :key-types] [:string :long])]
        different-types [(assoc t :version 2)
                         (assoc t :fingerprint "other")
                         (assoc t :test/extra "semantic")
                         (assoc-in t [:parameters :identity-domain-id] [source-id :order 1])
                         (assoc-in t [:parameters :identity-domain-id] [source-id :other 0])
                         (assoc-in t [:parameters :identity-domain-id]
                                   [#uuid "22222222-2222-4222-8222-222222222222" :order 0])]]
    (is (= t (value/-logical-type x)))
    (is (= ["tenant-a" 7] (ref/key-tuple x)))
    (doseq [same equivalents]
      (let [y (ref/entity-ref same ["tenant-a" 7])]
        (is (= x y))
        (is (= (.hashCode x) (.hashCode y) (hash x) (hash y)))
        (is (zero? (compare x y)))
        (is (Arrays/equals (ref/encode x) (ref/encode y)))))
    (doseq [different different-types]
      (let [y (ref/entity-ref different ["tenant-a" 7])]
        (is (not= x y))
        (is (not (zero? (compare x y))))))
    (is (not= x (ref/entity-ref t ["tenant-b" 7])))
    (is (not= x (ref/entity-ref t ["tenant-a" 8])))
    (is (= "#<EntityRef>" (str x)))
    (is (<= 64 (value/-retained-bytes x)))))

(deftest descriptor-equivalence-hash-and-order-laws
  (doseq [[a b] [[1 1N] [1M 1.00M] [(float 1.1) (double (float 1.1))]
                 [0.0 -0.0] [[1 2] '(1 2)]
                 [{:a #{1 2} :b [3]} (array-map :b '(3) :a #{2N 1N})]]]
    (let [t (type-for [:long])
          x (ref/entity-ref (assoc t :test/equivalent a) [1])
          y (ref/entity-ref (assoc t :test/equivalent b) [1])]
      (is (= a b))
      (is (= x y))
      (is (= (hash x) (hash y)))
      (is (zero? (compare x y)))
      (is (Arrays/equals (ref/encode x) (ref/encode y)))))
  (let [t (type-for [:long])
        values (mapv #(ref/entity-ref t [%]) [Long/MIN_VALUE -1 0 1 Long/MAX_VALUE])]
    (doseq [x values y values]
      (is (= (= x y) (zero? (compare x y))))
      (is (= (compare (compare x y) 0) (- (compare (compare y x) 0))))
      (is (= (compare (compare x y) 0)
             (compare (Arrays/compareUnsigned (ref/encode x) (ref/encode y)) 0))))
    (is (= (sort values) (sort (reverse values))))))

(deftest exact-runtime-key-semantics
  (doseq [[t keys]
          [[:bigdec [1M 1.00M 2M]]
           [:bigint [1N (BigInteger. "1") 2N]]
           [:double [0.0 -0.0 Double/NaN Double/POSITIVE_INFINITY]]
           [:bytes [(byte-array [1 2]) (byte-array [1 2]) (byte-array [1 3])]]
           [:instant [(Date. 0) (Date. 0) (Date. 1)]]
           [{:kind :tuple :elements [:string :bytes]}
            [["x" (byte-array [1])] ["x" (byte-array [1])] ["y" (byte-array [1])]]]]]
    (let [type (type-for [t])]
      (doseq [a keys b keys]
        (let [x (ref/entity-ref type [a]) y (ref/entity-ref type [b])]
          (is (= (QueryKey/equals a b) (= x y)))
          (is (= (= x y) (zero? (compare x y))))
          (when (= x y) (is (= (hash x) (hash y))))
          (is (= x (ref/decode (ref/encode x)))))))))

(deftest data-key-equality-agrees-with-query-keys
  (let [t (type-for [:data])
        leaves [1 1N (BigInteger. "1") (int 1) 1M 1.00M 1.0
                0.0 -0.0 nil "a" :a (byte-array [1]) (byte-array [1])]
        values (vec (concat (remove nil? leaves) (map vector leaves)
                            (map #(hash-map :value %) leaves) (map #(hash-set %) leaves)
                            [[1] '(1) {:a [1]} {:a '(1)}]))]
    (doseq [a values b values]
      (let [x (ref/entity-ref t [a]) y (ref/entity-ref t [b])]
        (is (= (QueryKey/equals a b) (= x y)) (str (pr-str a) " " (pr-str b)))))))

(deftest hashed-descriptor-extras-preserve-complete-type-equality
  (let [a 1M b 1.00M
        values [a b [a] [b] #{a} #{b} {a :value} {b :value}
                {:value a} {:value b} #{{:value a}} #{{:value b}}]
        t (type-for [:long])]
    (doseq [a values b values]
      (let [ta (assoc t :test/extra a) tb (assoc t :test/extra b)
            x (ref/entity-ref ta [1]) y (ref/entity-ref tb [1])]
        (is (= (= ta tb) (= x y)))
        (is (= ta (value/-logical-type x)))
        (is (= x (ref/decode (ref/encode x))))
        (when (= x y) (is (= (hash x) (hash y)))))))
  (let [t (type-for [:long])]
    (doseq [extra [#{(float 1.1)} {(float 1.1) :value} #{{:value (float 1.1)}}]]
      (is (thrown? clojure.lang.ExceptionInfo (ref/entity-ref (assoc t :test/extra extra) [1]))))))

(deftest references-own-mutable-keys-and-drop-backing-metadata
  (let [type (type-for [:bytes :instant])
        bytes (byte-array [1 2 3]) date (Date. 42)
        x (ref/entity-ref type [bytes date])
        expected (ref/entity-ref type [(byte-array [1 2 3]) (Date. 42)])]
    (aset-byte bytes 0 (byte 9))
    (.setTime date 99)
    (let [[copied-bytes copied-date] (ref/key-tuple x)]
      (aset-byte copied-bytes 0 (byte 8))
      (.setTime ^Date copied-date 100))
    (is (= expected x))
    (is (= (hash expected) (hash x))))
  (let [type (type-for [:long])
        x (ref/entity-ref (with-meta type {:test/doc "does not affect identity"})
                          (with-meta [1] {:test/doc "not a key component"}))]
    (is (nil? (meta (value/-logical-type x))))
    (is (nil? (meta (ref/key-tuple x))))))

(deftest export-scope-and-reconnects
  (let [type (type-for [:uuid {:kind :tuple :elements [:string :long]}])
        export-a #uuid "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        export-b #uuid "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        x (ref/entity-ref type [export-a ["part-1" 42]])]
    (is (= x (ref/entity-ref type [export-a ["part-1" 42]])))
    (is (= x (ref/decode (ref/encode x))))
    (is (not= x (ref/entity-ref type [export-b ["part-1" 42]])))
    (is (not= x (ref/entity-ref type [export-a ["part-1" 43]])))
    (is (not= x (ref/entity-ref type [export-a ["part-2" 42]])))))

(deftest invalid-types-keys-and-bounded-codec
  (let [t (type-for [:string :long]) x (ref/entity-ref t ["secret-value" 7]) bytes (ref/encode x)]
    (doseq [type [(assoc t :unknown 1) (assoc t :name :other/type)
                  (assoc-in t [:parameters :key-types] [])
                  (assoc-in t [:parameters :identity-domain-id] nil)]]
      (is (thrown? clojure.lang.ExceptionInfo (ref/entity-ref type ["tenant" 7]))))
    (doseq [key [["tenant"] ["tenant" 7 8] ["tenant" nil] [nil 7] ["tenant" 7.0] '("tenant" 7)]]
      (is (thrown? clojure.lang.ExceptionInfo (ref/entity-ref t key))))
    (doseq [bad [(byte-array 0) (Arrays/copyOf bytes (dec (alength bytes)))
                 (byte-array (concat bytes [0]))
                 (doto (aclone bytes) (aset-byte 0 (byte 0)))
                 (doto (aclone bytes) (aset-byte 4 (byte 127)))]]
      (try (ref/decode bad) (is false "malformed wire accepted")
           (catch clojure.lang.ExceptionInfo e
             (is (= {:error :federation/entity-ref} (ex-data e)))
             (is (nil? (.getCause e)))
             (is (not (.contains (str e) "secret-value"))))))
    (doseq [limits [(assoc ref/codec-limits :max-bytes 20)
                     (assoc ref/codec-limits :max-depth 2)
                     (assoc ref/codec-limits :max-nodes 2)]]
      (is (thrown? clojure.lang.ExceptionInfo (ref/decode bytes limits)))
      (is (thrown? clojure.lang.ExceptionInfo (ref/encode x limits)))
      (is (thrown? clojure.lang.ExceptionInfo (ref/entity-ref t ["tenant" 7] limits))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (ref/entity-ref (assoc t :test/deep (nth (iterate vector :leaf) 100)) ["tenant" 7])))))

(deftest nested-reference-keys-observe-the-same-construction-and-decode-limits
  (loop [t (type-for [:long]) x (ref/entity-ref (type-for [:long]) [1]) depth 0]
    (is (= x (ref/decode (ref/encode x))))
    (is (= (hash x) (hash (ref/decode (ref/encode x)))))
    (when (< depth 6)
      (let [outer (type-for [t])]
        (recur outer (ref/entity-ref outer [x]) (inc depth))))))

(deftest nippy-and-explicit-storage-hooks
  (let [x (ref/entity-ref (type-for [:long]) [7])
        registry (udf/create-registry)]
    (is (= x (nippy/thaw (nippy/freeze x))))
    (is (= (:fingerprint ref/type-contract) (fp/fingerprint :definition ref/storage-definition)))
    (is (identical? registry (ref/bind-codec! registry)))
    (is (= x ((udf/resolve registry (get-in ref/storage-definition [:payload :deserialize]))
              ((udf/resolve registry (get-in ref/storage-definition [:payload :serialize])) x))))
    (is (Arrays/equals (ref/encode x)
                       ((udf/resolve registry (get-in ref/storage-definition [:index :order-fn])) x)))
    (let [keys (QueryKey$KeySet.)]
      (.add keys x)
      (.add keys (ref/decode (ref/encode x)))
      (is (= 1 (.size keys))))))

(defn- unhex [hex]
  (byte-array (map #(unchecked-byte (Integer/parseInt (apply str %) 16)) (partition 2 hex))))

(deftest independent-portable-golden-fixtures
  (let [inputs (edn/read-string (slurp (io/resource "fixtures/entity-ref-inputs.edn")))
        goldens (into {} (comp (remove #(str/starts-with? % "#"))
                               (map #(str/split % #"\t")))
                       (str/split-lines (slurp (io/resource "fixtures/entity-ref-golden.tsv"))))]
    (is (= (set (map :id inputs)) (set (keys goldens))))
    (doseq [{:keys [id domain key-types key key-hex extras version fingerprint]} inputs]
      (let [type (merge (mapping/entity-type {:name ref/type-name :version (or version 1)
                                              :fingerprint (or fingerprint "fixture-entity-ref:wire-v1")}
                                             domain key-types) extras)
            key (if key-hex [(unhex key-hex)] key)
            x (ref/entity-ref type key)
            expected (get goldens id)
            decoded (ref/decode (unhex expected))]
        (is (= expected (fp/hex (ref/encode x))) id)
        (is (= x decoded) id)
        (is (= type (value/-logical-type decoded)) id)
        (is (QueryKey/equals key (ref/key-tuple decoded)) id)))))
