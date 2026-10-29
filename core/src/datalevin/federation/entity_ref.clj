(ns datalevin.federation.entity-ref
  "Nominal, immutable external entity identities. No provider or registry I/O.

  The codec is companion-owned and bounded. Nippy delegates to the same codec;
  it never invokes an arbitrary custom reader. Wire revision 1 is experimental."
  (:require [datalevin.federation.fingerprint :as fp]
            [datalevin.query.source :as source]
            [datalevin.query.source.value :as value]
            [datalevin.udf :as udf]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang BigInt IHashEq Ratio]
           [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream]
           [java.math BigDecimal BigInteger]
           [java.nio ByteBuffer]
           [java.nio.charset CodingErrorAction StandardCharsets]
           [java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetTime Period ZoneOffset]
           [java.util Arrays Date UUID]
           [datalevin.utl QueryKey]))

(def type-name :datalevin.federation/entity-ref)
(def codec-limits {:max-bytes 1048576 :max-nodes 65536 :max-depth 64})
(def ^:private magic [69 82 70 1])

(def storage-definition
  "Explicit database registration input. Loading this namespace registers no DB
  type; applications register this definition and bind its UDFs on each reopen."
  {:version 1
   :index {:type :bytes
           :order-fn {:udf/lang :jvm :udf/kind :order-fn
                      :udf/id :datalevin.federation/entity-ref-order :udf/version 1}}
   :payload {:serialize {:udf/lang :jvm :udf/kind :serializer
                         :udf/id :datalevin.federation/entity-ref-encode :udf/version 1}
             :deserialize {:udf/lang :jvm :udf/kind :deserializer
                           :udf/id :datalevin.federation/entity-ref-decode :udf/version 1}}})

(def type-contract
  "The registered runtime definition's identity, before nominal parameters."
  {:name type-name :version 1
   :fingerprint (fp/fingerprint :definition storage-definition)})

(defn- invalid! []
  (throw (ex-info "Invalid external entity reference" {:error :federation/entity-ref})))

(defn- ensure! [condition] (when-not condition (invalid!)))

(defn- utf8 ^bytes [^String s]
  (let [encoder (doto (.newEncoder StandardCharsets/UTF_8)
                  (.onMalformedInput CodingErrorAction/REPORT)
                  (.onUnmappableCharacter CodingErrorAction/REPORT))
        buffer (.encode encoder (java.nio.CharBuffer/wrap s))
        out (byte-array (.remaining buffer))]
    (.get buffer out)
    out))

(defn- unsigned-compare [a b]
  (let [c (compare (count a) (count b))]
    (if (zero? c) (compare a b) c)))

(defn- immutable-bytes [^bytes bytes]
  ;; Latin-1 is an exact byte-to-character mapping. String owns its backing,
  ;; remains immutable even through public deftype fields, and avoids boxing
  ;; every wire byte into a persistent vector. Never expose mutable arrays.
  (String. bytes StandardCharsets/ISO_8859_1))

(defn- byte-array-of ^bytes [^String bytes]
  (.getBytes bytes StandardCharsets/ISO_8859_1))

(defn- limits! [limits]
  (ensure! (and (map? limits) (= (set (keys codec-limits)) (set (keys limits)))
                (every? (fn [[k n]] (and (pos-int? n) (<= n (get codec-limits k)))) limits)))
  limits)

(defn- budget [limits]
  {:limits (limits! limits) :nodes (volatile! 0) :depth (volatile! 0)})

(defn- node! [{:keys [limits nodes] :as state} depth]
  (ensure! (and (<= depth (:max-depth limits))
                (<= (vswap! nodes inc) (:max-nodes limits))))
  (vswap! (:depth state) max depth))

(defn- preflight!
  "Walk before normalization or type validation can allocate or recurse."
  [x state depth metadata?]
  (node! state depth)
  (let [cap (get-in state [:limits :max-bytes])]
    (cond
      (string? x) (ensure! (<= (count x) cap))
      (bytes? x) (ensure! (<= (alength ^bytes x) cap))
      (or (instance? BigInteger x) (instance? BigInt x))
      (ensure! (<= (.bitLength (biginteger x)) cap))
      (decimal? x) (ensure! (<= (.bitLength (.unscaledValue ^BigDecimal x)) cap))
      (ratio? x) (do (ensure! (<= (.bitLength (.numerator ^Ratio x)) cap))
                     (ensure! (<= (.bitLength (.denominator ^Ratio x)) cap)))
      (value/plain-map? x)
      (do (ensure! (<= (*' 2 (count x)) (- (get-in state [:limits :max-nodes]) @(:nodes state))))
          (doseq [[k v] x] (preflight! k state (inc depth) metadata?)
                  (preflight! v state (inc depth) metadata?)))
      (or (vector? x) (list? x) (set? x))
      (do (ensure! (<= (count x) (- (get-in state [:limits :max-nodes]) @(:nodes state))))
          (run! #(preflight! % state (inc depth) metadata?) x))))
  (when (and metadata? (meta x))
    (preflight! (meta x) state (inc depth) true)))

(defn- metadata-normalize
  ;; Complete type descriptors use Clojure map equality, including numeric and
  ;; sequential equivalence. Keys retain their declared runtime scalar types.
  ([x] (metadata-normalize x false))
  ([x hashed?]
   ;; Float and Double can be equivalent with different Clojure hash-trie
   ;; routing, making equality depend on the surrounding trie. Such hashed
   ;; Float values need an explicit provider normalization contract first.
   (ensure! (not (and hashed? (instance? Float x))))
   (let [number (cond
                  (integer? x) (if (<= Long/MIN_VALUE x Long/MAX_VALUE) (long x) (biginteger x))
                  (decimal? x) (.stripTrailingZeros ^BigDecimal x)
                  (or (double? x) (float? x)) (if (zero? (double x)) 0.0 (double x))
                  :else nil)]
     (cond
       ;; Clojure hashed lookup can distinguish equivalent floating-point
       ;; representations with different hasheq values. Preserve that distinction
       ;; inside set members and map keys, including their nested values.
       (some? number) (if (and hashed? (not= (hash number) (hash x))) x number)
       (map? x) (into {} (map (fn [[k v]] [(metadata-normalize k true)
                                          (metadata-normalize v hashed?)])) x)
       (set? x) (into #{} (map #(metadata-normalize % true)) x)
       (sequential? x) (mapv #(metadata-normalize % hashed?) x)
       (symbol? x) (with-meta x nil)
       :else x))))

(defn- key-normalize [x]
  ;; QueryKey uses Java leaf equality, with Clojure's ordinary hashed lookup
  ;; semantics inside collections without bytes. Collections containing bytes
  ;; instead recurse through QueryKey, including their keys/set members.
  (cond
    (value/plain-map? x)
    (let [bytes? (QueryKey/containsBytes x)]
      (into {} (map (fn [[k v]] [(if bytes? (key-normalize k) (metadata-normalize k true))
                                 (key-normalize v)])) x))
    (set? x) (into #{} (map (if (QueryKey/containsBytes x) key-normalize
                               #(metadata-normalize % true))) x)
    (or (vector? x) (list? x)) (mapv key-normalize x)
    :else x))

(declare encode-node decode-node entity-ref encode decode decode-reference)

(defn- payload! [^DataOutputStream out ^bytes bytes {:keys [limits]}]
  (ensure! (<= (alength bytes) (:max-bytes limits)))
  (.writeInt out (alength bytes))
  (.write out bytes))

(defn- text! [out x state]
  (let [s (str x)]
    ;; UTF-8 needs at least one byte per UTF-16 unit. Reject giant values
    ;; before asking the charset encoder for an allocation.
    (ensure! (<= (count s) (get-in state [:limits :max-bytes])))
    (payload! out (utf8 s) state)))

(defn- encode-child ^bytes [x state depth]
  (let [bytes (ByteArrayOutputStream.)
        out (DataOutputStream. bytes)]
    (encode-node out x state depth)
    (ensure! (<= (.size bytes) (get-in state [:limits :max-bytes])))
    (.toByteArray bytes)))

(defn- encode-node [^DataOutputStream out x state depth]
  (node! state depth)
  (cond
    (nil? x) (.writeByte out 0)
    (false? x) (.writeByte out 1)
    (true? x) (.writeByte out 2)
    (instance? Long x) (do (.writeByte out 3) (.writeLong out (long x)))
    (instance? BigInt x) (do (.writeByte out 4) (text! out x state))
    (instance? BigInteger x) (do (.writeByte out 5) (text! out x state))
    (instance? Double x) (do (.writeByte out 6) (.writeLong out (Double/doubleToLongBits x)))
    (instance? Float x) (do (.writeByte out 7) (.writeInt out (Float/floatToIntBits x)))
    (instance? BigDecimal x) (do (.writeByte out 8) (.writeInt out (.scale ^BigDecimal x))
                               (text! out (.unscaledValue ^BigDecimal x) state))
    (string? x) (do (.writeByte out 9) (text! out x state))
    (keyword? x) (do (.writeByte out 10) (text! out (or (namespace x) "") state)
                    (text! out (name x) state))
    (symbol? x) (do (.writeByte out 11) (text! out (or (namespace x) "") state)
                   (text! out (name x) state))
    (uuid? x) (do (.writeByte out 12) (.writeLong out (.getMostSignificantBits ^UUID x))
                 (.writeLong out (.getLeastSignificantBits ^UUID x)))
    (= Date (class x)) (do (.writeByte out 13) (.writeLong out (.getTime ^Date x)))
    (bytes? x) (do (.writeByte out 14) (payload! out x state))
    (or (vector? x) (list? x))
    (do (.writeByte out 15) (.writeInt out (count x))
        (doseq [child x] (encode-node out child state (inc depth))))
    (value/plain-map? x)
    (let [entries (sort (fn [[ka va] [kb vb]]
                          (let [c (unsigned-compare ka kb)]
                            (if (zero? c) (unsigned-compare va vb) c)))
                        (mapv (fn [[k v]] [(immutable-bytes (encode-child k state (inc depth)))
                                           (immutable-bytes (encode-child v state (inc depth)))]) x))]
      (.writeByte out 17) (.writeInt out (count entries))
      (doseq [[k v] entries]
        (.write out (byte-array-of k)) (.write out (byte-array-of v))))
    (set? x)
    (let [items (sort unsigned-compare (mapv #(immutable-bytes (encode-child % state (inc depth))) x))]
      (.writeByte out 18) (.writeInt out (count items))
      (doseq [child items] (.write out (byte-array-of child))))
    (char? x) (do (.writeByte out 19) (.writeChar out (int x)))
    (ratio? x) (do (.writeByte out 20) (text! out (.numerator ^Ratio x) state)
                  (text! out (.denominator ^Ratio x) state))
    (satisfies? value/IQuerySourceValue x)
    (do (.writeByte out 21) (payload! out (encode x (:limits state)) state))
    (instance? Integer x) (do (.writeByte out 22) (.writeInt out (int x)))
    (instance? Short x) (do (.writeByte out 23) (.writeShort out (int x)))
    (instance? Byte x) (do (.writeByte out 24) (.writeByte out (int x)))
    (instance? Instant x) (do (.writeByte out 25) (.writeLong out (.getEpochSecond ^Instant x))
                            (.writeInt out (.getNano ^Instant x)))
    (instance? Duration x) (do (.writeByte out 26) (.writeLong out (.getSeconds ^Duration x))
                             (.writeInt out (.getNano ^Duration x)))
    (instance? Period x) (do (.writeByte out 27) (.writeInt out (.getYears ^Period x))
                           (.writeInt out (.getMonths ^Period x)) (.writeInt out (.getDays ^Period x)))
    (instance? LocalDate x) (do (.writeByte out 28) (.writeLong out (.toEpochDay ^LocalDate x)))
    (instance? LocalTime x) (do (.writeByte out 29) (.writeLong out (.toNanoOfDay ^LocalTime x)))
    (instance? LocalDateTime x)
    (do (.writeByte out 30) (.writeLong out (.toEpochDay (.toLocalDate ^LocalDateTime x)))
        (.writeLong out (.toNanoOfDay (.toLocalTime ^LocalDateTime x))))
    (instance? OffsetTime x)
    (do (.writeByte out 31) (.writeLong out (.toNanoOfDay (.toLocalTime ^OffsetTime x)))
        (.writeInt out (.getTotalSeconds (.getOffset ^OffsetTime x))))
    :else (invalid!))
  (ensure! (<= (.size out) (get-in state [:limits :max-bytes]))))

(defn- length! [^DataInputStream in {:keys [limits]}]
  (let [n (.readInt in)]
    (ensure! (<= 0 n (min (.available in) (:max-bytes limits))))
    n))

(defn- read-payload ^bytes [^DataInputStream in state]
  (let [bytes (byte-array (length! in state))]
    (.readFully in bytes)
    bytes))

(defn- read-text [in state]
  (str (.decode (doto (.newDecoder StandardCharsets/UTF_8)
                  (.onMalformedInput CodingErrorAction/REPORT)
                  (.onUnmappableCharacter CodingErrorAction/REPORT))
                (ByteBuffer/wrap (read-payload in state)))))

(defn- read-count [^DataInputStream in {:keys [limits nodes]} minimum]
  (let [n (.readInt in)]
    (ensure! (and (<= 0 n (- (:max-nodes limits) @nodes))
                  (<= (*' n minimum) (.available in))))
    n))

(defn- decode-node [^DataInputStream in state depth]
  (node! state depth)
  (case (.readUnsignedByte in)
    0 nil
    1 false
    2 true
    3 (.readLong in)
    4 (bigint (BigInteger. ^String (read-text in state)))
    5 (BigInteger. ^String (read-text in state))
    6 (Double/longBitsToDouble (.readLong in))
    7 (Float/intBitsToFloat (.readInt in))
    8 (let [scale (.readInt in)] (BigDecimal. (BigInteger. ^String (read-text in state)) scale))
    9 (read-text in state)
    10 (let [ns (read-text in state) n (read-text in state)] (keyword (not-empty ns) n))
    11 (let [ns (read-text in state) n (read-text in state)] (symbol (not-empty ns) n))
    12 (UUID. (.readLong in) (.readLong in))
    13 (Date. (.readLong in))
    14 (read-payload in state)
    15 (mapv (fn [_] (decode-node in state (inc depth))) (range (read-count in state 1)))
    17 (let [n (read-count in state 2)
             result (reduce (fn [m _]
                              (let [k (decode-node in state (inc depth))
                                    v (decode-node in state (inc depth))]
                                (ensure! (not (contains? m k)))
                                (assoc m k v))) {} (range n))]
         result)
    18 (let [n (read-count in state 1)
             items (mapv (fn [_] (decode-node in state (inc depth))) (range n))]
         (ensure! (= n (count (set items))))
         (set items))
    19 (.readChar in)
    20 (let [a (BigInteger. ^String (read-text in state))
             b (BigInteger. ^String (read-text in state))]
         (ensure! (pos? (.signum b)))
         (/ a b))
    21 (decode-reference (read-payload in state) state (inc depth))
    22 (Integer/valueOf (.readInt in))
    23 (Short/valueOf (.readShort in))
    24 (Byte/valueOf (.readByte in))
    25 (Instant/ofEpochSecond (.readLong in) (.readInt in))
    26 (Duration/ofSeconds (.readLong in) (.readInt in))
    27 (Period/of (.readInt in) (.readInt in) (.readInt in))
    28 (LocalDate/ofEpochDay (.readLong in))
    29 (LocalTime/ofNanoOfDay (.readLong in))
    30 (LocalDateTime/of (LocalDate/ofEpochDay (.readLong in)) (LocalTime/ofNanoOfDay (.readLong in)))
    31 (OffsetTime/of (LocalTime/ofNanoOfDay (.readLong in)) (ZoneOffset/ofTotalSeconds (.readInt in)))
    (invalid!)))

(deftype EntityRef [descriptor type-wire key-wire ^long retained ^long nodes ^long depth]
  value/IQuerySourceValue
  (-logical-type [_] descriptor)
  (-retained-bytes [_] retained)
  Object
  (equals [_ other]
    (and (instance? EntityRef other)
         (= type-wire (.-type-wire ^EntityRef other))
         (= key-wire (.-key-wire ^EntityRef other))))
  (hashCode [_] (hash [type-wire key-wire]))
  (toString [_] "#<EntityRef>")
  IHashEq
  (hasheq [this] (.hashCode this))
  Comparable
  (compareTo [_ other]
    (when-not (instance? EntityRef other) (throw (ClassCastException. "Expected EntityRef")))
    (let [c (unsigned-compare type-wire (.-type-wire ^EntityRef other))]
      (if (zero? c) (unsigned-compare key-wire (.-key-wire ^EntityRef other)) c))))

(defn entity-ref?
  "Return whether x is a nominal external entity reference."
  [x] (instance? EntityRef x))

(defn validate-type!
  "Validate the complete registered type and ordered, nonempty key contract."
  [descriptor]
  (preflight! descriptor (budget codec-limits) 0 true)
  (source/validate-type! descriptor)
  (ensure! (and (= :custom (:kind descriptor)) (= type-name (:name descriptor))
                (some? (get-in descriptor [:parameters :identity-domain-id]))
                (vector? (get-in descriptor [:parameters :key-types]))
                (seq (get-in descriptor [:parameters :key-types]))))
  (run! source/validate-type! (get-in descriptor [:parameters :key-types]))
  (metadata-normalize descriptor)
  descriptor)

(defn- key-matches? [type x]
  (and (some? x)
       (if (= :tuple (:kind type))
         (and (vector? x) (= (count x) (count (:elements type)))
              (every? true? (map key-matches? (:elements type) x)))
         (value/matches-type? type x))))

(defn entity-ref
  "Construct an owned reference from a complete type and a non-null key tuple.
  Mutable key leaves are encoded immediately and never retained or exposed."
  ([descriptor key] (entity-ref descriptor key codec-limits))
  ([descriptor key limits]
   (try
     (preflight! descriptor (budget limits) 0 true)
     (preflight! key (budget limits) 0 true)
     (validate-type! descriptor)
     (let [types (get-in descriptor [:parameters :key-types])
           state (budget limits)]
       (ensure! (and (vector? key) (= (count types) (count key))
                     (every? true? (map key-matches? types key))))
       (let [type-wire (immutable-bytes (encode-child (metadata-normalize descriptor) state 1))
             key-wire (immutable-bytes (encode-child (key-normalize key) state 1))
             inspected (budget limits)
             _ (node! inspected 0)
             ;; Decoding canonical metadata drops metadata/backing views, while
             ;; preserving the complete accepted descriptor under map equality.
             descriptor (decode-node (DataInputStream. (ByteArrayInputStream. (byte-array-of type-wire)))
                                     inspected 1)
             ;; Include nested reference envelopes in depth/node admission. The
             ;; immutable wire strings retain these measurements for encoding.
             _ (decode-node (DataInputStream. (ByteArrayInputStream. (byte-array-of key-wire)))
                            inspected 1)
             retained (+' 256 (value/value-charge descriptor)
                          (value/value-charge type-wire) (value/value-charge key-wire))]
         (ensure! (<= (+ 12 (count type-wire) (count key-wire)) (:max-bytes limits)))
         (EntityRef. descriptor type-wire key-wire (long retained)
                     (long @(:nodes inspected)) (long @(:depth inspected)))))
     (catch Exception _ (invalid!)))))

(defn key-tuple
  "Return a detached typed key tuple. Mutating returned leaves cannot change x."
  [^EntityRef x]
  (decode-node (DataInputStream. (ByteArrayInputStream. (byte-array-of (.-key-wire x))))
               (budget codec-limits) 0))

(defn encode
  "Return experimental wire bytes for x, enforcing limits before allocation."
  (^bytes [x] (encode x codec-limits))
  (^bytes [x limits]
   (ensure! (entity-ref? x))
   (limits! limits)
   (let [^EntityRef x x
         n (+ 12 (count (.-type-wire x)) (count (.-key-wire x)))]
     (ensure! (<= n (:max-bytes limits)))
     (ensure! (and (<= (.-nodes x) (:max-nodes limits))
                   (<= (.-depth x) (:max-depth limits))))
     (let [bytes (ByteArrayOutputStream. n)
           out (DataOutputStream. bytes)]
       (doseq [b magic] (.writeByte out b))
       (payload! out (byte-array-of (.-type-wire x)) (budget limits))
       (payload! out (byte-array-of (.-key-wire x)) (budget limits))
       (.toByteArray bytes)))))

(defn- decode-reference [bytes state depth]
  (let [limits (:limits state)]
    (ensure! (and (bytes? bytes) (<= 12 (alength ^bytes bytes) (:max-bytes limits))))
    (let [in (DataInputStream. (ByteArrayInputStream. bytes))]
      (node! state depth)
      (ensure! (= magic (mapv (fn [_] (.readUnsignedByte in)) (range 4))))
      (let [read-node (fn []
                        (let [part (DataInputStream. (ByteArrayInputStream. (read-payload in state)))
                              node (decode-node part state (inc depth))]
                          (ensure! (zero? (.available part)))
                          node))
            descriptor (read-node)
            key (read-node)
            x (entity-ref descriptor key limits)]
        (ensure! (and (zero? (.available in)) (Arrays/equals ^bytes bytes (encode x limits))))
        x))))

(defn decode
  "Read one bounded canonical reference, rejecting trailing or noncanonical bytes."
  ([bytes] (decode bytes codec-limits))
  ([bytes limits]
   (try
     (decode-reference bytes (budget limits) 0)
     (catch Exception _ (invalid!)))))

(defn bind-codec!
  "Bind the registered order/codec UDFs in an application-owned registry.
  Recreate these bindings when reopening a DB. This does not register a type."
  [registry]
  (udf/register! registry (get-in storage-definition [:index :order-fn]) encode)
  (udf/register! registry (get-in storage-definition [:payload :serialize]) encode)
  (udf/register! registry (get-in storage-definition [:payload :deserialize]) decode)
  registry)

(nippy/extend-freeze EntityRef :datalevin.federation/entity-ref-v1
  [x out]
  (let [bytes (encode x)]
    (.writeInt out (alength bytes))
    (.write out bytes)))

(nippy/extend-thaw :datalevin.federation/entity-ref-v1
  [in]
  (let [n (.readInt in)]
    (ensure! (<= 12 n (:max-bytes codec-limits)))
    (let [bytes (byte-array n)]
      (.readFully in bytes)
      (decode bytes))))
