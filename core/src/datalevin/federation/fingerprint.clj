(ns datalevin.federation.fingerprint
  "Companion-owned canonical-v1 metadata encoding and SHA-256 fingerprints."
  (:import [java.io ByteArrayOutputStream]
           [java.math BigDecimal BigInteger]
           [java.nio CharBuffer]
           [java.nio.charset CharacterCodingException StandardCharsets]
           [java.security MessageDigest]
           [java.util Date UUID]))

(defn- utf8 ^bytes [^String s]
  (try
    (let [buffer (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap s))
          bytes (byte-array (.remaining buffer))]
      (.get buffer bytes)
      bytes)
    (catch CharacterCodingException _
      (throw (ex-info "Malformed metadata text" {:error :federation/metadata})))))
(defn- frame ^bytes [tag ^bytes payload]
  (let [out (ByteArrayOutputStream.)]
    (.write out (utf8 (str tag (alength payload) ":")))
    (.write out payload)
    (.toByteArray out)))

(defn- concat-bytes ^bytes [parts]
  (let [out (ByteArrayOutputStream.)]
    (doseq [^bytes part parts] (.write out part))
    (.toByteArray out)))

(defn- compare-bytes [^bytes a ^bytes b]
  (loop [i 0]
    (if (= i (min (alength a) (alength b)))
      (compare (alength a) (alength b))
      (let [c (compare (bit-and 255 (aget a i)) (bit-and 255 (aget b i)))]
        (if (zero? c) (recur (inc i)) c)))))

(defn canonical-bytes
  "Encode accepted metadata with typed, byte-length-framed UTF-8 payloads.
  Map keys and set values sort by unsigned encoded bytes; sequences retain order."
  ^bytes [x]
  (cond
    (nil? x) (frame "n" (byte-array 0))
    (boolean? x) (frame "b" (utf8 (if x "1" "0")))
    (string? x) (frame "t" (utf8 x))
    (keyword? x) (frame "k" (concat-bytes [(canonical-bytes (namespace x))
                                         (canonical-bytes (name x))]))
    (symbol? x) (frame "y" (concat-bytes [(canonical-bytes (namespace x))
                                        (canonical-bytes (name x))]))
    (or (instance? Long x) (instance? Integer x)) (frame "l" (utf8 (str x)))
    (or (instance? BigInteger x) (instance? clojure.lang.BigInt x)) (frame "i" (utf8 (str x)))
    (instance? BigDecimal x) (frame "d" (utf8 (str (.scale ^BigDecimal x) ":"
                                                   (.unscaledValue ^BigDecimal x))))
    (instance? Double x)
    (if (Double/isFinite (double x)) (frame "f" (utf8 (Double/toHexString x)))
        (throw (ex-info "Nonfinite metadata number" {:error :federation/metadata})))
    (instance? Float x)
    (if (Float/isFinite (float x)) (frame "r" (utf8 (Float/toHexString x)))
        (throw (ex-info "Nonfinite metadata number" {:error :federation/metadata})))
    (instance? UUID x) (frame "u" (utf8 (str x)))
    (= Date (class x)) (frame "z" (utf8 (str (.getTime ^Date x))))
    (bytes? x) (frame "a" x)
    (and (map? x) (not (record? x)))
    (frame "m" (concat-bytes
                 (mapcat identity
                         (sort (fn [[a] [b]] (compare-bytes a b))
                               (map (fn [[k v]] [(canonical-bytes k) (canonical-bytes v)]) x)))))
    (set? x) (frame "s" (concat-bytes (sort compare-bytes (map canonical-bytes x))))
    (vector? x) (frame "v" (concat-bytes (map canonical-bytes x)))
    (list? x) (frame "q" (concat-bytes (map canonical-bytes x)))
    :else (throw (ex-info "Unsupported metadata value" {:error :federation/metadata}))))

(defn hex
  "Lowercase hexadecimal representation of bytes."
  [bytes]
  (apply str (map #(format "%02x" (bit-and 255 %)) bytes)))

(defn fingerprint
  "Digest a semantic payload. Purpose is schema or definition; neither is a row hash."
  [purpose payload]
  (when-not (#{:schema :definition} purpose)
    (throw (ex-info "Unknown digest purpose" {:error :federation/metadata})))
  (str "federation-" (name purpose) ":v1:sha256:"
       (hex (.digest (MessageDigest/getInstance "SHA-256") (canonical-bytes payload)))))
