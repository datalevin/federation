(ns ^:no-doc datalevin.federation.file.format
  "Bounded UTF-8 EDN Lines framing. Each physical line is one complete map."
  (:require [clojure.edn :as edn])
  (:import [java.io BufferedInputStream InputStream PushbackReader StringReader]
           [java.nio ByteBuffer]
           [java.nio.charset CharacterCodingException CodingErrorAction StandardCharsets]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def limits {:max-record-bytes 16384 :max-depth 32})

(defn fail! [error phase]
  (throw (ex-info "Export access failed" {:error error :phase phase})))

(defn valid-limits? [{:keys [max-record-bytes max-depth]}]
  (and (pos-int? max-record-bytes) (<= (long max-record-bytes) 1048576)
       (pos-int? max-depth) (<= (long max-depth) 128)))

(defn- depth! [^String text maximum check!]
  (loop [i 0, depth 0, quoted? false, escaped? false]
    (when (< i (.length text))
      (when (zero? (bit-and i 255)) (check!))
      (let [ch (.charAt text i)]
        (cond
          escaped? (recur (inc i) depth quoted? false)
          (= ch \\) (recur (inc i) depth quoted? true)
          (= ch \u0022) (recur (inc i) depth (not quoted?) false)
          quoted? (recur (inc i) depth true false)
          (= ch \;) nil
          (#{\{ \[ \(} ch)
          (do (when (>= depth (long maximum)) (fail! :query-source/limit :values))
              (recur (inc i) (inc depth) false false))
          (#{\} \] \)} ch) (recur (inc i) (dec depth) false false)
          :else (recur (inc i) depth false false))))))

(defn- parse-line [^bytes bytes n maximum check!]
  (try
    (let [decoder (doto (.newDecoder StandardCharsets/UTF_8)
                  (.onMalformedInput CodingErrorAction/REPORT)
                  (.onUnmappableCharacter CodingErrorAction/REPORT))
        text (str (.decode decoder (ByteBuffer/wrap bytes 0 (int n))))]
    (depth! text maximum check!)
    (with-open [reader (PushbackReader. (StringReader. text))]
      (let [options {:eof ::eof :readers {}
                     :default (fn [_ _] (fail! :query-source/invalid :values))}
            row (edn/read options reader)]
        (when-not (and (map? row) (= ::eof (edn/read options reader)))
          (fail! :query-source/invalid :values))
        row)))
    (catch clojure.lang.ExceptionInfo e (throw e))
    (catch CharacterCodingException _ (fail! :query-source/invalid :values))
    (catch RuntimeException _ (fail! :query-source/invalid :values))))

(defn reader
  "Create a charged-by-caller record reader. on-read observes actual bytes read
  from input, including buffering; read! returns nil only at physical EOF."
  [^InputStream input {:keys [max-record-bytes max-depth]} check! on-read]
  (let [observed (proxy [InputStream] []
                   (read
                     ([] (let [n (.read input)] (when-not (neg? n) (on-read 1)) n))
                     ([bytes offset length]
                      (let [n (.read input ^bytes bytes (int offset) (int length))]
                        (when (pos? n) (on-read n)) n)))
                   (close [] (.close input)))
        buffered (BufferedInputStream. observed 8192)
        bytes (byte-array (int max-record-bytes))
        read! (fn []
                (loop [n 0]
                  (when (zero? (bit-and n 255)) (check!))
                  (let [b (.read buffered)]
                    (cond
                      (= b -1) (when (pos? n) (parse-line bytes n max-depth check!))
                      (= b 10) (parse-line bytes n max-depth check!)
                      (= n (long max-record-bytes)) (fail! :query-source/limit :values)
                      :else (do (aset-byte bytes n (unchecked-byte b)) (recur (inc n)))))))]
    {:input buffered :read! read!}))
