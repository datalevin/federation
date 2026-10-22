(ns datalevin.federation.github.http
  "Bounded read-only GitHub transport. Response bodies, parser nodes and nesting
  are admitted before use; cancellation and the request timeout also cover bodies."
  (:require [datalevin.query.source :as spi])
  (:import [com.fasterxml.jackson.core JsonFactory JsonParser JsonToken StreamReadConstraints]
           [java.io ByteArrayOutputStream Closeable InputStream]
           [java.net URI]
           [java.net.http HttpClient HttpClient$Redirect HttpRequest HttpResponse HttpResponse$BodyHandlers]
           [java.time Duration]
           [java.util.concurrent CompletableFuture ScheduledThreadPoolExecutor ThreadFactory TimeUnit]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn fail!
  ([error phase] (fail! error phase {}))
  ([error phase details]
   (throw (ex-info "GitHub query source failed" (merge {:error error :phase phase} details)))))

(defn ensure! [condition phase]
  (when-not condition (fail! :query-source/invalid phase)))

(defonce ^:private client
  (delay (-> (HttpClient/newBuilder) (.followRedirects HttpClient$Redirect/NEVER)
             (.connectTimeout (Duration/ofSeconds 10)) .build)))
(defonce ^:private scheduler
  (delay (doto (ScheduledThreadPoolExecutor.
                 1 (reify ThreadFactory
                     (newThread [_ task]
                       (doto (Thread. task "federation-github-timeout") (.setDaemon true)))))
           (.setRemoveOnCancelPolicy true))))

(defn scratch-bytes
  "Conservative live workspace including bounded JSON nodes and projected refs."
  ^long [profile]
  (+ 65536 (* 8 (long (:max-response-bytes profile))) (* 4096 (long (:max-json-nodes profile)))))

(defn profile! [profile]
  (let [profile (merge {:api-base "https://api.github.com" :request-timeout-ms 10000
                        :max-response-bytes 262144 :max-json-nodes 4096} profile)
        uri (try (URI. ^String (:api-base profile)) (catch Exception _ nil))]
    (ensure! (and uri (some? (:compute-context-id profile))
                  (every? #{:api-base :compute-context-id :token :request-timeout-ms
                            :max-response-bytes :max-json-nodes} (keys profile))
                  (or (nil? (:token profile)) (and (string? (:token profile))
                                                  (re-matches #"[A-Za-z0-9_\-.]+" (:token profile))))
                  (every? #(and (pos-int? %) (<= (long %) 16777216))
                          ((juxt :max-response-bytes :max-json-nodes :request-timeout-ms) profile))
                  (.getHost ^URI uri) (nil? (.getUserInfo ^URI uri)) (nil? (.getQuery ^URI uri))
                  (nil? (.getFragment ^URI uri)) (not (.endsWith ^String (:api-base profile) "/"))
                  (or (= "https" (.getScheme ^URI uri))
                      (and (= "http" (.getScheme ^URI uri))
                           (#{"127.0.0.1" "localhost" "[::1]"} (.getHost ^URI uri))))) :descriptor)
    profile))

(defn- parse [^bytes body profile check!]
  (let [factory (-> (JsonFactory/builder)
                    (.streamReadConstraints (-> (StreamReadConstraints/builder)
                                                (.maxNestingDepth 32)
                                                (.maxStringLength (int (:max-response-bytes profile)))
                                                (.maxNumberLength 128) .build)) .build)
        nodes (volatile! 0)]
    (with-open [parser (.createParser ^JsonFactory factory body)]
      (letfn [(read! [depth]
                (check!)
                (vreset! nodes (inc (long @nodes)))
                (when (or (> (long @nodes) (long (:max-json-nodes profile))) (> (long depth) 32))
                  (fail! :query-source/limit :fetch))
                (let [token (.currentToken ^JsonParser parser)]
                  (condp identical? token
                    JsonToken/START_OBJECT
                    (loop [out {}]
                      (let [token (.nextToken ^JsonParser parser)]
                        (if (identical? token JsonToken/END_OBJECT) out
                          (do (ensure! (identical? token JsonToken/FIELD_NAME) :values)
                              (let [key (.currentName ^JsonParser parser)]
                                (ensure! (not (contains? out key)) :values)
                                (.nextToken ^JsonParser parser)
                                (recur (assoc out key (read! (inc (long depth))))))))))
                    JsonToken/START_ARRAY
                    (loop [out []]
                      (if (identical? (.nextToken ^JsonParser parser) JsonToken/END_ARRAY) out
                          (recur (conj out (read! (inc (long depth)))))))
                    JsonToken/VALUE_STRING (.getText ^JsonParser parser)
                    JsonToken/VALUE_NUMBER_INT (.getLongValue ^JsonParser parser)
                    JsonToken/VALUE_NUMBER_FLOAT (.getDecimalValue ^JsonParser parser)
                    JsonToken/VALUE_TRUE true
                    JsonToken/VALUE_FALSE false
                    JsonToken/VALUE_NULL nil
                    (fail! :query-source/invalid :values))))]
        (.nextToken ^JsonParser parser)
        (let [result (read! 0)]
          (ensure! (nil? (.nextToken ^JsonParser parser)) :values)
          result)))))

(defn get-json!
  "One actual GET, with one ledger permit. No hidden adapter retry or redirect.
  Callers reserve scratch-bytes for the lifetime of the parsed/projected result.
  blocked holds a prompt close action, also used for explicit session cancellation."
  [profile path api-version control limiter blocked check! metrics phase]
  (check!)
  (with-open [_ ^Closeable (spi/acquire-request! limiter)]
    (let [timeout (max 1 (min (long (:request-timeout-ms profile))
                              (long (or (spi/remaining-time-ms control) Long/MAX_VALUE))))
          builder (-> (HttpRequest/newBuilder (URI. (str (:api-base profile) path)))
                      (.timeout (Duration/ofMillis timeout))
                      (.header "Accept" "application/vnd.github+json")
                      (.header "User-Agent" "datalevin-federation-github/0.1")
                      (.header "X-GitHub-Api-Version" api-version) .GET)
          _ (when-let [token (:token profile)] (.header builder "Authorization" (str "Bearer " token)))
          resources (atom {})
          timed-out? (atom false)
          close! (fn []
                   (let [[before _] (swap-vals! resources assoc :closed? true)]
                     (when-let [^CompletableFuture future (:future before)] (.cancel future true))
                     (when-let [^InputStream input (:input before)]
                       (try (.close input) (catch Exception _ nil)))))
          registration (spi/register-cancel! control (fn [_] (close!)))
          timer (.schedule ^ScheduledThreadPoolExecutor @scheduler
                           ^Runnable #(do (reset! timed-out? true) (close!)) timeout TimeUnit/MILLISECONDS)]
      (reset! blocked close!)
      (try
        (check!)
        (swap! metrics update :external-requests inc)
        (let [future (.sendAsync ^HttpClient @client (.build builder) (HttpResponse$BodyHandlers/ofInputStream))
              previous (swap! resources assoc :future future)]
          (when (:closed? previous) (.cancel future true))
          (let [^HttpResponse response (.get ^CompletableFuture future timeout TimeUnit/MILLISECONDS)
                ^InputStream input (.body response)
                previous (swap! resources assoc :input input)]
            (when (:closed? previous) (.close input))
            (check!)
            (when @timed-out? (fail! :query-source/provider phase {:reason :request-timeout}))
            (let [status (.statusCode response)]
              (when-not (= 200 status)
                (let [headers (.headers response)
                      retry-after (try (some-> (.firstValue headers "retry-after") (.orElse nil) Long/parseLong)
                                       (catch Exception _ nil))]
                  (fail! (cond (and (= 403 status)
                                    (or retry-after (= "0" (.orElse (.firstValue headers "x-ratelimit-remaining") ""))))
                               :query-source/limit
                               (#{401 403} status) :query-source/unauthorized
                               (= 404 status) (if (= :acquisition phase) :query-source/unauthorized :query-source/incomplete)
                               (= 429 status) :query-source/limit
                               :else :query-source/provider)
                         phase (cond-> {:status status}
                                 (and retry-after (<= 0 (long retry-after) 86400))
                                 (assoc :retry-after-ms (* 1000 (long retry-after)))))))
              (ensure! (or (not (.isPresent (.firstValue (.headers response) "content-encoding")))
                           (= "identity" (.orElse (.firstValue (.headers response) "content-encoding") ""))) :values)
              (let [buffer (byte-array 8192) out (ByteArrayOutputStream.)
                    body (loop [total 0]
                           (check!)
                           (when @timed-out? (fail! :query-source/provider phase {:reason :request-timeout}))
                           (let [n (.read input buffer)]
                             (if (neg? n) (.toByteArray out)
                               (let [total (+ total n)]
                                 (swap! metrics update :read-bytes + n)
                                 (when (> total (long (:max-response-bytes profile))) (fail! :query-source/limit :fetch))
                                 (.write out buffer 0 n)
                                 (recur total)))))
                    parsed (parse body profile #(do (check!)
                                                  (when @timed-out?
                                                    (fail! :query-source/provider phase {:reason :request-timeout}))))]
                (check!)
                (when @timed-out? (fail! :query-source/provider phase {:reason :request-timeout}))
                parsed))))
        (catch Exception e
          (check!)
          (cond @timed-out? (fail! :query-source/provider phase {:reason :request-timeout})
                (instance? clojure.lang.ExceptionInfo e) (throw e)
                :else (fail! :query-source/provider phase)))
        (finally
          (reset! blocked nil)
          (.cancel timer false)
          (spi/unregister-cancel! registration)
          (close!))))))
