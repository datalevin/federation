(ns datalevin.federation.github-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.github :as github]
            [datalevin.federation.install :as install]
            [datalevin.federation.mapping :as mapping]
            [datalevin.query.source :as spi]
            [datalevin.query.source.batch :as batch]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.conformance :as conformance]
            [datalevin.query.source.value :as value]
            [datalevin.util :as u]
            [jsonista.core :as json])
  (:import [com.sun.net.httpserver HttpServer HttpHandler HttpExchange]
           [java.net InetSocketAddress]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent CountDownLatch Executors ExecutorService TimeUnit]))

(def deployment #uuid "136757e4-f993-4de9-9270-d28468167a4d")
(def declaration {:relation-id :github/work-items :source-instance-id deployment
                  :repository {:id 42 :owner "acme" :name "project"}})
(def accepted (github/accept-mapping declaration))
(def compiled (mapping/compile-logical-mapping accepted))
(def issue-type (get-in accepted [:identity :type]))
(def issues
  {1 {"id" 101 "number" 1 "title" "Fix storage" "state" "open" "html_url" "https://github.com/acme/project/issues/1"
      "locked" false "labels" [{"id" 10 "name" "bug"} {"id" 10 "name" "bug"} {"id" 11 "name" "urgent"}]
      "assignees" [{"id" 7 "login" "ada"} {"id" 8 "login" "grace"}]}
   2 {"id" 102 "number" 2 "title" "Update guide" "state" "closed" "html_url" "https://github.com/acme/project/pull/2"
      "locked" true "pull_request" {"url" "https://api.github.com/repos/acme/project/pulls/2"}
      "labels" [] "assignees" []}})

(defn- error [f]
  (try (f) nil (catch clojure.lang.ExceptionInfo e (ex-data e))))
(defn- category [f] (:error (error f)))
(defn- query [find in where]
  {:find (if (= (last find) (quote ...)) [find] find) :in in :where where :query-source {:consistency :best-effort}})
(def key-query (query '[?title ...] '[$g ?key]
                     '[[$g ?e :github.issue/key ?key] [$g ?e :github.issue/title ?title]]))

(defn- with-api [f]
  (let [requests (atom []) behavior (atom nil)
        server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
        executor (Executors/newCachedThreadPool)
        handler (reify HttpHandler
                  (handle [_ exchange]
                    (let [^HttpExchange exchange exchange path (.getPath (.getRequestURI exchange))
                          observation {:path path :method (.getRequestMethod exchange)
                                       :version (.getFirst (.getRequestHeaders exchange) "X-GitHub-Api-Version")}
                          _ (swap! requests conj observation)
                          default (if (= path "/repos/acme/project") {"id" 42}
                                    (when-let [[_ number] (re-matches #"/repos/acme/project/issues/(\d+)" path)]
                                      (issues (Long/parseLong number))))
                          response (if-let [respond @behavior] (respond exchange default) {:status 200 :json default})]
                      (try
                        (when response
                          (doseq [[k v] (:headers response)] (.set (.getResponseHeaders exchange) k v))
                          (let [body (.getBytes ^String (or (:raw response) (json/write-value-as-string (:json response))) StandardCharsets/UTF_8)]
                            (.sendResponseHeaders exchange (long (get response :status 200)) (alength body))
                            (when-let [before (:before-body response)] (before))
                            (.write (.getResponseBody exchange) body)))
                        (catch java.io.IOException _ nil)
                        (finally (.close exchange))))))]
    (.setExecutor server executor)
    (.createContext server "/" handler)
    (.start server)
    (try
      (f {:base (str "http://127.0.0.1:" (.getPort (.getAddress server))) :requests requests :behavior behavior})
      (finally (.stop server 0) (.shutdownNow ^ExecutorService executor)))))

(defn- source [api & [options]]
  (github/source accepted (merge {:api-base (:base api) :compute-context-id :github/test} options)))
(defn- input-facts []
  (let [project (mapping/fact-projector compiled)]
    (vec (mapcat (fn [[number item]]
                   (project [42 number]
                            {:github.issue/number number :github.issue/title (item "title") :github.issue/state (item "state")
                             :github.issue/label (mapv #(get % "name") (item "labels"))
                             :github.issue/assignee (mapv #(get % "id") (item "assignees"))
                             :github.issue/assignee-login (mapv #(get % "login") (item "assignees"))
                             :github.issue/locked? (item "locked")
                             :github.issue/pull-request? (boolean (item "pull_request"))})) issues))))

(deftest pure-explicit-contracts-and-runtime-secrets
  (with-api
    (fn [api]
      (let [provider (source api {:token "fixture-secret"}) info (spi/-source-info provider)]
        (is (= info (spi/-source-info provider)))
        (is (empty? @(:requests api)))
        (is (= #{:best-effort} (:read-capabilities info)))
        (is (not (.contains (pr-str info) "fixture-secret")))
        (is (= :many (get-in info [:logical-schema :attributes :github.issue/assignee :cardinality])))
        (is (= [deployment :github/user 0]
               (get-in info [:logical-schema :attributes :github.issue/assignee :reference-domain])))
        (doseq [bad [(assoc declaration :token "secret")
                     (assoc-in declaration [:repository :owner] "../evil")
                     (assoc declaration :attributes #{:unknown/value})]]
          (is (= :query-source/invalid (category #(github/accept-mapping bad)))))
        (is (= :query-source/invalid
               (category #(github/source (assoc-in accepted [:dependencies :api-version] "new")
                                          {:compute-context-id :test/a}))))
        (is (= :query-source/invalid
               (category #(github/source accepted {:compute-context-id :test/a :api-base "http://example.com"}))))))))

(deftest keyed-queries-match-eager-facts-and-preserve-multiplicity
  (with-api
    (fn [api]
      (let [provider (source api) eager (input-facts)
            queries [(query '[?title ?label ?login] '[$g ?key]
                            '[[$g ?e :github.issue/key ?key] [$g ?e :github.issue/title ?title]
                              [$g ?e :github.issue/label ?label] [$g ?e :github.issue/assignee-login ?login]])
                     (query '[?assignee ...] '[$g ?key]
                            '[[$g ?e :github.issue/key ?key] [$g ?e :github.issue/assignee ?assignee]])
                     (query '[?pr ?locked] '[$g ?key]
                            '[[$g ?e :github.issue/key ?key] [$g ?e :github.issue/pull-request? ?pr]
                              [$g ?e :github.issue/locked? ?locked]])
                     (assoc (query '[(count ?e) .] '[$g [[?number ?witness]]]
                                   '[[$g ?e :github.issue/number ?number]]) :with '[?witness])]]
        (doseq [q queries input (if (= 2 (count (:in q)))
                                 (if (= '?key (second (:in q))) [[42 1] [42 2]]
                                     [[[1 :a] [1 :b] [2 :c]]]) [])]
          (let [expected (d/q (dissoc q :query-source) eager input)
                actual (try (d/q (assoc-in q [:query-source :limits] {:batch-rows 1 :binding-rows 1}) provider input)
                            (catch Exception e (throw (ex-info "Lookup case" {:query q :input input} e))))]
            (is (= (if (coll? expected) (frequencies expected) expected)
                   (if (coll? actual) (frequencies actual) actual)) (pr-str q))))
        (is (every? #(and (= "GET" (:method %)) (= github/api-version (:version %))) @(:requests api)))
        (is (every? #(or (= "/repos/acme/project" (:path %))
                        (re-matches #"/repos/acme/project/issues/[12]" (:path %))) @(:requests api)))
        (is (zero? (:live-sessions (github/statistics provider))))))))

(deftest native-triage-join-is-selective-and-explains-actual-work
  (with-api
    (fn [api]
      (let [conn (d/create-conn nil {:triage/number {:db/valueType :db.type/long}
                                     :triage/repository {:db/valueType :db.type/long}
                                     :triage/priority {:db/valueType :db.type/long}}
                                {:kv-opts {:inmemory? true} :background-sampling? false})
            provider (source api)
            q (query '[?priority ?title ?label] '[$ $g]
                     '[[?t :triage/repository 42] [?t :triage/number ?number] [?t :triage/priority ?priority] [(>= ?priority 9)]
                       [$g ?e :github.issue/number ?number] [$g ?e :github.issue/title ?title]
                       [$g ?e :github.issue/label ?label]])]
        (try
          (d/transact! conn [{:triage/repository 42 :triage/number 1 :triage/priority 10}
                             {:triage/repository 42 :triage/number 2 :triage/priority 1}])
          (is (= #{[10 "Fix storage" "bug"] [10 "Fix storage" "urgent"]}
                 (set (d/q q @conn provider))))
          (is (not-any? #(= "/repos/acme/project/issues/2" (:path %)) @(:requests api)))
          (let [before (count @(:requests api)) explain (d/explain {:run? false} q @conn provider)]
            ;; Acquisition verifies access; planning itself performs no item read.
            (is (= 1 (- (count @(:requests api)) before)))
            (is (every? #(= :best-effort (:consistency %)) (:sources explain))))
          (let [explain (d/explain {:run? true} q @conn provider)
                metrics (map :metrics (:cursors explain))]
            (is (pos? (reduce + (map :external-requests metrics))))
            (is (pos? (reduce + (map :read-bytes metrics))))
            (is (every? #(nil? (:data-version %)) (:sources explain))))
          (finally (d/close conn)))))))

(defn- request [provider ctl]
  {:control ctl :query-id (random-uuid) :spi-version 2 :features #{} :consistency :best-effort
   :limits (control/limits ctl) :authorization nil :source-descriptors [(spi/-source-info provider)]})
(defn- plan [provider session request terms available]
  (spi/-plan-source provider session
    (merge request {:source-symbol '$g :region-id 0 :scope-id (get-in (spi/-source-info provider) [:scope :id])
                    :source-descriptor (spi/-source-info provider) :bindings {} :available-inputs available
                    :clauses [{:id 0 :kind :pattern :terms terms}]})))
(defn- open [provider session request alternative bindings]
  (spi/-open-cursor session
    (cond-> (merge (select-keys request [:control :query-id :spi-version :features :authorization :consistency :limits])
                   {:source-symbol '$g :region-id 0 :scope-id (get-in (spi/-source-info provider) [:scope :id])
                    :definition-fingerprint (:definition-fingerprint (spi/-source-info provider))
                    :alternative-id (:id alternative) :plan (:plan alternative)})
      bindings (assoc :bindings bindings))))
(defn- released? [ctl] (every? zero? (vals (dissoc (control/usage ctl) :external-requests))))

(deftest dependent-ids-batches-replay-and-cleanup
  (with-api
    (fn [api]
      (let [provider (source api) ctl (control/create) common (request provider ctl)
            session (spi/-open-session provider common)
            required [{:variable '?e :type issue-type :nullable? false}]
            terms '[[:var ?e] [:const :github.issue/label] [:var ?label]]
            inputs [{:binding-id 91 :values [(ref/entity-ref issue-type [42 1])]}
                    {:binding-id 92 :values [(ref/entity-ref issue-type [42 2])]}
                    {:binding-id 93 :values [(ref/entity-ref issue-type [99 2])]}]]
        (try
          (is (empty? (:alternatives (plan provider session common terms []))))
          (let [before (count @(:requests api))
                alternative (first (:alternatives (plan provider session common terms required)))
                _ (is (= before (count @(:requests api))))
                _ (is (= :unstable (get-in alternative [:properties :replay])))
                read! (fn [max-rows max-bytes]
                        (let [cursor (open provider session common alternative inputs)]
                          (try
                            (loop [all [] ids []]
                              (let [fetch {:control ctl :max-rows max-rows :max-bytes max-bytes}
                                    raw (spi/-next-batch! cursor fetch)
                                    accepted (batch/accept! {:control ctl} (:columns alternative) true fetch raw identity)]
                                (is (= (:byte-count accepted)
                                       (spi/batch-charge-bytes (:columns accepted) (:rows accepted) (:binding-ids accepted))))
                                (when-let [r (:reservation accepted)] (spi/release! r))
                                (if (:exhausted? accepted)
                                  [(into all (:rows accepted)) (into ids (:binding-ids accepted)) (spi/-cursor-metrics cursor)]
                                  (recur (into all (:rows accepted)) (into ids (:binding-ids accepted))))))
                            (finally (spi/-close-cursor! cursor) (spi/-close-cursor! cursor)))))
                [rows ids metrics] (read! 1 131072)
                minimum (apply max (map #(spi/batch-charge-bytes (:columns alternative) [%1] [%2]) rows ids))]
            (is (= [91 91] ids))
            (is (= 2 (:external-requests metrics) (:read-rows metrics)))
            (is (= rows (first (read! 100 minimum))))
            (is (= rows (first (read! 100 131072))))
            (is (= :query-source/limit (category #(read! 1 1)))))
          (finally (spi/-close-session! session) (spi/-close-session! session)))
        (is (released? ctl))
        (is (= 8 (:external-requests (control/usage ctl))))
        (control/close! ctl)))))

(deftest ambiguous-errors-and-resource-limits-do-not-become-empty-results
  (with-api
    (fn [api]
      (doseq [[response expected]
              [[{:status 401 :json {"message" "secret provider details"}} :query-source/unauthorized]
               [{:status 403 :json {}} :query-source/unauthorized]
               [{:status 404 :json {}} :query-source/incomplete]
               [{:status 429 :headers {"Retry-After" "3"} :json {}} :query-source/limit]
               [{:status 503 :json {}} :query-source/provider]
               [{:status 302 :headers {"Location" "http://example.com/leak"} :json {}} :query-source/provider]
               [{:status 200 :raw "{broken"} :query-source/provider]
               [{:status 200 :raw "{\"id\":101,\"id\":102,\"number\":1}"} :query-source/invalid]
               [{:status 200 :json (assoc (issues 1) "number" 2)} :query-source/invalid]
               [{:status 200 :json (assoc (issues 1) "title" nil)} :query-source/invalid]]]
        (testing (str (:status response) " " expected)
          (reset! (:behavior api) (fn [^HttpExchange exchange default]
                                    (if (= "/repos/acme/project" (.getPath (.getRequestURI exchange)))
                                      {:status 200 :json default} response)))
          (let [before (count @(:requests api)) provider (source api {:token "fixture-secret"})
                data (error #(d/q key-query provider [42 1]))]
            (is (= expected (:error data)))
            (is (not (.contains (pr-str data) "fixture-secret")))
            (is (= (if (and (= 200 (:status response)) (contains? (:json response) "title")
                                    (nil? (get-in response [:json "title"]))) 3 2)
                   (- (count @(:requests api)) before)))
            (is (zero? (:live-sessions (github/statistics provider)))))))
      (reset! (:behavior api) nil)
      (doseq [options [{:max-response-bytes 32} {:max-json-nodes 4}]]
        (let [provider (source api options)]
          (is (= :query-source/limit (category #(d/q key-query provider [42 1]))))
          (is (zero? (:live-sessions (github/statistics provider))))))
      (let [provider (source api)
            q (assoc-in key-query [:query-source :limits :external-requests] 1)]
        (is (= :query-source/limit (category #(d/q q provider [42 1])))))
      (let [provider (source api)
            q (assoc-in key-query [:query-source :limits :retained-bytes] 4096)]
        (is (= :query-source/limit (category #(d/q q provider [42 1]))))))))

(deftest explicit-session-cancellation-and-total-body-timeout
  (doseq [kind [:cancel :request-timeout :query-timeout]]
    (with-api
      (fn [api]
        (let [reading (CountDownLatch. 1) unblock (CountDownLatch. 1)
              provider (source api {:request-timeout-ms (if (= kind :request-timeout) 150 5000)})
              ctl (control/create (if (= kind :query-timeout) {:timeout-ms 300} {}))
              common (request provider ctl) session (spi/-open-session provider common)]
          (try
            (reset! (:behavior api)
                    (fn [_ default] {:status 200 :json default
                                     :before-body #(do (.countDown reading) (.await unblock 5 TimeUnit/SECONDS))}))
            (let [alternative (first (:alternatives
                                      (plan provider session common
                                            [[:const (ref/entity-ref issue-type [42 1])]
                                             [:const :github.issue/title] [:var '?title]] [])))
                  cursor (open provider session common alternative nil)
                  started (System/nanoTime)
                  task (future (category #(spi/-next-batch! cursor {:control ctl :max-rows 1 :max-bytes 131072})))]
              (try
                (is (.await reading 2 TimeUnit/SECONDS))
                (when (= kind :cancel) (spi/-cancel-session! session {:error :query-source/cancelled}))
                (is (= (case kind :cancel :query-source/cancelled :query-timeout :query/timeout :query-source/provider)
                       (deref task 2000 ::stuck)))
                (is (< (/ (- (System/nanoTime) started) 1000000.0) 2500.0))
                (finally (.countDown unblock) (future-cancel task) (spi/-close-cursor! cursor))))
            (finally (.countDown unblock) (spi/-close-session! session)))
          (is (released? ctl))
          (control/close! ctl))))))

(deftest repository-identity-and-consistency-fail-before-item-reads
  (with-api
    (fn [api]
      (let [provider (source api) ctl (control/create) common (request provider ctl)]
        (try
          (is (= :query-source/invalid
                 (category #(spi/-open-session provider (assoc common :consistency :snapshot)))))
          (is (empty? @(:requests api)))
          (reset! (:behavior api) (fn [_ _] {:status 200 :json {"id" 99}}))
          (is (= :query-source/stale-definition (category #(spi/-open-session provider common))))
          (is (released? ctl))
          (reset! (:behavior api) (fn [_ _] {:status 404 :json {}}))
          (is (= :query-source/unauthorized (category #(spi/-open-session provider common))))
          (is (released? ctl))
          (finally (control/close! ctl)))))))

(deftest nominal-identity-survives-persisted-reopen-and-fences-repositories
  (with-api
    (fn [api]
      (let [path (u/tmp-dir (str "github-definitions-" (random-uuid)))
            owner :datalevin.federation/definition
            definitions {:github {:datalevin/definition-kind :source
                                   owner {:format-version 1 :source-instance-id deployment :provider :github/issues
                                          :profile :profiles/github}}
                         :github/work-items {:datalevin/definition-kind :relation
                                             owner {:format-version 1 :source :github :accepted accepted}}}
            conn (d/create-conn path {} {:wal? false :background-sampling? false})]
        (try
          (install/install! conn definitions {:expected-versions {:github nil :github/work-items nil}})
          (d/close conn)
          (let [reopened (d/create-conn path) saved (install/load-definitions reopened)
                persisted (get-in saved [:schema :github/work-items owner :accepted])
                provider (github/source persisted {:api-base (:base api) :compute-context-id :github/reopen})
                e (ref/entity-ref issue-type [42 1])
                q (query '[?title ...] '[$g ?e] '[[$g ?e :github.issue/title ?title]])]
            (try
              (is (= ["Fix storage"] (d/q q provider e)))
              (is (= (spi/-source-info (source api))
                     (assoc (spi/-source-info provider) :compute-context-id :github/test
                            :definition-fingerprint (:definition-fingerprint (spi/-source-info (source api))))))
              (let [before (count @(:requests api))]
                (is (empty? (d/q q provider (ref/entity-ref issue-type [99 1]))))
                (is (= 1 (- (count @(:requests api)) before))))
              (is (= issue-type (value/-logical-type e)))
              (finally (d/close reopened))))
          (finally (d/close conn) (u/delete-files path)))))))

(deftest shared-mapped-entity-lookup-gate
  (with-api
    (fn [api]
      (conformance/mapped-lookup-semantics! (source api) (input-facts)
        {:identity :github.issue/key :value :github.issue/title
         :many :github.issue/label :reference :github.issue/assignee}))))

(deftest full-row-window-does-not-fetch-the-next-key
  (with-api
    (fn [api]
      (let [provider (source api) ctl (control/create) common (request provider ctl)
            session (spi/-open-session provider common)]
        (try
          (let [alternative (first (:alternatives
                                    (plan provider session common
                                          '[[:var ?e] [:const :github.issue/title] [:var ?title]]
                                          [{:variable '?e :type issue-type :nullable? false}])))
                inputs (mapv (fn [n] {:binding-id n :values [(ref/entity-ref issue-type [42 n])]}) [1 2])
                cursor (open provider session common alternative inputs)]
            (try
              (let [batch (spi/-next-batch! cursor {:control ctl :max-rows 1 :max-bytes 131072})]
                (try
                  (is (= 1 (:row-count batch)))
                  (is (= [1] (:binding-ids batch)))
                  (is (false? (:exhausted? batch)))
                  (is (= 2 (count @(:requests api))))
                  (is (= 1 (:external-requests (spi/-cursor-metrics cursor))))
                  (finally (spi/release! (:reservation batch)))))
              (finally (spi/-close-cursor! cursor))))
          (finally (spi/-close-session! session)))
        (is (released? ctl))
        (control/close! ctl)))))

(deftest live-replay-rate-errors-and-acquisition-failures
  (with-api
    (fn [api]
      (let [provider (source api) ctl (control/create) common (request provider ctl)
            session (spi/-open-session provider common)]
        (try
          (let [alternative (first (:alternatives
                                    (plan provider session common
                                          [[:const (ref/entity-ref issue-type [42 1])]
                                           [:const :github.issue/title] [:var '?title]] [])))
                fetch! (fn []
                         (let [cursor (open provider session common alternative nil)]
                           (try
                             (let [batch (spi/-next-batch! cursor {:control ctl :max-rows 10 :max-bytes 131072})]
                               (try (:rows batch) (finally (spi/release! (:reservation batch)))))
                             (finally (spi/-close-cursor! cursor)))))]
            (is (= [["Fix storage"]] (fetch!)))
            (reset! (:behavior api) (fn [_ default] {:status 200 :json (assoc default "title" "Changed live")}))
            (is (= [["Changed live"]] (fetch!)))
            (is (= :best-effort (:consistency (spi/-session-info session))))
            (is (nil? (:data-version (spi/-session-info session))))
            (doseq [response [{:status 429 :headers {"Retry-After" "3"} :json {}}
                              {:status 403 :headers {"Retry-After" "3"} :json {}}
                              {:status 403 :headers {"X-RateLimit-Remaining" "0"} :json {}}]]
              (reset! (:behavior api) (fn [_ _] response))
              (let [before (count @(:requests api)) data (error fetch!)]
                (is (= :query-source/limit (:error data)))
                (is (= (:status response) (:status data)))
                (when (get-in response [:headers "Retry-After"])
                  (is (= 3000 (:retry-after-ms data))))
                (is (= 1 (- (count @(:requests api)) before)))
                (is (empty? (:cursors @(:state session)))))))
          (finally (spi/-close-session! session)))
        (is (released? ctl))
        (reset! (:behavior api) (fn [_ _] {:status 200 :raw "{truncated"}))
        (is (= :query-source/provider (category #(spi/-open-session provider common))))
        (is (released? ctl))
        (is (zero? (:live-sessions (github/statistics provider))))
        (control/close! ctl)))))
