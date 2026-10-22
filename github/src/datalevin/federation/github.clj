(ns datalevin.federation.github
  "Live repository work-item lookups (issues and pull requests), with durable
  nominal keys and best-effort reads. No enumeration or search-cap completeness."
  (:require [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.github.http :as http :refer [ensure! fail!]]
            [datalevin.federation.mapping :as mapping]
            [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [java.io Closeable]
           [java.util UUID]
           [datalevin.utl QueryKey]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def api-version "2026-03-10")
(def ^:private fields
  {:github.issue/id ["id" :long :one]
   :github.issue/number ["number" :long :one]
   :github.issue/title ["title" :string :one]
   :github.issue/state ["state" :string :one]
   :github.issue/url ["html_url" :string :one]
   :github.issue/locked? ["locked" :boolean :one]
   :github.issue/pull-request? ["pull_request" :boolean :one]
   :github.issue/label ["labels" :string :many]
   :github.issue/assignee-login ["assignees" :string :many]
   :github.issue/assignee ["assignees" :reference :many]})

(defn accept-mapping
  "Accept a repository's explicit numeric ID and owner/name locator, without I/O.
  Required: :relation-id, :source-instance-id, :repository {:id :owner :name}.
  Optional :attributes selects known fields; by default every field is mapped.
  Durable key [repository-id issue-number] also names PRs in the issues API.
  The source UUID identifies a GitHub deployment, not a credential or repository."
  [declaration]
  (let [{:keys [relation-id source-instance-id repository]} declaration
        attributes (get declaration :attributes (set (keys fields)))
        domain [source-instance-id :github/issue 0]
        user-domain [source-instance-id :github/user 0]
        user-type (mapping/entity-type ref/type-contract user-domain [:long])]
    (ensure! (and (every? #{:relation-id :source-instance-id :repository :attributes} (keys declaration))
                  (keyword? relation-id) (uuid? source-instance-id)
                  (= #{:id :owner :name} (set (keys repository)))
                  (pos-int? (:id repository))
                  (every? #(and (string? %) (re-matches #"[A-Za-z0-9_.-]+" %) (not (#{"." ".."} %)))
                          ((juxt :owner :name) repository))
                  (set? attributes) (every? fields attributes)) :descriptor)
    (let [accepted
          {:representation-version 2 :relation-id relation-id :source-instance-id source-instance-id
           :identity {:type (mapping/entity-type ref/type-contract domain [:long :long])
                      :attribute :github.issue/key :scope :durable
                      :extraction {:shape :key-tuple :expressions [[:repository-id] [:field "number"]]}}
           :attributes
           (into {} (map (fn [attribute]
                           (let [[field type cardinality] (fields attribute)]
                             [attribute (merge {:extraction {:shape (if (= :many cardinality) :elements :value)
                                                            :expression [:github/field field attribute]}
                                                :cardinality cardinality :null :absent :conversion {:version 1}}
                                               (if (= :reference type)
                                                 {:reference {:domain user-domain :type user-type}} {:type type}))])) attributes))
           :scope {:id (str "github:repository:" (:id repository) ":all-work-items")
                   :kind :restricted :parameters {:repository-id (:id repository) :includes-pull-requests? true}}
           :completion {:domain :finite :extent :all-matches}
           :requirements {:api-version api-version :fields (into (sorted-map) (select-keys fields attributes))}
           :dependencies {:provider {:name :github/issues :version 1} :conversion-version 1
                          :repository repository :api-version api-version :access :key-lookup}}]
      (mapping/compile-logical-mapping accepted)
      accepted)))

(defn- compiled! [accepted]
  (let [compiled (mapping/compile-logical-mapping accepted)
        canonical (accept-mapping {:relation-id (:relation-id accepted)
                                   :source-instance-id (:source-instance-id accepted)
                                   :repository (get-in accepted [:dependencies :repository])
                                   :attributes (set (keys (:attributes accepted)))})]
    ;; The installer adds only its dependency manifest. Used endpoint/conversion
    ;; contracts cannot silently drift when a saved definition is reopened.
    (ensure! (= canonical (update accepted :dependencies dissoc :definition-contracts)) :descriptor)
    compiled))

(defn- active! [state ctl]
  (spi/check-active! ctl)
  (when (:cancelled? @state) (fail! :query-source/cancelled :execution))
  (when (:closed? @state) (fail! :query-source/invalid :execution)))

(defn- extraction [attribute document]
  (let [[field _ cardinality] (fields attribute)
        x (get document field)]
    (case attribute
      :github.issue/pull-request? (do (ensure! (or (nil? x) (map? x)) :values) (boolean x))
      (:github.issue/label :github.issue/assignee-login :github.issue/assignee)
      (do (ensure! (vector? x) :values)
          (mapv (fn [item]
                  (ensure! (map? item) :values)
                  (case attribute
                    :github.issue/label (let [name (get item "name")] (ensure! (string? name) :values) name)
                    :github.issue/assignee-login (let [login (get item "login")] (ensure! (string? login) :values) login)
                    :github.issue/assignee (let [id (get item "id")] (ensure! (pos-int? id) :values) id))) x))
      (do (ensure! (and (= :one cardinality) (contains? document field) (some? x)) :values) x))))

(defn- project [compiled projector key document attribute]
  (ensure! (and (map? document) (= (second key) (get document "number"))
                (pos-int? (get document "id"))) :values)
  (try
    (projector key (if (contains? (get-in compiled [:accepted :attributes]) attribute)
                     {attribute (extraction attribute document)} {})
               (if (= attribute :github.issue/key) #{} #{attribute}))
    (catch clojure.lang.ExceptionInfo e
      (if (#{:federation/mapping :federation/entity-ref} (:error (ex-data e)))
        (fail! :query-source/invalid :values) (throw e)))))

(defn- pattern-columns [descriptor clause]
  (let [types (get-in descriptor [:pattern-contract :attributes (second (second (:terms clause)))])]
    (when-not types (fail! :query-source/unsupported :clauses))
    [{:type (:entity-type types) :nullable? false} {:type :keyword :nullable? false}
     {:type (:value-type types) :nullable? false}]))

(defn- columns [descriptor clause]
  (reduce (fn [out [term column]]
            (if (and (= :var (first term)) (not-any? #(= (second term) (:variable %)) out))
              (conj out (assoc column :variable (second term))) out))
          [] (map vector (:terms clause) (pattern-columns descriptor clause))))

(def ^:private constraints {:max-binding-rows 256 :max-binding-bytes 262144 :max-request-binding-rows 256})
(def ^:private common [:control :query-id :spi-version :features :authorization :consistency :limits])

(defn- choices [descriptor request]
  (mapcat
    (fn [clause]
      (let [cols (columns descriptor clause)
            available (into {} (map (juxt :variable identity)) (:available-inputs request))
            positions (if (#{:github.issue/key :github.issue/number} (second (second (:terms clause)))) [0 2] [0])]
        (keep (fn [position]
                (let [[tag x] (nth (:terms clause) position)
                      column (some #(when (= x (:variable %)) %) cols)
                      bound? (contains? (:bindings request) x)
                      requires (if (and (= :var tag) (not bound?)) [column] [])]
                  (when (or (= :const tag) (and (= :var tag) (or bound? (= column (available x)))))
                    {:id (keyword "github-lookup" (str (:id clause) "-" position))
                     :clause clause :columns cols :requires requires :position position
                     :kind (if (zero? (long position)) :entity-lookup :value-lookup)}))) positions)))
    (filter #(= :pattern (:kind %)) (:clauses request))))

(defn- unknown-estimate [dependent?]
  (merge (zipmap (if dependent?
                  [:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
                   :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes]
                  [:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes])
                (repeat nil))
         {:confidence :unknown :provenance {:kind :unknown :id nil :observed-at-ms nil :valid-for-ms nil}}))

(defn- matching-row [plan bindings fact]
  (let [match (reduce (fn [out [term x]]
                        (when out
                          (case (first term)
                            :ignore out
                            :const (when (QueryKey/equiv (second term) x) out)
                            :var (let [sym (second term)]
                                   (if (contains? out sym)
                                     (when (if (contains? bindings sym) (QueryKey/equals (out sym) x)
                                               (QueryKey/equiv (out sym) x)) out)
                                     (assoc out sym x)))))) bindings (map vector (get-in plan [:clause :terms]) fact))]
    (when match (mapv match (map :variable (:columns plan))))))

(defn- lookup-key [compiled plan bindings]
  (let [[tag x] (nth (get-in plan [:clause :terms]) (:position plan))
        operand (if (= :const tag) x (get bindings x))
        type (get-in compiled [:accepted :identity :type])
        key (if (zero? (long (:position plan)))
              (when (and (ref/entity-ref? operand) (value/matches-type? type operand)) (ref/key-tuple operand))
              (if (= :github.issue/number (second (second (get-in plan [:clause :terms]))))
                [(get-in compiled [:accepted :dependencies :repository :id]) operand] operand))]
    (when (and (sequential? key) (= 2 (count key))
                (every? #(and (integer? %) (<= 1 % Long/MAX_VALUE)) key)
                (= (first key) (get-in compiled [:accepted :dependencies :repository :id])))
      (mapv long key))))

(defn- repository-path [compiled]
  (let [{:keys [owner name]} (get-in compiled [:accepted :dependencies :repository])]
    (str "/repos/" owner "/" name)))

(defn- next-row! [cursor check!]
  (let [{:keys [state plan inputs compiled projector profile request limiter blocked metrics]} cursor
        dependent? (boolean (seq (:requires plan)))]
    (loop []
      (check!)
      (let [{:keys [pending remaining input-index current-bindings current-id]} @state]
        (cond
          pending pending
          (seq remaining)
          (let [fact (first remaining)]
            (swap! state assoc :remaining (subvec remaining 1))
            (if-let [row (matching-row plan current-bindings fact)]
              (let [result {:row row :id current-id}] (swap! state assoc :pending result) result)
              (recur)))
          (>= (long input-index) (long (if dependent? (count @inputs) 1))) nil
          :else
          (let [input (when dependent? (nth @inputs input-index))
                bindings (merge (:bindings plan) (zipmap (map :variable (:requires plan)) (:values input)))
                key (lookup-key compiled plan bindings)]
            (swap! state assoc :input-index (inc (long input-index)) :current-bindings bindings :current-id (:binding-id input) :remaining [])
            (when key
              (let [document (http/get-json! profile (str (repository-path compiled) "/issues/" (second key))
                               api-version (:control request) limiter blocked check! metrics :fetch)
                    attribute (second (second (get-in plan [:clause :terms])))
                    facts (project compiled projector key document attribute)]
                (swap! metrics update :read-rows inc)
                (swap! state assoc :remaining facts)))
            (recur)))))))

(defrecord Cursor [compiled projector profile plan inputs request limiter blocked state session-state metrics reservation deregister!]
  spi/IQuerySourceCursor
  (-next-batch! [this {:keys [control max-rows max-bytes]}]
    (ensure! (and (identical? control (:control request)) (pos-int? max-rows) (pos-int? max-bytes)) :fetch)
    (let [check! #(do (active! state control) (active! session-state control))
          dependent? (boolean (seq (:requires plan))) columns (:columns plan)
          started (System/nanoTime)]
      (locking state
        (try
          (check!)
          (let [first-row (next-row! this check!)
                charge (fn [rows ids] (if dependent? (spi/batch-charge-bytes columns rows ids)
                                         (spi/batch-charge-bytes columns rows)))
                first-charge (long (if first-row (charge [(:row first-row)] [(:id first-row)]) (charge [] [])))
                _ (when (> first-charge (long max-bytes)) (fail! :query-source/limit :batch))
                window (if first-row (max first-charge (min 65536 (long max-bytes))) first-charge)
                token (when (pos? first-charge) (spi/reserve! control {:retained-bytes window :in-flight-batches 1}))]
            (try
              (let [[rows ids bytes exhausted?]
                    (loop [rows [] ids [] bytes (long (charge [] []))]
                      (if (= (count rows) (long max-rows))
                        [rows ids bytes (and (empty? (:remaining @state))
                                             (>= (long (:input-index @state))
                                                 (long (if dependent? (count @inputs) 1))))]
                        (if-let [{:keys [row id]} (next-row! this check!)]
                          (let [n (count rows)
                                extra (+ (- (long (value/value-charge [row])) (long (value/value-charge [])))
                                         (if dependent? (- (long (value/value-charge [id]))
                                                           (long (value/value-charge []))) 0))
                                attempted (if (zero? n) (long (charge [row] [id])) (+ bytes extra))]
                            (if (> attempted window)
                              [rows ids bytes false]
                              (do (swap! state dissoc :pending)
                                  (recur (conj rows (value/owned-copy row check!))
                                         (if dependent? (conj ids id) ids) attempted))))
                          [rows ids bytes true])))]
                (check!)
                (swap! metrics #(-> % (update :batches inc) (update :rows + (count rows))
                                    (update :charged-bytes + bytes)))
                (cond-> {:columns columns :rows rows :row-count (count rows) :byte-count bytes
                         :reservation token :exhausted? (boolean exhausted?)}
                  dependent? (assoc :binding-ids ids)))
              (catch Throwable e (when token (spi/release! token)) (throw e))))
          (finally (swap! metrics update :elapsed-ns + (- (System/nanoTime) started)))))))
  (-cursor-metrics [_] @metrics)
  (-close-cursor! [this]
    (let [[before _] (swap-vals! state assoc :closed? true)]
      (when-not (:closed? before)
        (when-let [close! @blocked] (close!))
        ;; Fetch owns the scratch until its blocked I/O has unwound.
        (locking state
          (reset! inputs nil)
          (swap! state dissoc :remaining :pending :current-bindings)
          (spi/release! reservation)
          (deregister! this))))
    nil))

(defrecord Session [owner descriptor compiled projector profile request info state reservations limiter blocked registration stats]
  spi/IQuerySourceSession
  (-session-info [_] info)
  (-open-cursor [_ open]
    (let [ctl (:control request)]
      (locking state
        (active! state ctl)
        (when-not (= (:definition-fingerprint descriptor) (:definition-fingerprint open))
          (fail! :query-source/stale-definition :open))
        (let [plan (get-in @state [:plans (:plan open)]) dependent? (boolean (seq (:requires plan)))]
          (ensure! (and plan (= (:id plan) (:alternative-id open))
                        (= (:source-symbol plan) (:source-symbol open)) (= (:scope-id plan) (:scope-id open))
                        (= (:region-id plan) (:region-id open))
                        (= (select-keys request common) (select-keys open common)) (empty? (:cursors @state))) :open)
          (if dependent?
            (do (ensure! (and (vector? (:bindings open)) (<= 1 (count (:bindings open))
                                (min (long (get-in request [:limits :binding-rows])) 256))) :open)
                (when (> (long (spi/binding-charge-bytes (:requires plan) (:bindings open)))
                         (min (long (get-in request [:limits :binding-bytes])) 262144))
                  (fail! :query-source/limit :open)))
            (ensure! (not (contains? open :bindings)) :open))
          (let [bytes (+ (http/scratch-bytes profile) (* 2 (long (value/value-charge [plan (:bindings open)]))))
                reservation (spi/reserve! ctl {:retained-bytes bytes})]
            (try
              (let [cursor (->Cursor compiled projector profile plan
                             (atom (when dependent? (value/owned-copy (:bindings open)))) request limiter blocked
                             (atom {:input-index 0 :remaining []}) state
                             (atom {:batches 0 :rows 0 :charged-bytes 0 :read-rows 0 :read-bytes 0
                                    :external-requests 0 :elapsed-ns 0}) reservation
                             #(swap! state update :cursors disj %))]
                (swap! state update :cursors conj cursor)
                cursor)
              (catch Throwable e (spi/release! reservation) (throw e))))))))
  (-cancel-session! [_ _]
    (swap! state assoc :cancelled? true)
    (when-let [close! @blocked] (close!))
    nil)
  (-close-session! [_]
    (let [[before _] (swap-vals! state assoc :closed? true)]
      (when-not (:closed? before)
        (when-let [close! @blocked] (close!))
        (run! spi/-close-cursor! (:cursors before))
        (when-let [r @registration] (spi/unregister-cancel! r))
        (.close ^Closeable limiter)
        (run! spi/release! @reservations)
        (reset! reservations [])
        (swap! state assoc :plans {} :cursors #{})
        (swap! stats update :live-sessions dec)))
    nil))

(defrecord GithubSource [descriptor compiled projector profile stats]
  spi/IQuerySource
  (-source-info [_] descriptor)
  (-open-session [this request]
    (let [ctl (:control request)]
      (spi/check-active! ctl)
      (ensure! (and (= 2 (:spi-version request)) (= :best-effort (:consistency request))
                    (= #{} (:features request)) (seq (:source-descriptors request))
                    (every? #(= descriptor %) (:source-descriptors request))) :acquisition)
      (let [reservation (spi/reserve! ctl {:retained-bytes (+ 4096 (long (value/value-charge [descriptor compiled])))})
            limiter (try (spi/request-limiter ctl {:max-concurrent-requests 1 :rate-limit nil})
                         (catch Throwable e (spi/release! reservation) (throw e)))
            blocked (atom nil) session (volatile! nil)]
        (try
          (let [scratch (spi/reserve! ctl {:retained-bytes (http/scratch-bytes profile)})]
            (try
              (let [repository (http/get-json! profile (repository-path compiled) api-version ctl limiter blocked
                                 #(spi/check-active! ctl) stats :acquisition)]
                (when-not (= (get repository "id") (get-in compiled [:accepted :dependencies :repository :id]))
                  (fail! :query-source/stale-definition :acquisition)))
              (finally (spi/release! scratch))))
          (let [acquired (->Session this descriptor compiled projector profile request
                           (merge (select-keys descriptor [:source-instance-id :compute-context-id])
                                  {:spi-version 2 :features #{} :consistency :best-effort :session-id (UUID/randomUUID)
                                   :authorization-token :github-profile :acquired-at-ms (System/currentTimeMillis)
                                   :data-version nil :consumption {:opens :many :max-open-cursors 1}
                                   :request-constraints {:max-concurrent-requests 1 :rate-limit nil}})
                           (atom {:plans {} :cursors #{}}) (atom [reservation]) limiter blocked (atom nil) stats)]
            (vreset! session acquired)
            (swap! stats update :live-sessions inc)
            (reset! (:registration acquired) (spi/register-cancel! ctl #(spi/-cancel-session! acquired %)))
            (spi/check-active! ctl)
            acquired)
          (catch Throwable e
            (if @session (spi/-close-session! @session)
                (do (.close ^Closeable limiter) (spi/release! reservation)))
            (throw e))))))
  (-plan-source [this session request]
    (let [state (:state session) ctl (:control request)]
      (locking state
        (active! state ctl)
        (ensure! (and (identical? this (:owner session)) (= descriptor (:source-descriptor request))
                      (= 2 (:spi-version request)) (= #{} (:features request))
                      (identical? ctl (get-in session [:request :control]))
                      (= (:query-id request) (get-in session [:request :query-id]))
                      (= (:scope-id request) (get-in descriptor [:scope :id]))) :planning)
        {:alternatives
         (mapv (fn [{:keys [id clause columns requires kind] :as candidate}]
                 (let [plan (merge candidate (select-keys request [:source-symbol :region-id :scope-id :bindings]))
                       token (spi/reserve! ctl {:retained-bytes (+ 2048 (* 2 (long (value/value-charge plan))))})]
                   (try
                     (let [handle (Object.)]
                       (swap! state assoc-in [:plans handle] (value/owned-copy plan))
                       (swap! (:reservations session) conj token)
                       {:id id :plan handle :covers #{(:id clause)} :prefilters #{} :requires requires :columns columns
                        :scope-id (:scope-id request) :completion :all-bindings
                        :access {:kind kind :anchor-clause (:id clause)}
                        :properties {:replay :unstable :ordering [] :blocking-state :none}
                        :constraints constraints :estimate (unknown-estimate (boolean (seq requires)))
                        :explain {:label "GitHub work-item key lookup"}})
                     (catch Throwable e (spi/release! token) (throw e))))) (choices descriptor request))}))))

(defn source
  "Reopen accepted metadata with an explicit runtime profile, without I/O.
  Requires :compute-context-id; optional :token, :api-base, :request-timeout-ms,
  :max-response-bytes, :max-json-nodes. Only best-effort keyed lookups are offered.
  Session setup verifies the repository's numeric ID; no implicit re-discovery.
  HTTP failures, including ambiguous 404s, fail the query. No automatic retries."
  [accepted profile]
  (let [compiled (compiled! accepted) profile (http/profile! profile)
        descriptor (assoc (mapping/source-info accepted (:compute-context-id profile)) :read-capabilities #{:best-effort})]
    (spi/validate-source-info! descriptor)
    (->GithubSource descriptor compiled (mapping/fact-projector compiled) profile
                    (atom {:external-requests 0 :read-bytes 0 :live-sessions 0}))))

(defn statistics
  "Cumulative acquisition work, separate from actual per-cursor work. No secrets."
  [^GithubSource source] @(:stats source))
