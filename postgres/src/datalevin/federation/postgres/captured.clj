(ns ^:no-doc datalevin.federation.postgres.captured
  "Session-owned, budgeted lookup indexes over immutable captured rows."
  (:require [datalevin.federation.entity-ref :as ref]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.plan :as plan]
            [datalevin.query.source :as spi]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl QueryKey$EquivSet QueryKey$KeyMap]
           [java.util ArrayList]))

(defn- term-bound? [bindings [tag x]]
  (or (= :const tag) (and (= :var tag) (contains? bindings x))))

(defn- selector [candidate]
  ;; Entity keys are unique. Prefer them even if the access was anchored on
  ;; another attribute; otherwise use the chosen bound value before residuals.
  (let [clauses (cons (:anchor candidate) (:clauses candidate))
        bindings (into (or (:bindings candidate) {}) (map #(vector (:variable %) nil)) (:requires candidate))]
    (or (some (fn [{:keys [terms]}]
                (when (term-bound? bindings (first terms)) {:attribute ::entity :term (first terms)})) clauses)
        (some (fn [{:keys [terms]}]
                (when (term-bound? bindings (nth terms 2))
                  {:attribute (second (second terms)) :term (nth terms 2)})) clauses))))

(defn- row-value [accepted attribute row]
  (if (= ::entity attribute)
    (ref/entity-ref (get-in accepted [:identity :type]) (mapv row (pm/key-columns accepted)))
    (let [raw (mapv row (pm/attribute-columns accepted attribute))]
      (when (every? some? raw)
        (if (some #{::plan/oversized} raw) ::plan/oversized
          (if-let [type (get-in accepted [:attributes attribute :reference :type])]
            (ref/entity-ref type raw)
            (if (= 1 (count raw)) (first raw) raw)))))))

(defn- clear! [{:keys [entries equivalent unknown reservations]}]
  (when entries (.clear ^QueryKey$KeyMap entries))
  (when equivalent (.clear ^QueryKey$EquivSet equivalent))
  (when unknown (.clear ^ArrayList unknown))
  (run! spi/release! reservations))

(defn close! [indexes]
  (let [[before _] (swap-vals! indexes (constantly nil))]
    (doseq [index (vals before) :when (map? index)] (clear! index))))

(defn- build! [context accepted rows attribute equiv?]
  (let [entries (QueryKey$KeyMap.) equivalent (when equiv? (QueryKey$EquivSet.))
        unknown (ArrayList.) reservations (atom []) headroom (volatile! nil)
        reserve! #(swap! reservations conj (spi/reserve! (:control context) {:retained-bytes %}))]
    (try
      ;; Keep space for an owned output page while admitting this optional
      ;; cache. Release this scratch reservation after the build, not into it.
      (vreset! headroom (spi/reserve! (:control context) {:retained-bytes 1048576}))
      (reserve! 4096)
      (doseq [row rows]
        (jdbc/check! context)
        (swap! (:metrics context) update :read-rows inc)
        (let [x (row-value accepted attribute row)]
          (when (some? x)
            ;; Includes table growth, bucket/row references, key ownership and
            ;; reservation bookkeeping. Captured payloads can be borrowed, but
            ;; logical reference/tuple keys may allocate new owned values.
            (reserve! (+ 512 (* 4 (value/value-charge [x]))))
            (if (= ::plan/oversized x)
              ;; A size marker cannot prove a failed equality. Keep these rows
              ;; as residuals so materializing an oversized value still fails.
              (.add unknown row)
              (let [key (if equivalent
                          (let [canonical (.get equivalent x)]
                            (if (some? canonical) canonical (do (.add equivalent x) x))) x)
                    bucket (or (.get entries key)
                               (let [bucket (ArrayList.)] (.put entries key bucket) bucket))]
                (.add ^ArrayList bucket row))))))
      {:entries entries :equivalent equivalent :unknown unknown :reservations @reservations}
      (catch Throwable e
        (clear! {:entries entries :equivalent equivalent :unknown unknown :reservations @reservations})
        (if (and (= :query-source/limit (:error (ex-data e)))
                 (= :retained-bytes (:category (ex-data e))))
          ::declined
          (throw e)))
      (finally (when-let [token @headroom] (spi/release! token))))))

(defn row-source!
  "Select captured candidate rows without changing the ordinary row matcher.
  Called under the session lock. Indexes are lazy, reused across opens, and
  optional when the shared retained-memory ledger cannot admit a complete one."
  [context accepted {:keys [snapshot indexes]} candidate]
  (let [rows @snapshot]
    (if-let [{:keys [attribute term]} (when (seq rows) (selector candidate))]
      (let [equiv? (= :const (first term)) cache-key [attribute equiv?]
            index (or (get @indexes cache-key)
                      (let [index (build! context accepted rows attribute equiv?)]
                        (swap! indexes assoc cache-key index) index))]
        (if (= ::declined index) (constantly rows)
          (fn [bindings]
            (jdbc/check! context)
            (let [x (if equiv? (second term) (get bindings (second term)))
                  key (if-let [equivalent (:equivalent index)] (.get ^QueryKey$EquivSet equivalent x) x)]
              (concat (when (some? key) (.get ^QueryKey$KeyMap (:entries index) key)) (:unknown index))))))
      (constantly rows))))
