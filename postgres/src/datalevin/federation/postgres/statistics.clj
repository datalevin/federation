(ns ^:no-doc datalevin.federation.postgres.statistics
  "Advisory cardinalities from session-local PostgreSQL catalog statistics."
  (:require [clojure.set :as set]
            [datalevin.federation.postgres.mapping :as pm]
            [datalevin.federation.postgres.plan :as plan]))

(defn- finite? [x]
  (and (number? x) (Double/isFinite (double x))))

(defn- term-bound? [candidate [tag x]]
  (or (= :const tag)
      (and (= :var tag)
           (or (contains? (:bindings candidate) x)
               (some #(= x (:variable %)) (:requires candidate))))))

(defn distinct-count
  "Decode PostgreSQL's absolute count or negative fraction of table rows.
  A missing/zero statistic does not establish uniqueness or an empty column."
  [rows n]
  (when (and (finite? rows) (not (neg? rows)) (finite? n) (not (zero? n)))
    (max 1.0 (min (max 1.0 (double rows))
                  (if (neg? n) (* (- (double n)) rows) (double n))))))

(defn reference-statistics
  "Refine sampled distinctness with the size of an already acquired nominal
  reference domain. This is a low-confidence uniform-domain assumption, not
  referential integrity or a completeness proof. Single-column keys only."
  [accepted statistics views]
  (reduce-kv
    (fn [statistics attribute spec]
      (let [columns (pm/attribute-columns accepted attribute)
            domain (get-in spec [:reference :domain])
            target (when domain
                     (some #(when (and (= domain (get-in % [:accepted :identity :type :parameters :identity-domain-id]))
                                        (= 1 (count (pm/key-columns (:accepted %))))) %) views))
            rows (when target (:rows @(:statistics target)))]
        (if (and (finite? (:rows statistics)) (not (neg? (:rows statistics)))
                 (= 1 (count columns)) (finite? rows) (pos? rows))
          (assoc-in statistics [:columns (first columns) :domain-distinct]
                    (min (max 1.0 (double (:rows statistics))) (double rows))) statistics)))
    statistics (:attributes accepted)))

(defn filter-selectivity
  "Uniform selectivity of local equality/membership filters on columns that
  were not used to bind this access. Keep only the strongest filter, avoiding
  multiplication of correlated predicates. Unknown statistics retain one."
  [accepted candidate statistics]
  (let [clauses (:clauses candidate)
        known (into (set (keys (:bindings candidate))) (map :variable) (:requires candidate))
        counts
        (keep (fn [{:keys [operator args]}]
                (let [[a b] args [a b] (if (and (= operator :=) (= :const (first a))) [b a] [a b])
                      [tag variable] a [other x] b
                      n (case operator := 1
                          :in (when (and (or (sequential? x) (set? x)) (<= (count x) 256)
                                         (every? #(or (nil? %) (string? %)) x))
                                (max 1 (count (set (remove nil? x))))) nil)]
                  (when (and n (= :var tag) (= :const other) (not (known variable)))
                    (some (fn [{:keys [terms]}]
                            (when (= a (nth terms 2))
                              (let [columns (pm/attribute-columns accepted (second (second terms)))]
                                (when (= 1 (count columns))
                                  (when-let [d (distinct-count (:rows statistics)
                                                (get-in statistics [:columns (first columns) :distinct]))]
                                    (min 1.0 (/ (double n) d))))))) clauses))))
              (:predicates candidate))]
    (reduce min 1.0 counts)))

(defn- catalog-estimate
  "Estimate rows per access or per dependent binding. Null fractions and the
  most selective bound column refine reltuples; equality assumes uniform values.
  Correlated filters are not multiplied. Primary-key equality caps the estimate
  at one. These numbers only rank plans; they never establish emptiness."
  [accepted candidate statistics]
  (let [dependent? (boolean (seq (:requires candidate)))
        unknown (plan/unknown-estimate dependent?)
        rows (:rows statistics)]
    (if-not (and statistics (finite? rows) (not (neg? rows))) unknown
      (let [clauses (:clauses candidate)
            columns (set (mapcat #(pm/attribute-columns accepted (second (second (:terms %)))) clauses))
            bound-columns
            (into #{}
                  (mapcat (fn [{:keys [terms]}]
                            (concat (when (term-bound? candidate (first terms)) (pm/key-columns accepted))
                                    (when (term-bound? candidate (nth terms 2))
                                      (pm/attribute-columns accepted (second (second terms))))))) clauses)
            nonnull (fn [column]
                      (let [n (get-in statistics [:columns column :null-fraction])]
                        (if (and (finite? n) (<= 0 n 1)) (- 1.0 (double n)) 1.0)))
            present (reduce min (double rows) (map #(* rows (nonnull %)) columns))
            cardinality
            (reduce (fn [n column]
                      (if-let [distinct (let [catalog (distinct-count rows (get-in statistics [:columns column :distinct]))
                                             domain (distinct-count rows (get-in statistics [:columns column :domain-distinct]))]
                                         (when (or catalog domain) (max (or catalog 1.0) (or domain 1.0))))]
                        (min n (/ (* rows (nonnull column)) distinct)) n))
                    present bound-columns)
            cardinality (if (set/subset? (set (pm/key-columns accepted)) bound-columns)
                          (min cardinality 1.0) cardinality)]
        (assoc unknown (if dependent? :rows-per-binding :rows) cardinality
               :confidence :low
               :provenance {:kind :statistics :id (if (some :domain-distinct (vals (:columns statistics)))
                                                  "postgres-reference-domain-cardinality-v2" "postgres-catalog-cardinality-v1")
                            :observed-at-ms (:observed-at-ms statistics) :valid-for-ms nil})))))

(defn estimate
  "Prefer a prepared EXPLAIN cardinality for independent filtered accesses.
  Dependent accesses and missing/invalid preparations retain catalog estimates."
  ([accepted candidate statistics] (catalog-estimate accepted candidate statistics))
  ([accepted candidate statistics filtered]
   (if (and (empty? (:requires candidate)) (finite? (:rows filtered)) (not (neg? (:rows filtered))))
     (assoc (plan/unknown-estimate false) :rows (double (:rows filtered)) :confidence :low
            :provenance {:kind :statistics :id "postgres-explain-filtered-cardinality-v1"
                         :observed-at-ms (:observed-at-ms filtered) :valid-for-ms nil})
     (catalog-estimate accepted candidate statistics))))
