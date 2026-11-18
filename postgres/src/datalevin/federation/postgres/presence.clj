(ns datalevin.federation.postgres.presence
  "Remove fact-presence tests only when remaining SQL already implies them."
  (:require [clojure.set :as set]))

(def strict-operators #{:= :not= :< :<= :> :>= :like :not-like :like-any :in :not-in})

(defn strict-variables
  "Variables necessarily non-NULL when an admitted SQL predicate is TRUE.
  AND combines guarantees; OR retains only guarantees common to every branch.
  Empty membership and nil constants are conservatively excluded."
  [{:keys [kind operator clauses args]}]
  (if (= :boolean kind)
    (let [sets (mapv strict-variables clauses)]
      (case operator :and (apply set/union #{} sets)
            :or (if (seq sets) (apply set/intersection sets) #{}) #{}))
    (if (and (= :predicate kind) (strict-operators operator)
             (not-any? #(and (= :const (first %)) (nil? (second %))) args)
             (or (not (#{:in :not-in :like-any} operator))
                 (and (= :const (first (second args))) (seq (remove nil? (second (second args)))))))
      (into #{} (comp (filter #(= :var (first %))) (map second)) args) #{})))

(defn normalize
  "All removals are zero-parameter raw-column IS NOT NULL clauses. Retain
  every join/filter and the original order of all remaining conditions."
  [conditions variables predicates join-columns]
  (let [required (into #{} (mapcat strict-variables) predicates)
        ;; NOT of a composite equality can be TRUE with a NULL component.
        ;; Only scalar predicates prove every raw field present. Join equality
        ;; is a conjunction and separately proves all participating fields.
        scalar (filter #(= 1 (count (get-in variables [% :raw-sql]))) required)
        columns (into (set join-columns) (mapcat #(get-in variables [% :raw-sql])) scalar)
        implied (into #{} (map #(str % " IS NOT NULL")) columns)
        removed (filterv implied conditions)]
    {:conditions (filterv (complement implied) conditions)
     :removed removed :strict-variables (vec (sort required)) :guaranteed-columns (vec (sort columns))}))
