(ns datalevin.federation.postgres.summary
  "Sufficient native MIN state before recursively scoped qualification."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datalevin.federation.postgres.jdbc :as jdbc]
            [datalevin.federation.postgres.demand :as demand]))

(defn column [sql]
  (when-let [[_ alias name] (re-matches #"(r\d+)\.\"((?:[^\"]|\"\")+)\"" sql)]
    {:sql sql :alias alias :name (str/replace name "\"\"" "\"")}))

(defn equality [sql]
  (let [ref "(r[0-9]+\\.\"(?:[^\"]|\"\")+\")"]
    (when-let [[_ left right] (re-matches (re-pattern (str "\\(*\\s*" ref "\\s*=\\s*" ref "\\s*\\)*")) sql)]
      [(column left) (column right)])))

(defn props [tables {:keys [alias name]}]
  (get-in tables [alias :accepted :requirements :columns name]))

(defn pk [table]
  (set (map :column (get-in table [:accepted :requirements :primary-key]))))

(defn binary-association?
  "Recognize the supported physical association using a complete catalog read.
  Mapped or query-projected columns cannot establish physical arity."
  [{:keys [kind row-security? columns primary-key]}]
  (and (= "r" kind) (false? row-security?) (= 3 (count columns))
       (= 1 (count primary-key))
       (every? #(and (#{"int2" "int4" "int8"} (:type %)) (nil? (:enum %)))
               (vals columns))))

(defn- binary? [table binary-associations]
  (contains? binary-associations (get-in table [:accepted :requirements :table])))

(defn candidate [tables order records native used owner component]
  (let [local (filterv #(set/subset? (:aliases %) component) records)
        outer (filterv #(not (set/subset? (:aliases %) component)) records)
        cross (filterv #(seq (set/intersection component (:aliases %))) outer)
        interfaces
        (mapv (fn [record]
                (when (empty? (:params record))
                  (when-let [[left right] (equality (:sql record))]
                    (let [[inside outside] (if (component (:alias left)) [left right] [right left])
                          type (:type (props tables inside))]
                      (when (and (component (:alias inside)) (not (component (:alias outside)))
                                 (#{"int2" "int4" "int8"} type) (= type (:type (props tables outside))))
                        {:record record :left left :right right :inside inside :outside outside}))))) cross)
        keys (set (map #(get-in % [:inside :sql]) interfaces))
        values (filterv #(seq (set/intersection component (get-in native [% :aliases]))) used)
        columns (mapv #(when (= #{owner} (get-in native [% :aliases]))
                         (when (= 1 (count (get-in native [% :sql])))
                           (column (first (get-in native [% :sql]))))) values)
        key (get-in (first interfaces) [:inside])]
    (when (and (seq cross) (every? some? interfaces) (= 1 (count keys))
               (seq values) (every? some? columns) (every? #(= owner (:alias %)) columns)
               (not-any? #(= "__dtlv_summary_key" (:name %)) columns)
               (not-any? #(get-in tables [% :accepted :requirements :row-security?]) component)
               (= "r" (get-in tables [owner :accepted :requirements :kind]))
               (seq (pk (tables (:alias key))))
               (not (set/subset? (pk (tables (:alias key))) #{(:name key)})))
      {:owner owner :component component :local local :outer outer :interfaces interfaces :key key
       :columns (vec (distinct columns)) :variables values :order (.indexOf ^java.util.List order owner)})))

(defn compose [tables order nodes records outer required]
  (let [g (demand/graph (set/union nodes outer) records)
        boundary (into #{} (mapcat (fn [{:keys [aliases]}]
                                    (let [outside (set/difference aliases nodes)]
                                      (when-not (set/subset? outside outer)
                                        (throw (ex-info "Unknown summary ancestor" {::decline true})))
                                      (when (seq outside) (set/intersection nodes aliases))))) records)
        core (or (demand/connected-core g nodes (or required boundary) order) nodes)
        visible (set/union outer core)
        local (filterv #(set/subset? (:aliases %) visible) records)
        groups (demand/components g (set/difference nodes core) order)
        children (mapv #(compose tables order % (filterv (fn [r] (seq (set/intersection % (:aliases r)))) records) visible nil) groups)
        indices (into (mapv :index local) (mapcat :condition-order) children)]
    (when-not (= (sort indices) (sort (map :index records)))
      (throw (ex-info "Summary predicate coverage differs" {::decline true})))
    {:from (str/join ", " (for [a order :when (core a)]
                           (or (:summary-sql (tables a)) (demand/table-sql (tables a)))))
     :conditions (into (mapv :sql local) (map #(str "EXISTS (SELECT 1 FROM " (:from %) " WHERE " (demand/conjunction (:conditions %)) ")")) children)
     :params (vec (concat (mapcat :params local) (mapcat :params children)))
     :parameter-order (vec (concat (mapcat :parameter-indices local) (mapcat :parameter-order children)))
     :condition-order indices
     :all-aliases (vec (concat (filter core order) (mapcat :all-aliases children)))
     :tree {:aliases (filterv core order) :condition-indices (mapv :index local) :children (mapv :tree children)}}))

(defn choose
  [{:keys [tables conditions params input grouping window find with optional? absence? used
           native-expressions oversized binary-associations bad] :as options}]
  (let [fallback (demand/choose options)
        observed (into #{} (mapcat #(get-in native-expressions [% :aliases])) used)
        validation? (some (fn [{:keys [accepted columns]}]
                            (some #(#{"numeric" "timestamptz"}
                                      (get-in accepted [:requirements :columns % :type])) columns)) tables)
        supported? (and (not input) (empty? grouping) (not window) (empty? with) (not optional?) (not absence?)
                        (= oversized "FALSE") (not validation?) (string? bad)
                        (empty? (re-seq #"\b(r\d+)\s*\." (demand/unquoted bad)))
                        (not (str/includes? (demand/unquoted bad) "?")) (< 1 (count observed))
                        (seq find) (every? #(and (= :aggregate (:kind %)) (= :min (:operator %))) find)
                        (every? #(and (= 1 (count (get-in native-expressions [% :aliases])))
                                      (= 1 (count (get-in native-expressions [% :sql])))) used))]
    (if-not supported? fallback
      (try
        (let [order (mapv :alias tables) table-map (zipmap order tables)
              records (demand/condition-records conditions (vec params) (set order))
              g (demand/graph (set order) records)
              candidates
              (for [owner order :when (observed owner)
                    component (cons #{owner} (for [other order
                                                  :when (and (not (observed other)) ((g owner) other)
                                                             (binary? (table-map other) binary-associations))] #{owner other}))
                    :let [choice (candidate table-map order records native-expressions used owner component)]
                    :when choice] choice)
              selected (first (sort-by (juxt #(count (:component %)) :order #(str (sort (:component %)))) candidates))]
          (if-not selected fallback
            (let [{:keys [owner component local outer interfaces key columns]} selected
                  key-name "__dtlv_summary_key"
                  key-sql (str owner "." (jdbc/identifier key-name))
                  replacements (into {} (for [{:keys [record left right inside]} interfaces]
                                          [(:index record)
                                           (assoc record :sql (str "(" (if (= left inside) key-sql (:sql left)) " = "
                                                                   (if (= right inside) key-sql (:sql right)) ")")
                                                  :aliases (conj (set/difference (:aliases record) component) owner))]))
                  outer (mapv #(get replacements (:index %) %) outer)
                  summary-sql (str "(SELECT " (:sql key) " AS " (jdbc/identifier key-name) ", "
                                   (str/join ", " (map #(str "MIN(CASE WHEN " bad " THEN NULL ELSE " (:sql %) " END) AS " (jdbc/identifier (:name %))) columns))
                                   " FROM " (str/join ", " (map #(demand/table-sql (table-map %)) (filter component order)))
                                   " WHERE " (demand/conjunction (mapv :sql local)) " GROUP BY " (:sql key) ") AS " owner)
                  new-order (filterv #(or (= owner %) (not (component %))) order)
                  new-tables (assoc table-map owner (assoc (table-map owner) :summary-sql summary-sql))
                  result (compose new-tables new-order (set new-order) outer #{} observed)
                  local-indices (mapv :index local)
                  condition-order (into local-indices (:condition-order result))
                  parameter-order (into (vec (mapcat :parameter-indices local)) (:parameter-order result))]
              (when-not (and (= (sort condition-order) (range (count records)))
                             (= (sort parameter-order) (range (count params)))
                             (= (sort (:all-aliases result)) (sort new-order)))
                (throw (ex-info "Summary occurrence/binding coverage differs" {::decline true})))
              (assoc result :applied? true :summary? true
                     :condition-order condition-order :parameter-order parameter-order
                     :params (into (vec (mapcat :params local)) (:params result))
                     :summary {:owner owner :component (filterv component order) :key (:sql key)
                               :columns (mapv :sql columns) :local-condition-indices local-indices
                               :outer-condition-indices (mapv :index outer) :parameter-order parameter-order
                               :replaced-correlations (mapv (fn [{:keys [record]}] {:index (:index record) :before (:sql record)
                                                                                :after (:sql (replacements (:index record)))}) interfaces)}))))
        (catch clojure.lang.ExceptionInfo e
          (if (or (::decline (ex-data e)) (:datalevin.federation.postgres.demand/decline (ex-data e))) fallback (throw e)))))))
