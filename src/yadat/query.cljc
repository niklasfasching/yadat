(ns yadat.query
  (:require [yadat.util :as util]
            [yadat.relation :as r]
            [yadat.parser :as parser]
            [yadat.db :as db]
            [clojure.set :as set]
            [yadat.pull :as pull]))

(def default-pull-limit 1000)

(defprotocol FindElement
  (resolve-find-element [this db row])
  (element-vars [this]))

(defprotocol FindSpec
  (resolve-find-spec [this db rows])
  (spec-vars [this]))

(defprotocol Clause
  (resolve-clause [this db relations]))

(defprotocol PullPattern
  (resolve-pull-pattern [this db eid]))

(defprotocol PullElement
  (resolve-pull-element [this db entity]))

(defn extend-entity
  [db entity a datoms options]
  (let [{:keys [resolve as default]} options
        vs (mapv (fn [[e _ v :as datom]]
                   (cond
                     (and (db/reverse-ref? a) resolve) (resolve e)
                     (db/reverse-ref? a) {:db/id e}
                     resolve (resolve v)
                     (db/is? db a :component) (resolve
                                               (parser/->PullWildcard)
                                               db {:db/id v})
                     (db/is? db a :reference) {:db/id v}
                     :else v)) datoms)
        v-or-nil (if (db/is? db a :many) (not-empty vs) (first vs))
        v (if (some? v-or-nil) v-or-nil default)
        k (or as a)]
    (assoc entity k v)))

(defn select-datoms [db entity a {:keys [limit] :as options}]
  (let [query-datom (if (db/reverse-ref? a)
                      [nil (db/reversed-ref a) (:db/id entity)]
                      [(:db/id entity) a nil])
        datoms (take (or limit default-pull-limit) (db/select db query-datom))]
    datoms))

;; group by variables from with?
(defn aggregate [elements tuples]
  (let [aggregate-idx (reduce-kv (fn [m i e]
                                   (if (instance? yadat.parser.FindAggregate e)
                                     (let [f (util/resolve-symbol (:f e))
                                           constant-args (butlast (:args e))]
                                       (assoc m i [f constant-args]))
                                     m)) {} (vec elements))
        group-indexes (set/difference (set (range (count elements)))
                                      (set (keys aggregate-idx)))
        groups (vals (group-by #(map (partial nth %) group-indexes) tuples))]
    (map (fn [[tuple :as tuples]]
           (mapv (fn [v i]
                   (if-let [[f args] (get aggregate-idx i)]
                     (apply f (concat args [(map #(nth % i) tuples)]))
                     v)) tuple (range))) groups)))

(extend-protocol Clause
  yadat.parser.AndClause
  (resolve-clause [{:keys [clauses]} db relations]
    (loop [[clause & clauses] clauses
           relations relations]
      (if (and (nil? clause) (nil? clauses))
        relations
        (recur clauses (resolve-clause clause db relations)))))

  yadat.parser.OrClause
  (resolve-clause [{:keys [clauses]} db relations]
    (let [f (fn [clause]
              (resolve-clause (parser/->AndClause clause) db relations))
          out-relations (mapcat f clauses)
          or-relations (remove (set relations) out-relations)
          or-relation (r/merge or-relations r/union)]
      (conj relations or-relation)))

  yadat.parser.NotClause
  (resolve-clause [{:keys [clauses]} db relations]
    (let [and-clause (parser/->AndClause clauses)
          out-relations (resolve-clause and-clause db relations)
          not-relations (remove (set relations) out-relations)
          not-relation (r/merge not-relations r/inner-join)
          [relations relation] (r/split relations (:columns not-relation))
          new-relation (r/disjoin relation not-relation)]
      (conj relations new-relation)))

  yadat.parser.FunctionClause
  (resolve-clause [{:keys [args vars f]} db relations]
    (let [[relations relation] (r/split relations (filter util/var? args))
          rows (util/apply-function (:rows relation) f args vars)
          columns (into (:columns relation) vars)
          relation (r/relation columns rows)]
      (conj relations relation)))

  yadat.parser.PredicateClause
  (resolve-clause [{:keys [args f]} db relations]
    (let [[relations relation] (r/split relations (filter util/var? args))
          rows (util/apply-predicate (:rows relation) f args)
          relation (r/relation (:columns relation) rows)]
      (conj relations relation)))

  yadat.parser.PatternClause
  (resolve-clause [{:keys [pattern]} db relations]
    (let [datom (map (fn [x] (if (or (util/var? x) (= x '_)) nil x)) pattern)
          index (reduce-kv (fn [m i x]
                             (if (util/var? x) (assoc m i x) m)) {} pattern)
          datoms (db/select db datom)
          rows (map #(reduce-kv (fn [row i x]
                                  (if-let [variable (index i)]
                                    (assoc row variable x)
                                    row)) {} %) datoms)
          relation (r/relation (vals index) rows)]
      (conj relations relation))))

(extend-protocol FindElement
  yadat.parser.FindVariable
  (element-vars [{:keys [var]}]
    [var])
  (resolve-find-element [{:keys [var]} db row]
    (get row var))

  yadat.parser.FindPull
  (element-vars [{:keys [var]}]
    [var])
  (resolve-find-element [{:keys [pattern var]} db row]
    (resolve-pull-pattern pattern db (get row var)))

  yadat.parser.FindAggregate
  (element-vars [{:keys [args]}]
    (filter util/var? args))
  (resolve-find-element [{:keys [args]} db row]
    (some #(if (util/var? %) (get row %)) args)))

(extend-protocol FindSpec
  yadat.parser.FindScalar
  (spec-vars [{:keys [element]}]
    (element-vars element))
  (resolve-find-spec [{:keys [element]} db rows]
    (resolve-find-element element db (first rows)))

  yadat.parser.FindTuple
  (spec-vars [{:keys [elements]}]
    (mapcat element-vars elements))
  (resolve-find-spec [{:keys [elements]} db rows]
    (mapv #(resolve-find-element % db (first rows)) elements))

  yadat.parser.FindRelation
  (spec-vars [{:keys [elements]}]
    (mapcat element-vars elements))
  (resolve-find-spec [{:keys [elements]} db rows]
    (let [tuples (map (fn [row]
                        (mapv #(resolve-find-element % db row) elements))
                      rows)]
      (if (some #(instance? yadat.parser.FindAggregate %) elements)
        (aggregate elements tuples)
        tuples)))

  yadat.parser.FindCollection
  (spec-vars [{:keys [element]}]
    (element-vars element))
  (resolve-find-spec [{:keys [element]} db rows]
    (let [values (map #(resolve-find-element element db %) rows)]
      (if (instance? yadat.parser.FindAggregate element)
        (aggregate [element] (map vector values))
        values))))

(extend-protocol PullPattern
  yadat.parser.PullPattern
  (resolve-pull-pattern [{:keys [elements]} db eid]
    (loop [entity {:db/id eid}
           [e & es] elements]
      (if (and (nil? e) (nil? es))
        entity
        (recur (resolve-pull-element e db entity) es)))))

(extend-protocol PullElement
  yadat.parser.PullWildcard
  (resolve-pull-element [_ db entity]
    (let [datoms (db/select db [(:db/id entity) nil nil])]
      (reduce (fn [entity [a datoms]]
                (extend-entity db entity a datoms nil))
              entity (group-by second datoms))))

  yadat.parser.PullAttribute
  (resolve-pull-element [{:keys [a]} db entity]
    (let [datoms (select-datoms db entity a nil)]
      (extend-entity db entity a datoms nil)))

  yadat.parser.PullMap
  (resolve-pull-element [{:keys [m]} db entity]
    (let [[raw-spec pattern] (first (seq m))
          resolve (fn [eid] (resolve-pull-pattern pattern db eid))
          spec (if (keyword? raw-spec)
                 [raw-spec :resolve resolve]
                 (concat raw-spec [:resolve resolve]))]
      (resolve-pull-element db entity spec)))

  yadat.parser.PullAttributeWithOptions
  (resolve-pull-element [{:keys [a options]} db entity]
    (let [datoms (select-datoms db entity a options)]
      (extend-entity db entity a datoms options))))

(defn resolve
  "Resolve `query` map against `db`."
  [db query]
  (let [find (parser/find-spec (:find query))
        variables (set (concat (spec-vars find) (:with query)))
        ;; in (resolve-in (or (:in query) '[$])
        clause (parser/->AndClause (map parser/parse-clause (:where query)))
        tuples (->> (r/merge (resolve-clause clause db []) r/inner-join)
                    :rows
                    (map #(select-keys % variables)))]
    (resolve-find-spec find db tuples)))
