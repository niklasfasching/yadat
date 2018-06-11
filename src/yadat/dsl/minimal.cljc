(ns yadat.dsl.minimal
  (:require [yadat.util :as util]
            [yadat.relation :as r]
            [yadat.dsl :as dsl]
            [yadat.db :as db]
            [clojure.set :as set]))

(def default-pull-limit 1000)

(defn apply-function [rows raw-f raw-args raw-vars]
  (let [f (util/resolve-symbol raw-f)]
    (map (fn [r] (let [args (mapv #(get r % %) raw-args)
                       result (apply f args)]
                   (if (= (count raw-vars) 1)
                     (into r [[(first raw-vars) result]])
                     (into r (map vector result raw-vars))))) rows)))

(defn apply-predicate [rows raw-f raw-args]
  (let [f (util/resolve-symbol raw-f)]
    (filter (fn [b] (apply f (mapv #(get b % %) raw-args))) rows)))

(defn datom->value [db [e a v :as datom]]
  (cond
    (and (db/reverse-ref? a) resolve) (resolve e)
    (db/reverse-ref? a) {:db/id e}
    resolve (resolve v)
    (db/is? db a :component) (resolve
                              (dsl/->PullWildcard)
                              db {:db/id v})
    (db/is? db a :reference) {:db/id v}
    :else v))

(defn extend-entity
  [db entity a datoms options]
  (let [{:keys [resolve as default]} options
        vs (mapv #(datom->value db %) datoms)
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
                                   (if (instance? yadat.dsl.FindAggregate e)
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

(extend-protocol dsl/IClause
  yadat.dsl.AndClause
  (resolve-clause [{:keys [clauses]} db relations]
    (loop [[clause & clauses] clauses
           relations relations]
      (if (and (nil? clause) (nil? clauses))
        relations
        (recur clauses (dsl/resolve-clause clause db relations)))))

  yadat.dsl.OrClause
  (resolve-clause [{:keys [clauses]} db relations]
    (let [f (fn [clause] (dsl/resolve-clause clause db relations))
          out-relations (mapcat f clauses)
          or-relations (remove (set relations) out-relations)
          or-relation (r/merge r/union or-relations)]
      (conj relations or-relation)))

  yadat.dsl.NotClause
  (resolve-clause [{:keys [clauses]} db relations]
    (let [and-clause (dsl/->AndClause clauses)
          out-relations (dsl/resolve-clause and-clause db relations)
          not-relations (remove (set relations) out-relations)
          not-relation (r/merge r/inner-join not-relations)
          [relations relation] (r/split (:columns not-relation) relations)
          new-relation (r/disjoin relation not-relation)]
      (conj relations new-relation)))

  yadat.dsl.FunctionClause
  (resolve-clause [{:keys [args vars f]} db relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-function (:rows relation) f args vars)
          columns (into (:columns relation) vars)
          relation (r/relation columns rows)]
      (conj relations relation)))

  yadat.dsl.PredicateClause
  (resolve-clause [{:keys [args f]} db relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-predicate (:rows relation) f args)
          relation (r/relation (:columns relation) rows)]
      (conj relations relation)))

  yadat.dsl.PatternClause
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

(extend-protocol dsl/IFindElement
  yadat.dsl.FindVariable
  (element-vars [{:keys [var]}]
    [var])
  (resolve-find-element [{:keys [var]} db row]
    (get row var))

  yadat.dsl.FindPull
  (element-vars [{:keys [var]}]
    [var])
  (resolve-find-element [{:keys [pattern var]} db row]
    (dsl/resolve-pull-pattern pattern db (get row var)))

  yadat.dsl.FindAggregate
  (element-vars [{:keys [args]}]
    (filter util/var? args))
  (resolve-find-element [{:keys [args]} db row]
    (some #(if (util/var? %) (get row %)) args)))

(extend-protocol dsl/IFindSpec
  yadat.dsl.FindScalar
  (spec-vars [{:keys [element]}]
    (dsl/element-vars element))
  (resolve-find-spec [{:keys [element]} db rows]
    (dsl/resolve-find-element element db (first rows)))

  yadat.dsl.FindTuple
  (spec-vars [{:keys [elements]}]
    (mapcat dsl/element-vars elements))
  (resolve-find-spec [{:keys [elements]} db rows]
    (mapv #(dsl/resolve-find-element % db (first rows)) elements))

  yadat.dsl.FindRelation
  (spec-vars [{:keys [elements]}]
    (mapcat dsl/element-vars elements))
  (resolve-find-spec [{:keys [elements]} db rows]
    (let [tuples (map (fn [row]
                        (mapv #(dsl/resolve-find-element % db row) elements))
                      rows)]
      (if (some #(instance? yadat.dsl.FindAggregate %) elements)
        (aggregate elements tuples)
        tuples)))

  yadat.dsl.FindCollection
  (spec-vars [{:keys [element]}]
    (dsl/element-vars element))
  (resolve-find-spec [{:keys [element]} db rows]
    (let [values (map #(dsl/resolve-find-element element db %) rows)]
      (if (instance? yadat.dsl.FindAggregate element)
        (aggregate [element] (map vector values))
        values))))

(extend-protocol dsl/IPullPattern
  yadat.dsl.PullPattern
  (resolve-pull-pattern [{:keys [elements]} db eid]
    (loop [entity {:db/id eid}
           [e & es] elements]
      (if (and (nil? e) (nil? es))
        entity
        (recur (dsl/resolve-pull-element e db entity) es)))))

(extend-protocol dsl/IPullElement
  yadat.dsl.PullWildcard
  (resolve-pull-element [_ db entity]
    (let [datoms (db/select db [(:db/id entity) nil nil])]
      (reduce (fn [entity [a datoms]]
                (extend-entity db entity a datoms nil))
              entity (group-by second datoms))))

  yadat.dsl.PullAttribute
  (resolve-pull-element [{:keys [a]} db entity]
    (let [datoms (select-datoms db entity a nil)]
      (extend-entity db entity a datoms nil)))

  ;; some of this stuff should be pulled out into the dsl
  ;; e.g. the [element pattern|recursion-limit part]
  ;; no idea how to go from here yet
  yadat.dsl.PullMap
  (resolve-pull-element [{:keys [m]} db entity]
    (let [[raw-spec pattern] (first (seq m))
          resolve (fn [eid] (dsl/resolve-pull-pattern pattern db eid))
          spec (if (keyword? raw-spec)
                 [raw-spec :resolve resolve]
                 (concat raw-spec [:resolve resolve]))]
      (dsl/resolve-pull-element spec db entity)))

  yadat.dsl.PullAttributeWithOptions
  (resolve-pull-element [{:keys [a options]} db entity]
    (let [datoms (select-datoms db entity a options)]
      (extend-entity db entity a datoms options))))

(defn query
  "Resolve `query` map against `db`."
  [db query]
  (let [find (dsl/find-spec (:find query))
        where (dsl/where-clauses (:where query))
        variables (set (concat (dsl/spec-vars find) (:with query)))
        ;; in (resolve-in (or (:in query) '[$])
        tuples (->> (dsl/resolve-clause where db [])
                    (r/merge r/inner-join)
                    :rows
                    (map #(select-keys % variables)))]
    (dsl/resolve-find-spec find db tuples)))
