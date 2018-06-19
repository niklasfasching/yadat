(ns yadat.query
  (:require [clojure.set :as set]
            [yadat.db :as db]
            [yadat.dsl :as dsl]
            [yadat.pull :as pull]
            [yadat.relation :as r]
            [yadat.util :as util]))

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

(defprotocol IQuery
  (resolve-query [this inputs relations]))

(defprotocol IRules
  (resolve-rules [this dbs relations]))

(defprotocol IRule
  (resolve-rule [this dbs relations]))

(defprotocol IInput
  (resolve-input [this value]))

(defprotocol IFindElement
  (resolve-find-element [this dbs row]))

(defprotocol IFindSpec
  (resolve-find-spec [this dbs rows]))

(defprotocol IClause
  (resolve-clause [this dbs relations]))

(extend-protocol IClause
  yadat.dsl.AndClause
  (resolve-clause [{:keys [clauses]} dbs relations]
    (loop [[clause & clauses] clauses
           relations relations]
      (if (and (nil? clause) (nil? clauses))
        relations
        (recur clauses (resolve-clause clause dbs relations)))))

  yadat.dsl.OrClause
  (resolve-clause [{:keys [clauses]} dbs relations]
    (let [f (fn [clause] (resolve-clause clause dbs relations))
          out-relations (mapcat f clauses)
          or-relations (remove (set relations) out-relations)
          or-relation (r/merge r/union or-relations)]
      (conj relations or-relation)))

  yadat.dsl.NotClause
  (resolve-clause [{:keys [clauses]} dbs relations]
    (let [and-clause (dsl/->AndClause clauses)
          out-relations (resolve-clause and-clause dbs relations)
          not-relations (remove (set relations) out-relations)
          not-relation (r/merge r/inner-join not-relations)
          [relations relation] (r/split (:columns not-relation) relations)
          new-relation (r/disjoin relation not-relation)]
      (conj relations new-relation)))

  yadat.dsl.FunctionClause
  (resolve-clause [{:keys [args vars f]} dbs relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-function (:rows relation) f args vars)
          columns (into (:columns relation) vars)
          relation (r/relation columns rows)]
      (conj relations relation)))

  yadat.dsl.PredicateClause
  (resolve-clause [{:keys [args f]} dbs relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-predicate (:rows relation) f args)
          relation (r/relation (:columns relation) rows)]
      (conj relations relation)))

  yadat.dsl.PatternClause
  (resolve-clause [{:keys [pattern]} dbs relations]
    (let [dbs (first dbs) ;; TODO support multiple dbs
          datom (map (fn [x] (if (or (util/var? x) (= x '_)) nil x)) pattern)
          index (reduce-kv (fn [m i x]
                             (if (util/var? x) (assoc m i x) m)) {} pattern)
          datoms (db/select dbs datom)
          rows (map #(reduce-kv (fn [row i x]
                                  (if-let [variable (index i)]
                                    (assoc row variable x)
                                    row)) {} %) datoms)
          relation (r/relation (vals index) rows)]
      (conj relations relation))))

(extend-protocol IFindElement
  yadat.dsl.FindVariable
  (resolve-find-element [{:keys [var]} dbs row]
    (get row var))

  yadat.dsl.FindPull
  (resolve-find-element [{:keys [pattern var]} dbs row]
    (let [db (first dbs)] ;; TODO
      (pull/resolve-pull-pattern pattern db (get row var))))

  yadat.dsl.FindAggregate
  (resolve-find-element [{:keys [args]} dbs row]
    (some #(if (util/var? %) (get row %)) args)))

(extend-protocol IFindSpec
  yadat.dsl.FindScalar
  (resolve-find-spec [{:keys [element]} dbs rows]
    (resolve-find-element element dbs (first rows)))

  yadat.dsl.FindTuple
  (resolve-find-spec [{:keys [elements]} dbs rows]
    (mapv #(resolve-find-element % dbs (first rows)) elements))

  yadat.dsl.FindRelation
  (resolve-find-spec [{:keys [elements]} dbs rows]
    (let [tuples (map (fn [row]
                        (mapv #(resolve-find-element % dbs row) elements))
                      rows)]
      (if (some #(instance? yadat.dsl.FindAggregate %) elements)
        (aggregate elements tuples)
        tuples)))

  yadat.dsl.FindCollection
  (resolve-find-spec [{:keys [element]} dbs rows]
    (let [values (map #(resolve-find-element element dbs %) rows)]
      (if (instance? yadat.dsl.FindAggregate element)
        (aggregate [element] (map vector values))
        values))))

(extend-protocol IInput
  yadat.dsl.InputSource
  (resolve-input [this value]
    value)

  yadat.dsl.InputScalar
  (resolve-input [this value]
    (r/relation #{(:var this)} #{value}))

  yadat.dsl.InputTuple
  (resolve-input [this value]
    (r/relation (set (:vars this)) #{value}))

  yadat.dsl.InputCollection
  (resolve-input [this value]
    (r/relation #{(:var this)} (set value)))

  yadat.dsl.InputRelation
  (resolve-input [this value]
    (r/relation (set (:vars this)) (set value))))

(extend-protocol dsl/IQuery
  yadat.dsl.Query
  (resolve-query [this input-values]
    (let [variables (set (concat (dsl/vars (:find this))
                                 (dsl/vars (:with this))))
          inputs (map resolve-input (:inputs this) input-values)
          {relations true dbs false} (group-by
                                      #(= (type %) yadat.relation.Relation)
                                      inputs)
          tuples (->> (resolve-clause (:where this) dbs relations)
                      (r/merge r/inner-join)
                      (:rows)
                      (map #(select-keys % variables))
                      (set))]
      (resolve-find-spec (:find this) dbs tuples))))
