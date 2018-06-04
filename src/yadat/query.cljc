(ns yadat.query
  (:refer-clojure :exclude [resolve])
  (:require [clojure.core.match :refer [match]]
            [yadat.util :as util]
            [yadat.relation :as r]
            [yadat.db :as db]
            [clojure.set :as set]
            [yadat.pull :as pull]))

;; should have the resolve in it's own namespace i guess
;; would that mean where, find, pull, in having own namespaces?
;; all for query
;; otherwise really confusing
;; but cannot define multiple resolve (w/o namespacing them)
;; - multiple protocols with shared method name in same ns -> overwrite
;; so... call it resolve-find-spec, resolve-find-element, resolve-clause
;;

(defprotocol FindSpec
  (resolve-find-spec [_ db rows]))

(defprotocol FindElement
  (resolve-find-element [_ db row]))

(defprotocol Clause
  (resolve-clause [_ db relations]))

(defrecord AndClause [clauses]
  Clause
  (resolve-clause [_ db relations]
    (loop [[clause & clauses] clauses
           relations relations]
      (if (and (nil? clause) (nil? clauses))
        relations
        (recur clauses (resolve-clause clause db relations))))))

(defrecord OrClause [clauses]
  Clause
  (resolve-clause [_ db relations]
    (let [out-relations (mapcat #(resolve-clause (->AndClause %) db relations)
                                clauses)
          or-relations (remove (set relations) out-relations)
          or-relation (r/merge or-relations r/union)]
      (conj relations or-relation))))

(defrecord NotClause [clauses]
  Clause
  (resolve-clause [_ db relations]
    (let [out-relations (resolve-clause (->AndClause clauses) db relations)
          not-relations (remove (set relations) out-relations)
          not-relation (r/merge not-relations r/inner-join)
          [relations relation] (r/split relations (:columns not-relation))
          new-relation (r/disjoin relation not-relation)]
      (conj relations new-relation))))

(defrecord FunctionClause [f args vars]
  Clause
  (resolve-clause [_ db relations]
    (let [[relations relation] (r/split relations (filter util/var? args))
          rows (util/apply-function (:rows relation) f args vars)
          columns (into (:columns relation) vars)
          relation (r/relation columns rows)]
      (conj relations relation))))

(defrecord PredicateClause [f args]
  Clause
  (resolve-clause [_ db relations]
    (let [[relations relation] (r/split relations (filter util/var? args))
          rows (util/apply-predicate (:rows relation) f args)
          relation (r/relation (:columns relation) rows)]
      (conj relations relation))))

(defrecord PatternClause [pattern]
  Clause
  (resolve-clause [_ db relations]
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

(defrecord FindVariable [var]
  FindElement
  (resolve-find-element [_ db row]
    (get row var)))

(defrecord FindPull [var pattern]
  FindElement
  (resolve-find-element [_ db row]
    (pull/resolve-pull-pattern pattern db (get row var))))

(defrecord FindAggregate [f args]
  FindElement
  (resolve-find-element [_ db row]
    (some #(if (util/var? %) (get row %)) args)))

(defn aggregate [elements tuples]
  (let [aggregate-idx (reduce-kv (fn [m i e]
                                   (if (instance? FindAggregate e)
                                     (let [[f-symbol & args] e
                                           f (util/resolve-symbol f-symbol)
                                           constant-args (butlast args)]
                                       (assoc m i [f constant-args]))
                                     m)) {} elements)
        group-indexes (set/difference (set (range (count elements)))
                                      (set (keys aggregate-idx)))
        groups (vals (group-by #(map (partial nth %) group-indexes) tuples))]
    (map (fn [[tuple :as tuples]]
           (mapv (fn [v i]
                   (if-let [[f args] (get aggregate-idx i)]
                     (apply f (concat args [(map #(nth % i) tuples)]))
                     v)) tuple (range))) groups)))

(defrecord FindScalar [element]
  FindSpec
  (resolve-find-spec [_ db rows]
    (resolve-find-element element db (first rows))))

(defrecord FindTuple [elements]
  FindSpec
  (resolve-find-spec [_ db rows]
    (mapv #(resolve-find-element % db (first rows)) elements)))

(defrecord FindRelation [elements]
  FindSpec
  (resolve-find-spec [_ db rows]
    (let [tuples (map (fn [row]
                        (mapv #(resolve-find-element % db row) elements))
                      rows)]
      (if (some #(instance? FindAggregate) elements)
        (aggregate elements tuples)
        tuples))))

(defrecord FindCollection [element]
  FindSpec
  (resolve-find-spec [_ db rows]
    (let [values (map #(resolve-find-element element db %) rows)]
      (if (instance? FindAggregate element)
        (aggregate [element] (map vector values))
        values))))

(defn ->Clause [clause]
  (match [clause]
    [(['or & clauses] :seq)] (->OrClause (map ->Clause clauses))
    [(['not & clauses] :seq)] (->NotClause (map ->Clause clauses))
    [(['and & clauses] :seq)] (->AndClause (map ->Clause clauses))
    [[([f & args] :seq)]] (->PredicateClause f args)
    [[([f & args] :seq) vars]] (->FunctionClause f args vars)
    [[_ _ _]] (->PatternClause clause)
    :else (throw (ex-info "Invalid clause" {:clause clause}))))

(defn ->FindElement [element]
  (match [element]
    [(v :guard util/var?)] (->FindVariable v)
    [(['pull (v :guard util/var?) [& pattern]] :seq)] (->FindPull v pattern)
    [([f & args] :seq)] (->FindAggregate f args)
    :else (throw (ex-info "Invalid find element" {:element element}))))

(defn ->FindSpec [spec]
  (match [spec]
    [[e '.]] (->FindScalar (->FindElement e))
    [[[e '...]]] (->FindCollection (->FindElement e))
    [[[& es]]] (->FindTuple (map ->FindElement es))
    [[& es]] (->FindRelation (map ->FindElement es))
    :else (throw (ex-info "Invalid find spec" {:spec spec}))))
