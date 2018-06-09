(ns yadat.parser
  (:require [clojure.core.match :refer [match]]
            [yadat.util :as util]))

(defrecord AndClause [clauses])
(defrecord OrClause [clauses])
(defrecord NotClause [clauses])
(defrecord FunctionClause [f args vars])
(defrecord PredicateClause [f args])
(defrecord PatternClause [pattern])

(defrecord FindScalar [element])
(defrecord FindTuple [elements])
(defrecord FindRelation [elements])
(defrecord FindCollection [element])

(defrecord FindVariable [var])
(defrecord FindPull [var pattern])
(defrecord FindAggregate [f args])

(defrecord PullPattern [elements])

(defrecord PullWildcard [])
(defrecord PullAttribute [a])
(defrecord PullMap [m])
(defrecord PullAttributeWithOptions [a options])
(defrecord PullAttributeExpression [a options])

(defn clause [clause]
  (match [clause]
    [(['or & clauses] :seq)] (->OrClause (map parse-clause clauses))
    [(['not & clauses] :seq)] (->NotClause (map parse-clause clauses))
    [(['and & clauses] :seq)] (->AndClause (map parse-clause clauses))
    [[([f & args] :seq)]] (->PredicateClause f args)
    [[([f & args] :seq) vars]] (->FunctionClause f args vars)
    [[_ _ _]] (->PatternClause clause)
    :else (throw (ex-info "Invalid clause" {:clause clause}))))

(defn where-clauses [clauses]
  (->AndClause (map parse-clause clauses)))

(defn find-element [element]
  (match [element]
    [(v :guard util/var?)] (->FindVariable v)
    [(['pull (v :guard util/var?) [& pattern]] :seq)] (->FindPull v pattern)
    [([f & args] :seq)] (->FindAggregate f args)
    :else (throw (ex-info "Invalid find element" {:element element}))))

(defn find-spec [spec]
  (match [spec]
    [[e '.]] (->FindScalar (find-element e))
    [[[e '...]]] (->FindCollection (find-element e))
    [[[& es]]] (->FindTuple (map find-element es))
    [[& es]] (->FindRelation (map find-element es))
    :else (throw (ex-info "Invalid find spec" {:spec spec}))))

(defn pull-element [element]
  (match [element]
    ['*] (->PullWildcard)
    [(a :guard keyword?)] (->PullAttribute a)
    [(m :guard map?)] (->PullMap m)
    [([(a :guard keyword?) & options] :seq)] (->PullAttributeWithOptions
                                              a (apply hash-map options))
    :else (throw (ex-info "Invalid pull element" {:element element}))))

(defn pull-pattern [pattern]
  (->Pull (map pull-element pattern)))
