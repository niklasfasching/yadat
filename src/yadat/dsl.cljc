(ns yadat.dsl
  (:require [clojure.core.match :refer [match]]
            [yadat.util :as util]))

(defprotocol IRules
  (resolve-rules [this db relations]))

(defprotocol IRule
  (resolve-rule [this db relations]))

(defprotocol IFindElement
  (resolve-find-element [this db row])
  (element-vars [this]))

(defprotocol IFindSpec
  (resolve-find-spec [this db rows])
  (spec-vars [this]))

(defprotocol IClause
  (resolve-clause [this db relations]))

(defprotocol IPullPattern
  (resolve-pull-pattern [this db eid]))

(defprotocol IPullElement
  (resolve-pull-element [this db entity]))

(defrecord Rules [rules])
(defrecord Rule [name required-vars vars clauses])

(defrecord AndClause [clauses])
(defrecord OrClause [clauses])
(defrecord OrJoinClause [clauses])
(defrecord NotClause [clauses])
(defrecord NotJoinClause [clauses])
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

(defn pull-element [form]
  (match [form]
    ['*] (->PullWildcard)
    [(a :guard keyword?)] (->PullAttribute a)
    [(m :guard map?)] (let [[element pattern] (first (seq m))]
                        (->PullMap m))
    [([(a :guard keyword?) & options] :seq)] (->PullAttributeWithOptions
                                              a (apply hash-map options))
    :else (throw (ex-info "Invalid pull element" {:element form}))))

(defn pull-pattern [form]
  (->PullPattern (map pull-element form)))

(defn where-clause [form]
  (match [form]
    [(['or & clauses] :seq)] (->OrClause (map where-clause clauses))
    [(['or-join & clauses] :seq)] (->OrJoinClause (map where-clause clauses))
    [(['not & clauses] :seq)] (->NotClause (map where-clause clauses))
    [(['not-join & clauses] :seq)] (->NotJoinClause (map where-clause clauses))
    [(['and & clauses] :seq)] (->AndClause (map where-clause clauses))
    [[([(f :guard symbol?) & args] :seq)]] (->PredicateClause f args)
    [[([(f :guard symbol?) & args] :seq)
      & (vars :guard #(every? util/var? %))]] (->FunctionClause f args vars)
    [[_ _ _]] (->PatternClause form)
    :else (throw (ex-info "Invalid clause" {:clause form}))))

(defn where-clauses [form]
  (->AndClause (map where-clause form)))

(defn find-element [form]
  (match [form]
    [(v :guard util/var?)] (->FindVariable v)
    [(['pull (v :guard util/var?) [& pattern]] :seq)] (->FindPull
                                                       v (pull-pattern pattern))
    [([f & args] :seq)] (->FindAggregate f args)
    :else (throw (ex-info "Invalid find element" {:element form}))))

(defn find-spec [form]
  (match [form]
    [[e '.]] (->FindScalar (find-element e))
    [[[e '...]]] (->FindCollection (find-element e))
    [[[& es]]] (->FindTuple (map find-element es))
    [[& es]] (->FindRelation (map find-element es))
    :else (throw (ex-info "Invalid find spec" {:spec form}))))

(defn rule [form]
  (match [form]
    [[([(name :guard symbol?) [& required-vars]
        & vars] :seq) & clauses]] (->Rule name required-vars vars
                                          (map where-clause clauses))
    [[([(name :guard symbol?)
        & vars] :seq) & clauses]] (->Rule name nil vars
                                          (map where-clause clauses))
    :else (throw (ex-info "Invalid rule definition" {:rule form}))))

(defn rules [form]
  (->Rules (map rule form)))
