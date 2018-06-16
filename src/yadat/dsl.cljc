(ns yadat.dsl
  (:require [clojure.core.match :refer [match]]
            [yadat.util :as util]))

(defprotocol IVariableContainer
  (vars [this]))

(defprotocol IQuery
  (resolve-query [this inputs relations]))

(defprotocol IRules
  (resolve-rules [this db relations]))

(defprotocol IRule
  (resolve-rule [this db relations]))

(defprotocol IFindElement
  (resolve-find-element [this db row]))

(defprotocol IFindSpec
  (resolve-find-spec [this db rows]))

(defprotocol IClause
  (resolve-clause [this db relations]))

(defprotocol IPullPattern
  (resolve-pull-pattern [this db eid]))

(defprotocol IPullElement
  (resolve-pull-element [this db entity]))

(defrecord Query [find where in with])

(defrecord Rules [rules]
  IVariableContainer
  (vars [this] (mapcat vars rules)))

(defrecord Rule [name required-vars vars clauses]
  IVariableContainer
  (vars [this] (concat required-vars vars)))

(defrecord AndClause [clauses]
  IVariableContainer
  (vars [this] (mapcat vars clauses)))

(defrecord OrClause [clauses]
  IVariableContainer
  (vars [this] (mapcat vars clauses)))

(defrecord OrJoinClause [vars clauses]
  IVariableContainer
  (vars [this] vars))

(defrecord NotClause [clauses]
  IVariableContainer
  (vars [this] (mapcat vars clauses)))

(defrecord NotJoinClause [vars clauses]
  IVariableContainer
  (vars [this] vars))

(defrecord FunctionClause [f args vars]
  IVariableContainer
  (vars [this] vars))

(defrecord PredicateClause [f args]
  IVariableContainer
  (vars [this] []))

(defrecord PatternClause [pattern]
  IVariableContainer
  (vars [this] (filter util/var? pattern)))

(defrecord FindScalar [element]
  IVariableContainer
  (vars [this] (vars element)))

(defrecord FindTuple [elements]
  IVariableContainer
  (vars [this] (mapcat vars elements)))

(defrecord FindRelation [elements]
  IVariableContainer
  (vars [this] (mapcat vars elements)))

(defrecord FindCollection [element]
  IVariableContainer
  (vars [this] (vars element)))

(defrecord FindVariable [var]
  IVariableContainer
  (vars [this] [var]))

(defrecord FindPull [var pattern]
  IVariableContainer
  (vars [this] [var]))

(defrecord FindAggregate [f args]
  IVariableContainer
  (vars [this] (filter util/var? args)))

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
    [(m :guard map?)] (let [[element pattern] (first (seq m))] ;; todo
                        (->PullMap m))
    [([(a :guard keyword?) & options] :seq)] (->PullAttributeWithOptions
                                              a (apply hash-map options))
    :else (throw (ex-info "Invalid pull element" {:element form}))))

(defn pull-pattern [form]
  (->PullPattern (map pull-element form)))

(defn where-clause [form]
  (match [form]
    [(['or & clauses] :seq)] (->OrClause (map where-clause clauses))
    [(['or-join [& vars] & clauses] :seq)] (->OrJoinClause
                                            vars (map where-clause clauses))
    [(['not & clauses] :seq)] (->NotClause (map where-clause clauses))
    [(['not-join [& vars] & clauses] :seq)] (->NotJoinClause
                                             vars (map where-clause clauses))
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

;; inputs cannot be parsed at query parse time, must be resolved to relations and shit at execution time

(def query-cache {})
(defn validate-query [])

(defn query [form]
  (let [query (->Query
               (find-spec (:find form))
               (where-clauses (:where form))
               nil
               nil)]
    ;; validate vars in find exist in body

    ))
