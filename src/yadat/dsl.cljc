(ns yadat.dsl
  (:require [clojure.core.match :refer [match]]
            [yadat.util :as util]))

(def cache (atom {}))

(defn update-cache
  "Return `cache` with [`raw-form` `parsed-form`]. Limits cache size to 100.
  This is a really naive cache - no strategy is followed, a random entry is
  removed when the cache grows to big. Should be replaced by a LRU / LFU cache
  later on but we need to start with something and this was the most basic thing
  i could think of."
  [cache raw-form parsed-form]
  (if (< (count cache) 100)
    (assoc cache raw-form parsed-form)
    (assoc (into {} (rest cache)) raw-form parsed-form)))

(defprotocol IVariableContainer
  (vars [this]))

(defprotocol IQuery
  (resolve-query [this inputs relations]))

(defprotocol IRules
  (resolve-rules [this dbs relations]))

(defprotocol IRule
  (resolve-rule [this dbs relations]))

(defprotocol IInput
  (resolve-input [this value]))

(defprotocol IInputs
  (resolve-inputs [this values]))

(defprotocol IFindElement
  (resolve-find-element [this dbs row]))

(defprotocol IFindSpec
  (resolve-find-spec [this dbs rows]))

(defprotocol IClause
  (resolve-clause [this dbs relations]))

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

(defrecord Inputs [inputs])

(defrecord InputSource [src-var])

(defrecord InputScalar [var]
  IVariableContainer
  (vars [this] [var]))

(defrecord InputTuple [vars]
  IVariableContainer
  (vars [this] vars))

(defrecord InputCollection [var]
  IVariableContainer
  (vars [this] [var]))

(defrecord InputRelation [vars]
  IVariableContainer
  (vars [this] vars))

(defrecord With [vars]
  IVariableContainer
  (vars [this] vars))

(defn pull-element [form]
  (match [form]
    ['*] (->PullWildcard)
    [(a :guard keyword?)] (->PullAttribute a)
    [(m :guard map?)] (let [[element pattern] (first (seq m))] ;; todo
                        (->PullMap m))
    [([(a :guard keyword?) & options] :seq)] (->PullAttributeWithOptions
                                              a (apply hash-map options))
    :else (throw (ex-info "Invalid pull element" {:element form}))))

(defn pull-pattern [src form]
  (->PullPattern src (map pull-element form)))

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


;; The pull expression pattern can also be bound dynamically as an :in parameter to query:
(defn find-element [form]
  (match [form]
    [(v :guard util/var?)] (->FindVariable v)
    [(['pull (v :guard util/var?) [& pattern]] :seq)] (->FindPull
                                                       v (pull-pattern '$ pattern)) ;; TODO
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

(defn input [form]
  (match [form]
    [(v :guard util/src?)] (->InputSource v)
    [(v :guard util/var?)] (->InputScalar v)
    [[[& vs]]] (->InputRelation vs)
    [[v '...]] (->InputCollection v)
    [[& vs]] (->InputTuple vs)
    :else (throw (ex-info "Invalid input definition" {:input form}))))

(defn inputs [form]
  (if form
    (->Inputs (map input form))
    (->Inputs (map input '[$]))))

(defn with [form]
  (->With form))

(defn query [form]
  (if-let [parsed-query (get cache form)]
    parsed-query
    (let [parsed-query (->Query
                        (find-spec (:find form))
                        (where-clauses (:where form))
                        (inputs (:in form))
                        (with (:with form)))] ;; TODO validate query
      (swap! cache update-cache form parsed-query))))
