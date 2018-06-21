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

(defrecord Query [find where in with])

(defrecord Rules [rules]
  IVariableContainer
  (vars [this] (mapcat vars rules)))

(defrecord Rule [name required-vars vars clauses]
  IVariableContainer
  (vars [this] (concat required-vars vars)))

(defrecord AndClause [src clauses]
  IVariableContainer
  (vars [this] (mapcat vars clauses)))

(defrecord OrClause [src clauses]
  IVariableContainer
  (vars [this] (mapcat vars clauses)))

(defrecord OrJoinClause [src vars clauses]
  IVariableContainer
  (vars [this] vars))

(defrecord NotClause [src clauses]
  IVariableContainer
  (vars [this] (mapcat vars clauses)))

(defrecord NotJoinClause [src vars clauses]
  IVariableContainer
  (vars [this] vars))

(defrecord FunctionClause [f args vars]
  IVariableContainer
  (vars [this] vars))

(defrecord PredicateClause [f args]
  IVariableContainer
  (vars [this] []))

(defrecord PatternClause [src pattern]
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

(defrecord FindPull [src var pattern]
  IVariableContainer
  (vars [this] [var]))

(defrecord FindAggregate [f args]
  IVariableContainer
  (vars [this] (filter util/var? args)))

(defrecord PullPattern [elements])
(defrecord PullWildcard [])
(defrecord PullAttribute [a])
(defrecord PullMap [a pattern])
(defrecord PullAttributeWithOptions [a options])
(defrecord PullAttributeExpression [a options])

(defrecord Inputs [inputs])

(defrecord InputSource [src])

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

(def vars? (partial every? util/var?))

(defn parse-pull-element [form]
  (match [form]
    ['*]
    (->PullWildcard)

    [(a :guard keyword?)]
    (->PullAttribute a)

    [(m :guard [map? #(= (count %) 1)])]
    (apply ->PullMap (first (seq m)))

    [([(a :guard keyword?) & (options :guard #(even? (count %)))] :seq)]
    (->PullAttributeWithOptions a (apply hash-map options))

    :else (throw (ex-info "Invalid pull element" {:element form}))))

(defn parse-pull-pattern [form]
  (->PullPattern (map parse-pull-element form)))

(defn parse-clause [form]
  (match [form]
    [(['or & clauses] :seq)]
    (->OrClause nil (map parse-clause clauses))

    [(['or (src :guard util/src?) & clauses] :seq)]
    (->OrClause src (map parse-clause clauses))

    [(['or-join [& (vars :guard vars?)] & clauses] :seq)]
    (->OrJoinClause nil vars (map parse-clause clauses))

    [(['or-join (src :guard util/src?) [& (vars :guard vars?)] & clauses] :seq)]
    (->OrJoinClause src vars (map parse-clause clauses))

    [(['not & clauses] :seq)]
    (->NotClause nil (map parse-clause clauses))

    [(['not (src :guard util/src?) & clauses] :seq)]
    (->NotClause src (map parse-clause clauses))

    [(['not-join [& vars] & clauses] :seq)]
    (->NotJoinClause nil vars (map parse-clause clauses))

    [(['not-join (src :guard util/src?) [& vars] & clauses] :seq)]
    (->NotJoinClause src vars (map parse-clause clauses))

    [(['and & clauses] :seq)]
    (->AndClause nil (map parse-clause clauses))

    [(['and (src :guard util/src?) & clauses] :seq)]
    (->AndClause src (map parse-clause clauses))

    [[([(f :guard symbol?) & args] :seq)]]
    (->PredicateClause f args)

    [[([(f :guard symbol?) & args] :seq) & (vars :guard vars?)]]
    (->FunctionClause f args vars)

    [[(src :guard util/src?) _ _ _]]
    (->PatternClause src form)

    [[_ & _]]
    (->PatternClause nil form)

    :else (throw (ex-info "Invalid clause" {:clause form}))))

;; The pull expression pattern can also be bound dynamically as an :in parameter to query:
(defn parse-find-element [form]
  (match [form]
    [(v :guard util/var?)]
    (->FindVariable v)

    [(['pull (v :guard util/var?) pattern] :seq)]
    (->FindPull nil v pattern)

    [(['pull (src :guard util/src?) (v :guard util/var?) pattern] :seq)]
    (->FindPull src v pattern)

    [([f & args] :seq)]
    (->FindAggregate f args)

    :else (throw (ex-info "Invalid find element" {:element form}))))

(defn parse-find-spec [form]
  (match [form]
    [[e '.]] (->FindScalar (parse-find-element e))
    [[[e '...]]] (->FindCollection (parse-find-element e))
    [[[& es]]] (->FindTuple (map parse-find-element es))
    [[& es]] (->FindRelation (map parse-find-element es))
    :else (throw (ex-info "Invalid find spec" {:spec form}))))

(defn parse-rule [form]
  (match [form]
    [[([(name :guard symbol?) [& required-vars] & vars] :seq) & clauses]]
    (->Rule name required-vars vars (map parse-clause clauses))

    [[([(name :guard symbol?) & vars] :seq) & clauses]]
    (->Rule name nil vars (map parse-clause clauses))

    :else (throw (ex-info "Invalid rule definition" {:rule form}))))

(defn parse-rules [form]
  (->Rules (map parse-rule form)))

(defn parse-input [form]
  (match [form]
    [(v :guard util/src?)] (->InputSource v)
    [(v :guard util/var?)] (->InputScalar v)
    [[[& vs]]] (->InputRelation vs)
    [[v '...]] (->InputCollection v)
    [[& vs]] (->InputTuple vs)
    :else (throw (ex-info "Invalid input definition" {:input form}))))

(defn parse-query [form]
  (if-let [parsed-query (get cache form)]
    parsed-query
    (let [parsed-query (->Query
                        (parse-find-spec (:find form))
                        (->AndClause nil (map parse-clause (:where form)))
                        (->Inputs (map parse-input (or (:in form) '[$])))
                        (->With (:with form)))]
      ;; TODO validate query
      (swap! cache update-cache form parsed-query)
      parsed-query)))
