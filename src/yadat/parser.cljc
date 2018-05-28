(ns yadat.parser
  (:require [clojure.spec.alpha :as s]
            [yadat.util :as util]))

(s/def ::clause
  (s/alt :or (s/cat :type #{'or} :clauses (s/spec (s/+ ::clause)))
         :and (s/cat :type #{'and} :clauses (s/+ ::clause))
         :not (s/cat :type #{'not} :clauses (s/+ ::clause))
         :function (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                          :arguments (s/+ any?)
                          :variables (s/+ util/var?))
         :predicate (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                           :arguments (s/+ any?))
         :pattern (s/& (s/+ any?) (fn [pattern]
                                    (and (some util/var? pattern)
                                         (<= (count pattern) 3))))))
(s/conform ::clause '(or [1 ?a]))

(s/def ::spec
  (s/alt :scalar (s/cat :element ::element :dot #{'.})
         :collection (s/spec (s/cat :elements (s/+ util/var?) :dots #{'...}))
         :tuple (s/spec (s/cat :elements (s/+ ::element)))
         :relation (s/cat :elements (s/+ ::element))))

(s/def ::element
  (s/alt :variable util/var?
         :pull (s/cat :type #{'pull} :variable util/var? :spec any?)
         :aggregate (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                           :constants (s/* (complement util/var?))
                           :variable util/var?)))

(s/conform ::spec '[[?help ...]])

(s/explain ::spec '[?a ?b (pull ?a [*]) (agg id)])

(s/conform ::find-element '(pull ?id []))
(s/conform ::find-element '(agg foo bar ?id))

(case t
  :scalar (let [row (-> relation :rows first)]))
