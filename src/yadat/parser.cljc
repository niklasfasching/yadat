(ns yadat.parser
  (:require [clojure.spec.alpha :as s]
            [yadat.util :as util]))

(s/def ::spec
  (s/alt :scalar (s/cat :element ::element :_ #{'.})
         :collection (s/spec (s/cat :elements (s/+ util/var?) :_ #{'...}))
         :tuple (s/spec (s/cat :elements (s/+ ::element)))
         :relation (s/cat :elements (s/+ ::element))))

(s/def ::element
  (s/alt :variable util/var?
         :pull (s/cat :_ #{'pull} :variable util/var? :spec any?)
         :aggregate (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                           :constants (s/* (complement util/var?))
                           :variable util/var?)))

(s/explain ::spec '[[?help ...]])

(s/explain ::spec '[?a ?b (pull ?a [*]) (agg id)])

(s/conform ::find-element '(pull ?id []))
(s/conform ::find-element '(agg foo bar ?id))

(case t
  :scalar (let [row (-> relation :rows first)]))
