(ns yadat.find
  (:require [clojure.set :as set]
            [yadat.pull :as pull]
            [yadat.relation :as r]
            [yadat.util :as util]
            [yadat.where :as where]
            [clojure.walk :as walk]))

;; parser -> find
;; ->

(defn element-type [element]
  (cond
    (util/var? element) :variable
    (and (seq? element) (= (first element) 'pull)) :pull
    (seq? element) :aggregate
    :else (throw (ex-info "Invalid find element" {:element element}))))

(defn spec-type [[x1 x2 :as spec]]
  (cond
    (and (= (count spec) 2) (= x2 '.)) :scalar   ; [?a .]     -> single value
    (and (= (count spec) 1) (vector? x1) (= (count x1) 2)
         (= (second x1) '...)) :collection       ; [[?a ...]] -> list of values
    (and (= (count spec) 1) (vector? x1)) :tuple ; [[?a ?b]]  -> single tuple
    (vector? spec) :relation                     ; [?a ?b]    -> list of lists
    :else (throw (ex-info "Invalid find spec" {:spec spec}))))


;; in spec is list of elements
;; in = [ (src-var | rules-var | plain-symbol | binding)+ ]

(defn in-element-type [element]
  (cond
    (util/var? element) :scalar                  ; ?a
    (and (vector? element)
         (= (second element) '...)) :collection  ; [?a ...]
    (and (vector? element) (= (count element) 1)
         (vector (first element))) :relation     ; [[?a ?b]]
    (vector? element) :tuple                     ; [?a ?b]
    :else (throw (ex-info "Invalid in-element" {:element element}))))

;; scalar -> relation {:variables #{?a} #{{?a 1}}}
;; collection -> relation {:variables #{?a} #{{?a 1} {?a 2}}}
;; relation -> relation {:variables #{?a ?b} #{{?a 1 ?b 1} {?a 2 ?b 2}}}
;; tuple -> relation {:variables #{?a ?b} #{{?a 1 ?b 1}}}

;; need to distinguish those and rules and

(defmulti resolve-in-element
  (fn [in-spec inputs] (in-spec-type in-spec)))

'{:in [% ?var]}

(query db rules query)
;; rules set ... what is done with that?
;; i'm resolving to relations here...
;; rules set... where does that go?
;; result: relations[] + rules
;; rules can then be used as special predicates

;; rules is another big thing - and rules actually require a parser to distinguish required and optional argumets
;; non-bound variables are output!

;; rather than keeping going here... maybe tests + parser would be a better use of time


;; if i included times in the lsf dump:
;; -> could calc most prolific professor - teaches most
;; -> another one as gets around most, i.e. most rooms associated
;; -> most courses taught
;; all that requires everything is kinda stable
;; for that i need tests

;; think about how to make tests easier - example data?
