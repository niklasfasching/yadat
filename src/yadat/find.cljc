(ns yadat.find
  (:require [clojure.set :as set]
            [yadat.pull :as pull]
            [yadat.relation :as r]
            [yadat.util :as util]
            [yadat.where :as where]
            [clojure.walk :as walk]))

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

(defn aggregate [elements tuples]
  (let [aggregate-idx (reduce-kv (fn [m i e]
                                   (if (= (element-type e) :aggregate)
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

(defmulti resolve-element
  (fn [db row element] (element-type element)))

(defmethod resolve-element :variable [db row element]
  (get row element))

(defmethod resolve-element :aggregate [db row element]
  (let [[_ & raw-args] element
        arg (some #(if (util/var? %) (get row %)) raw-args)]
    arg))

(defmethod resolve-element :pull [db row element]
  (let [[_ variable pattern] element
        eid (get row variable)]
    (pull/resolve-pull db eid pattern)))

(defmulti resolve-spec
  (fn [db tuples spec] (spec-type spec)))

(defmethod resolve-spec :scalar [db tuples spec]
  (let [[element] spec]
    (resolve-element db (first tuples) element)))

(defmethod resolve-spec :relation [db tuples spec]
  (let [elements spec
        resolve-tuple (fn [tuple] (mapv #(resolve-element db tuple %) elements))
        out-tuples (map resolve-tuple tuples)]
    (if (some #(= (element-type %) :aggregate) elements)
      (aggregate elements out-tuples)
      out-tuples)))

(defmethod resolve-spec :collection [db tuples spec]
  (let [[[element]] spec
        values (map #(resolve-element db % element) tuples)]
    (if (= (element-type element) :aggregate)
      (aggregate [element] (map vector values))
      values)))

(defmethod resolve-spec :tuple [db tuples spec]
  (let [[elements] spec]
    (mapv #(resolve-element db (first tuples) %) elements)))


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
