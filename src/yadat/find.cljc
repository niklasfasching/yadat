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
