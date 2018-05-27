(ns yadat.query
  (:refer-clojure :exclude [var?])
  (:require [clojure.set :as set]
            [yadat.db :as db]
            [yadat.relation :as r]
            [yadat.pull :as pull]
            [yadat.util :as util]
            [yadat.where :as where]))

(defn aggregate [elements tuples]
  (let [aggregate-idx (reduce-kv (fn [m i e]
                                   (if (= (find-element-type e) :aggregate)
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

(defn find-element-type [element]
  (cond
    (var? element) :variable
    (and (seq? element) (= (first element) 'pull)) :pull
    (seq? element) :aggregate
    :else (throw (ex-info "Invalid find element" {:element element}))))

(defmulti resolve-find-element
  (fn [db row element] (find-element-type element)))

(defmethod resolve-find-element :variable [db row element]
  (get row element))

(defmethod resolve-find-element :aggregate [db row element]
  (let [[_ & raw-args] element
        arg (some #(if (var? %) (get row %)) raw-args)]
    arg))

(defmethod resolve-find-element :pull [db row element]
  (let [[_ variable pattern] element
        eid (get row variable)]
    (pull/resolve-pull db eid pattern)))

(defn find-spec-type [[x1 x2 :as spec]]
  (cond
    (and (= (count spec) 2) (= x2 '.)) :scalar   ; [?a .]     -> single value
    (and (= (count spec) 1) (vector? x1) (= (count x1) 2)
         (= (second x1) '...)) :collection       ; [[?a ...]] -> list of values
    (and (= (count spec) 1) (vector? x1)) :tuple ; [[?a ?b]]  -> single tuple
    (vector? spec) :relation                     ; [?a ?b]    -> list of lists
    :else (throw (ex-info "Invalid find spec" {:spec spec}))))

(defmulti resolve-find-spec
  (fn [db relation spec] (find-spec-type spec)))

(defmethod resolve-find-spec :scalar [db relation spec]
  (let [row (-> relation :rows first)
        [element] spec]
    (resolve-find-element db row element)))

(defmethod resolve-find-spec :relation [db relation spec]
  (let [elements spec
        resolve-row (fn [row] (map #(resolve-find-element db row %) elements))
        tuples (map resolve-row (:rows relation))]
    (if (some #(= (find-element-type %) :aggregate) elements)
      (aggregate elements tuples)
      tuples)))

(defmethod resolve-find-spec :collection [db relation spec]
  (let [[[element]] spec
        values (map #(resolve-find-element db % element)
                    (:rows relation))]
    (if (= (find-element-type element) :aggregate)
      (aggregate [element] (map vector values))
      values)))

(defmethod resolve-find-spec :tuple [db relation spec]
  (let [[elements] spec
        row (-> relation :rows first)]
    (mapv #(resolve-find-element db row %) elements)))

(defn q
  "Executes the `query` map against `db`. Returns a set of tuples.
  db contains all facts
  Query is a map with the keys [find where]
  See datomic docs"
  [db {:keys [find where] :as query}]
  (let [relations (where/resolve-clauses db '() where)
        relation (r/merge relations r/inner-join)]
    (resolve-find-spec db relation find)))
