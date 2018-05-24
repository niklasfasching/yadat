(ns yadat.query
  (:refer-clojure :exclude [var?])
  (:require [clojure.set :as set]
            [yadat.db :as db]
            [yadat.relation :as r]
            [yadat.pull :as pull]
            [yadat.util :as util]))

(defn var? [x]
  (and (symbol? x) (= \? (first (name x)))))

(defn apply-function [rows raw-f raw-args raw-vars]
  (let [f (util/resolve-symbol raw-f)]
    (map (fn [r] (let [args (mapv #(get r % %) raw-args)
                       result (apply f args)]
                   (if (= (count raw-vars) 1)
                     (into r [[(first raw-vars) result]])
                     (into r (map vector result raw-vars))))) rows)))

(defn apply-predicate [rows raw-f raw-args]
  (let [f (util/resolve-symbol raw-f)]
    (filter (fn [b] (apply f (mapv #(get b % %) raw-args))) rows)))

(defn where-clause-type [clause]
  (cond
    (and (seq? clause)
         (= (first clause) 'or)) :or
    (and (seq? clause)
         (= (first clause) 'and)) :and
    (and (seq? clause)
         (= (first clause) 'not)) :not
    (and (vector? clause)
         (seq? (first clause))
         (> (count clause) 1)) :function
    (and (vector? clause)
         (seq? (first clause))
         (= (count clause) 1)) :predicate
    (vector? clause) :pattern
    :else (throw (ex-info "Could not resolve clause" {:clause clause}))))

(declare resolve-where-clauses)

(defmulti resolve-where-clause
  "Resolves `clause` into a relation based on `db` and `relations`.
  Returns a list of relations of which the first relation must be the resolved
  relation. We cannot just return the resolved relation as there are clauses
  that modify the input `relations`."
  (fn [db relations clause] (where-clause-type clause)))

(defmethod resolve-where-clause :or [db relations clause]
  (let [[_ & clauses] clause
        out-relations (mapcat #(resolve-where-clauses db relations [%]) clauses)
        or-relations (remove (set relations) out-relations)
        or-relation (r/merge or-relations r/union)]
    (conj relations or-relation)))

(defmethod resolve-where-clause :and [db relations clause]
  (let [[_ & clauses] clause]
    (resolve-where-clauses db relations clauses)))

(defmethod resolve-where-clause :not [db relations clause]
  (let [[_ & clauses] clause
        out-relations (resolve-where-clauses db relations clauses)
        not-relations (remove (set relations) out-relations)
        not-relation (r/merge not-relations r/inner-join)
        [relations relation] (r/split relations (:columns not-relation))
        new-relation (r/disjoin relation not-relation)]
    (conj relations new-relation)))

(defmethod resolve-where-clause :function [db relations clause]
  (let [[[raw-f & raw-args] & raw-vars] clause
        [relations relation] (r/split relations (filter var? raw-args))
        rows (apply-function (:rows relation) raw-f raw-args raw-vars)
        columns (into (:columns relation) raw-vars)
        relation (r/relation columns rows)]
    (conj relations relation)))

(defmethod resolve-where-clause :predicate [db relations clause]
  (let [[[raw-f & raw-args]] clause
        [relations relation] (r/split relations (filter var? raw-args))
        rows (apply-predicate (:rows relation) raw-f raw-args)
        relation (r/relation (:columns relation) rows)]
    (conj relations relation)))

(defmethod resolve-where-clause :pattern [db relations clause ]
  (let [query-datom (map (fn [x] (if (or (var? x) (= x '_)) nil x)) clause)
        index (reduce-kv (fn [m i x] (if (var? x) (assoc m i x) m)) {} clause)
        datoms (db/select db query-datom)
        rows (map #(reduce-kv (fn [row i x]
                                (if-let [variable (index i)]
                                  (assoc row variable x)
                                  row)) {} %) datoms)
        relation (r/relation (vals index) rows)]
    (conj relations relation)))

(defn resolve-where-clauses
  "Resolves `clauses` against `db` and `relations`. Returns list of relations.
  Each clause is resolved into a relation and `conj`[oined] to the given list of
  relations."
  [db relations clauses]
  (loop [[clause & clauses] clauses
         relations relations]
    (if (and (nil? clause) (nil? clauses))
      relations
      (let [relations (resolve-where-clause db relations clause)]
        (recur clauses relations)))))

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
  (throw (ex-info "Not implemented" {})))

(defmethod resolve-find-element :pull [db row element]
  (let [[_ variable pattern] element
        eid (get row variable)]
    (pull/resolve-pull db eid pattern)))

(defn find-spec-type [[x1 x2 :as spec]]
  (cond
    (and (= (count spec) 2) (= x2 '.)) :scalar   ; [?a .]
    (and (= (count spec) 1) (vector? x1) (= (count x1) 2)
         (= (second x1) '...)) :collection       ; [[?a ...]]
    (and (= (count spec) 1) (vector? x1)) :tuple ; [[?a ?b]]
    (vector? spec) :relation                     ; [?a ?b]
    :else (throw (ex-info "Invalid find spec" {:spec spec}))))

(defmulti resolve-find-spec
  (fn [db relation spec] (find-spec-type spec)))

(defmethod resolve-find-spec :scalar [db relation spec]
  (let [row (-> relation :rows first)
        [element] spec]
    (resolve-find-element db row element)))

(defmethod resolve-find-spec :relation [db relation spec]
  (let [xf (map (fn [row] (mapv #(resolve-find-element db row %) spec)))]
    (transduce xf conj #{} (:rows relation))))

(defmethod resolve-find-spec :collection [db relation spec]
  (let [[[element]] spec
        xf (map #(resolve-find-element db % element))]
    (transduce xf conj [] (:rows relation))))

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
  (let [relations (resolve-where-clauses db '() where)
        relation (r/merge relations r/inner-join)]
    (resolve-find-spec db relation find)))
