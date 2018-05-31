(ns yadat.where
  (:require [yadat.db :as db]
            [yadat.util :as util]
            [yadat.relation :as r]))

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

(defn clause-type [clause]
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

(declare resolve-clauses)

(defmulti resolve-clause
  "Resolves `clause` into a relation based on `db` and `relations`.
  Returns a list of relations of which the first relation must be the resolved
  relation. We cannot just return the resolved relation as there are clauses
  that modify the input `relations`."
  (fn [db relations clause] (clause-type clause)))

(defmethod resolve-clause :or [db relations clause]
  (let [[_ & clauses] clause
        out-relations (mapcat #(resolve-clauses db relations [%]) clauses)
        or-relations (remove (set relations) out-relations)
        or-relation (r/merge or-relations r/union)]
    (conj relations or-relation)))

(defmethod resolve-clause :and [db relations clause]
  (let [[_ & clauses] clause]
    (resolve-clauses db relations clauses)))

(defmethod resolve-clause :not [db relations clause]
  (let [[_ & clauses] clause
        out-relations (resolve-clauses db relations clauses)
        not-relations (remove (set relations) out-relations)
        not-relation (r/merge not-relations r/inner-join)
        [relations relation] (r/split relations (:columns not-relation))
        new-relation (r/disjoin relation not-relation)]
    (conj relations new-relation)))

(defmethod resolve-clause :function [db relations clause]
  (let [[[raw-f & raw-args] & raw-vars] clause
        [relations relation] (r/split relations (filter util/var? raw-args))
        rows (apply-function (:rows relation) raw-f raw-args raw-vars)
        columns (into (:columns relation) raw-vars)
        relation (r/relation columns rows)]
    (conj relations relation)))

(defmethod resolve-clause :predicate [db relations clause]
  (let [[[raw-f & raw-args]] clause
        [relations relation] (r/split relations (filter util/var? raw-args))
        rows (apply-predicate (:rows relation) raw-f raw-args)
        relation (r/relation (:columns relation) rows)]
    (conj relations relation)))

(defmethod resolve-clause :pattern [db relations clause ]
  (let [query-datom (map (fn [x] (if (or (util/var? x) (= x '_)) nil x)) clause)
        index (reduce-kv (fn [m i x]
                           (if (util/var? x) (assoc m i x) m)) {} clause)
        datoms (db/select db query-datom)
        rows (map #(reduce-kv (fn [row i x]
                                (if-let [variable (index i)]
                                  (assoc row variable x)
                                  row)) {} %) datoms)
        relation (r/relation (vals index) rows)]
    (conj relations relation)))

(defn resolve-clauses
  "Resolves `clauses` against `db` and `relations`. Returns list of relations.
  Each clause is resolved into a relation and `conj`[oined] to the given list of
  relations."
  [db relations clauses]
  (loop [[clause & clauses] clauses
         relations relations]
    (if (and (nil? clause) (nil? clauses))
      relations
      (let [relations (resolve-clause db relations clause)]
        (recur clauses relations)))))
