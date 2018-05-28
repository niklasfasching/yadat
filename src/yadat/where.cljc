(ns yadat.where
  (:require [yadat.db :as db]
            [yadat.util :as util]
            [yadat.relation :as r]
            [clojure.spec.alpha :as s]))

(declare resolve-clauses)

(s/def ::clause
  (s/alt :or (s/cat :type #{'or} :clauses (s/spec (s/+ ::clause)))
         :and (s/cat :type #{'and} :clauses (s/+ ::clause))
         :not (s/cat :type #{'not} :clauses (s/+ ::clause))
         :function (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                          :arguments (s/+ any?)
                          :variables (s/+ util/var?))
         :predicate (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                           :arguments (s/+ any?))
         :pattern (s/& (s/+ any?) #(and (some util/var? %)
                                        (<= (count %) 3)))))

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

(defn resolve-clause [db relations clause]
  (let [[t v] (s/conform ::clause)] ;; TODO handle invalid
    (case t
      :or
      (->> (mapcat #(resolve-clauses db relations [%]) (:clauses v))
           (remove (set relations))
           (r/merge r/union)
           (conj relations))

      :and
      (resolve-clauses db relations (:clauses v))

      :not
      (let [not-relation (->> (resolve-clauses db relations (:clauses v))
                              (remove (set relations))
                              (r/merge r/inner-join))
            [relations relation] (r/split relations (:columns not-relation))
            new-relation (r/disjoin relation not-relation)]
        (conj relations new-relation))

      :function
      (let [[relations relation] (r/split relations (filter util/var? (:arguments v)))
            rows (apply-function (:rows relation) (:fn v) (:arguments v) (:variables v))
            columns (into (:columns relation) (:variables v))
            relation (r/relation columns rows)]
        (conj relations relation))

      :predicate
      (let [[relations relation] (r/split relations (filter util/var? (:arguments v)))
            rows (apply-predicate (:rows relation) (:fn v) (:arguments v))
            relation (r/relation (:columns relation) rows)]
        (conj relations relation))

      :pattern
      (let [query-datom (map (fn [x] (if (or (util/var? x) (= x '_)) nil x)) clause)
            index (reduce-kv (fn [m i x] (if (util/var? x) (assoc m i x) m)) {} clause)
            datoms (db/select db query-datom)
            rows (map #(reduce-kv (fn [row i x]
                                    (if-let [variable (index i)]
                                      (assoc row variable x)
                                      row)) {} %) datoms)
            relation (r/relation (vals index) rows)]
        (conj relations relation)))))

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
