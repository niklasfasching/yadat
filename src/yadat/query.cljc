(ns yadat.query
  (:require [clojure.set :as set]
            [yadat.db :as db]
            [yadat.parser :as parser]
            [yadat.pull :as pull]
            [yadat.relation :as r]
            [yadat.util :as util]))

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

;; group by variables from with?
(defn aggregate [elements tuples]
  (let [aggregate-idx (reduce-kv (fn [m i e]
                                   (if (instance? yadat.parser.FindAggregate e)
                                     (let [f (util/resolve-symbol (:f e))
                                           constant-args (butlast (:args e))]
                                       (assoc m i [f constant-args]))
                                     m)) {} (vec elements))
        group-indexes (set/difference (set (range (count elements)))
                                      (set (keys aggregate-idx)))
        groups (vals (group-by #(map (partial nth %) group-indexes) tuples))]
    (map (fn [[tuple :as tuples]]
           (mapv (fn [v i]
                   (if-let [[f args] (get aggregate-idx i)]
                     (apply f (concat args [(map #(nth % i) tuples)]))
                     v)) tuple (range))) groups)))

(extend-protocol parser/IClause
  yadat.parser.AndClause
  (resolve-clause [{:keys [clauses]} db relations]
    (loop [[clause & clauses] clauses
           relations relations]
      (if (and (nil? clause) (nil? clauses))
        relations
        (recur clauses (parser/resolve-clause clause db relations)))))

  yadat.parser.OrClause
  (resolve-clause [{:keys [clauses]} db relations]
    (let [f (fn [clause] (parser/resolve-clause clause db relations))
          out-relations (mapcat f clauses)
          or-relations (remove (set relations) out-relations)
          or-relation (r/merge r/union or-relations)]
      (conj relations or-relation)))

  yadat.parser.NotClause
  (resolve-clause [{:keys [clauses]} db relations]
    (let [and-clause (parser/->AndClause clauses)
          out-relations (parser/resolve-clause and-clause db relations)
          not-relations (remove (set relations) out-relations)
          not-relation (r/merge r/inner-join not-relations)
          [relations relation] (r/split (:columns not-relation) relations)
          new-relation (r/disjoin relation not-relation)]
      (conj relations new-relation)))

  yadat.parser.FunctionClause
  (resolve-clause [{:keys [args vars f]} db relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-function (:rows relation) f args vars)
          columns (into (:columns relation) vars)
          relation (r/relation columns rows)]
      (conj relations relation)))

  yadat.parser.PredicateClause
  (resolve-clause [{:keys [args f]} db relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-predicate (:rows relation) f args)
          relation (r/relation (:columns relation) rows)]
      (conj relations relation)))

  yadat.parser.PatternClause
  (resolve-clause [{:keys [pattern]} db relations]
    (let [datom (map (fn [x] (if (or (util/var? x) (= x '_)) nil x)) pattern)
          index (reduce-kv (fn [m i x]
                             (if (util/var? x) (assoc m i x) m)) {} pattern)
          datoms (db/select db datom)
          rows (map #(reduce-kv (fn [row i x]
                                  (if-let [variable (index i)]
                                    (assoc row variable x)
                                    row)) {} %) datoms)
          relation (r/relation (vals index) rows)]
      (conj relations relation))))

(extend-protocol parser/IFindElement
  yadat.parser.FindVariable
  (resolve-find-element [{:keys [var]} db row]
    (get row var))

  yadat.parser.FindPull
  (resolve-find-element [{:keys [pattern var]} db row]
    (parser/resolve-pull-pattern pattern db (get row var)))

  yadat.parser.FindAggregate
  (resolve-find-element [{:keys [args]} db row]
    (some #(if (util/var? %) (get row %)) args)))

(extend-protocol parser/IFindSpec
  yadat.parser.FindScalar
  (resolve-find-spec [{:keys [element]} db rows]
    (parser/resolve-find-element element db (first rows)))

  yadat.parser.FindTuple
  (resolve-find-spec [{:keys [elements]} db rows]
    (mapv #(parser/resolve-find-element % db (first rows)) elements))

  yadat.parser.FindRelation
  (resolve-find-spec [{:keys [elements]} db rows]
    (let [tuples (map (fn [row]
                        (mapv #(parser/resolve-find-element % db row) elements))
                      rows)]
      (if (some #(instance? yadat.parser.FindAggregate %) elements)
        (aggregate elements tuples)
        tuples)))

  yadat.parser.FindCollection
  (resolve-find-spec [{:keys [element]} db rows]
    (let [values (map #(parser/resolve-find-element element db %) rows)]
      (if (instance? yadat.parser.FindAggregate element)
        (aggregate [element] (map vector values))
        values))))

(defn query
  "Resolve `query` map against `db`."
  [db query]
  (let [find (parser/find-spec (:find query))
        where (parser/where-clauses (:where query))
        variables (set (concat (parser/vars find) (:with query)))
        ;; in (resolve-in (or (:in query) '[$])
        tuples (->> (parser/resolve-clause where db [])
                    (r/merge r/inner-join)
                    (:rows)
                    (map #(select-keys % variables))
                    (set))]
    (parser/resolve-find-spec find db tuples)))
