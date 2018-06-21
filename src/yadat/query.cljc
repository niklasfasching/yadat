(ns yadat.query
  (:require [clojure.set :as set]
            [yadat.db :as db]
            [yadat.dsl :as dsl]
            [yadat.pull :as pull]
            [yadat.relation :as r]
            [yadat.util :as util]))

(def ^:dynamic *default-source* nil)

(defn apply-function [rows raw-f raw-args raw-vars]
  (let [f (util/resolve-symbol raw-f)]
    (map (fn [r] (let [args (mapv #(get r % %) raw-args)
                       result (apply f args)]
                   (if (= (count raw-vars) 1)
                     (into r [[(first raw-vars) result]])
                     (into r (map vector result raw-vars))))) rows)))



(defn aggregate-tuples [elements sources tuples]
  (mapv (fn [value element i]
          (if (instance? yadat.dsl.FindAggregate element)
            (let [f (or (get sources (:f element))
                        (util/resolve-symbol (:f element)))
                  constant-args (butlast (:args element))
                  values (map #(get % i) tuples)]
              (apply f (concat constant-args [values])))
            value)) (first tuples) elements (range)))

(defn resolve-tuples
  [elements sources rows]
  (let [aggregate-indexes (keep-indexed
                           (fn [i e] (if (instance? yadat.dsl.FindAggregate e)
                                       i nil)) elements)
        tuples (map (fn [r] (mapv #(resolve-find-element % sources r) elements))
                    rows)]
    (if (empty? aggregate-indexes)
      tuples
      (let [args (mapcat #(vector % nil) aggregate-indexes)
            groups (vals (group-by #(apply assoc (conj args %)) tuples))]
        (map (fn [tuples] (aggregate-tuples elements sources tuples))
             groups)))))

(defn resolve-var [var relations]
  (if-let [{:keys [rows]} (some #(#{var} (:columns %)) relations)]
    (get (first rows) var)))

(defprotocol IInput
  (resolve-input [this value]))

(defprotocol IInputs
  (resolve-inputs [this values]))

(defprotocol IFindElement
  (resolve-find-element [this sources row]))

(defprotocol IFindSpec
  (resolve-find-spec [this sources rows]))

(defprotocol IClause
  (resolve-clause [this sources relations]))

(extend-protocol IClause
  yadat.dsl.AndClause
  (resolve-clause [{:keys [src clauses]} sources relations]
    (binding [*default-source* (get sources src *default-source*)]
      (loop [[clause & clauses] clauses
             relations relations]
        (if (and (nil? clause) (nil? clauses))
          relations
          (recur clauses (resolve-clause clause sources relations))))))

  yadat.dsl.OrClause
  (resolve-clause [{:keys [src clauses]} sources relations]
    (binding [*default-source* (get sources src *default-source*)]
      (let [f (fn [clause] (resolve-clause clause sources relations))
            out-relations (mapcat f clauses)
            or-relations (remove (set relations) out-relations)
            or-relation (r/merge r/union or-relations)]
        (conj relations or-relation))))

  yadat.dsl.NotClause
  (resolve-clause [{:keys [src clauses]} sources relations]
    (binding [*default-source* (get sources src *default-source*)]
      (let [and-clause (dsl/->AndClause clauses)
            out-relations (resolve-clause and-clause sources relations)
            not-relations (remove (set relations) out-relations)
            not-relation (r/merge r/inner-join not-relations)
            [relations relation] (r/split (:columns not-relation) relations)
            new-relation (r/disjoin relation not-relation)]
        (conj relations new-relation))))

  yadat.dsl.FunctionClause
  (resolve-clause [{:keys [args vars f]} sources relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          rows (apply-function (:rows relation) f args vars)
          columns (into (:columns relation) vars)
          relation (r/relation columns rows)]
      (conj relations relation)))

  yadat.dsl.PredicateClause
  (resolve-clause [{:keys [f args]} sources relations]
    (let [[relations relation] (r/split (filter util/var? args) relations)
          resolved-f (or (get sources f) (util/resolve-symbol f))
          filtered-rows (filter (fn [row]
                                  (let [args (map #(get row % %) args)]
                                    (apply resolved-f args))) (:rows relation))
          filtered-relation (r/relation (:columns relation) filtered-rows)]
      (conj relations filtered-relation)))

  yadat.dsl.PatternClause
  (resolve-clause [{:keys [src pattern]} sources relations]
    (let [db (get sources src *default-source*)
          datom (map (fn [x] (if (or (util/var? x) (= x '_)) nil x)) pattern)
          index (reduce-kv (fn [m i x]
                             (if (util/var? x) (assoc m i x) m)) {} pattern)
          datoms (db/select db datom)
          rows (map #(reduce-kv (fn [row i x]
                                  (if-let [variable (index i)]
                                    (assoc row variable x)
                                    row)) {} %) datoms)
          relation (r/relation (vals index) rows)]
      (conj relations relation))))

(extend-protocol IFindElement
  yadat.dsl.FindVariable
  (resolve-find-element [{:keys [var]} sources row]
    (get row var))

  yadat.dsl.FindPull
  (resolve-find-element [{:keys [src var pattern]} sources row]
    (let [db (get sources src *default-source*)
          pattern (get sources pattern pattern)]
      (pull/resolve-pull-pattern pattern db (get row var))))

  yadat.dsl.FindAggregate
  (resolve-find-element [{:keys [args]} sources row]
    (some #(if (util/var? %) (get row %)) args)))

(extend-protocol IFindSpec
  yadat.dsl.FindScalar
  (resolve-find-spec [{:keys [element]} sources rows]
    (ffirst (resolve-tuples [element] sources rows)))

  yadat.dsl.FindTuple
  (resolve-find-spec [{:keys [elements]} sources rows]
    (first (resolve-tuples elements sources rows)))

  yadat.dsl.FindRelation
  (resolve-find-spec [{:keys [elements]} sources rows]
    (resolve-tuples elements sources rows))

  yadat.dsl.FindCollection
  (resolve-find-spec [{:keys [element]} sources rows]
    (let [tuples (resolve-tuples [element] sources rows)]
      (map first tuples))))

(extend-protocol IInput
  yadat.dsl.InputSource
  (resolve-input [{:keys [src]} value]
    {src value})

  yadat.dsl.InputScalar
  (resolve-input [{:keys [var]} value]
    (r/relation #{var} #{{var value}}))

  yadat.dsl.InputTuple
  (resolve-input [{:keys [vars]} tuple]
    (let [rows (apply hash-map (interleave vars tuple))]
      (r/relation (set vars) (set rows))))

  yadat.dsl.InputCollection
  (resolve-input [{:keys [var]} values]
    (r/relation #{var} (set (map (fn [v] {var v}) values))))

  yadat.dsl.InputRelation
  (resolve-input [{:keys [vars]} tuples]
    (r/relation (set vars)
                (set (map #(apply hash-map (interleave vars %)) tuples)))))

(extend-protocol IInputs
  yadat.dsl.Inputs
  (resolve-inputs [this values]
    (let [inputs (map resolve-input (:inputs this) values)
          predicate #(instance? yadat.relation.Relation %)
          {relations true individual-sources false} (group-by predicate inputs)
          var-sources (into {} (mapcat (fn [{:keys [columns rows]}]
                                         (map (fn [c] [c (get (first rows) c)])
                                              columns)) relations))
          sources (apply merge (concat individual-sources var-sources))]
      [sources relations])))

(defn resolve-query [form input-values]
  (let [query (dsl/parse-query form)
        variables (set (concat (dsl/vars (:find query))
                               (dsl/vars (:with query))))
        [sources relations] (resolve-inputs (:in query) input-values)]
    (binding [*default-source* (get sources '$)]
      (->> (resolve-clause (:where query) sources relations)
           (r/merge r/inner-join)
           (:rows)
           (map #(select-keys % variables))
           (set)
           (resolve-find-spec (:find query) sources)))))
