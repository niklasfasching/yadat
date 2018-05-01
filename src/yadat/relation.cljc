(ns yadat.relation
  (:require [clojure.set :as set]))

(defrecord Relation [id columns rows])

(defn relation [columns rows]
  (->Relation (gensym "relation-") (set columns) (set rows)))

(defn intersect?
  [{cs1 :columns rs1 :rows :as r1} {cs2 :columns rs2 :rows :as r2}]
  (-> (set/intersection cs1 cs2) not-empty boolean))

(defn cross-join
  "Cross joins (cartesian product) relations `r1` & `r2`. Returns a relation."
  [{cs1 :columns rs1 :rows :as r1} {cs2 :columns rs2 :rows :as r2}]
  (let [columns (set/union cs1 cs2)
        rows (for [b1 rs1 b2 rs2] (merge b1 b2))]
    (relation columns rows)))

(defn inner-join
  "Inner join relations `r1` & `r2` on shared columns. Returns a relation."
  [{cs1 :columns rs1 :rows :as r1} {cs2 :columns rs2 :rows :as r2}]
  (relation (set/union cs1 cs2) (set/join rs1 rs2)))

(defn disjoin
  "Subtract relation `r2` from `r1`. Returns a relation.
  Removes any rows from relation `r1` that (at least partially) match
  any rows in `r2`. `clojure.set/difference` for relations."
  [{cs1 :columns rs1 :rows :as r1} {cs2 :columns rs2 :rows :as r2}]
  (let [columns (set/union cs1 cs2)
        rows (set/difference rs1 (set/join rs1 rs2))]
    (relation columns rows)))

(defn union
  "Union relations `r1` & `r2`. Returns relation."
  [{cs1 :columns rs1 :rows :as r1} {cs2 :columns rs2 :rows :as r2}]
  (relation (set/union cs1 cs2) (set/union rs1 rs2)))

(defn merge
  "Combines `relations` into a single relation.
  Relations with shared columns are merged with (`f` r1 r2). Unrelated
  relations are merged with `cross-join-relations`."
  [relations f]
  (loop [[r1 r2 & rs] relations
         out-rs []]
    (cond
      (and (nil? r1) (nil? r2) (nil? rs)) (if-not (empty? out-rs)
                                            (reduce cross-join out-rs))
      (intersect? r1 r2) (recur (concat [(f r1 r2)] out-rs rs) [])
      :else (recur rs (concat out-rs (remove nil? [r1 r2]))))))

(defn split
  "Splits `relations` on `columns`. Returns [relations relation].
  relation is the `inner-join` of all relations intersecting `columns`,
  relations are all remaining relations."
  [relations columns]
  (let [groups (group-by #(intersect? (relation columns nil) %) relations)
        {intersecting-relations true relations false} groups
        intersecting-relation (merge intersecting-relations inner-join)]
    [relations intersecting-relation]))
