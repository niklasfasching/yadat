(ns yadat.db.sorted-set
  (:require [yadat.db :as db]
            [yadat.util :as util]
            [yadat.schema :as schema]))

(defn make-datom-comparator
  "Returns a comparator. Compares datom fields `i1`-`i3` in order using `cmp`.
  `cmp` is a comparator. `i1`-`i3` are indexes into the datom [e a v],
  i.e. values must be in #{0 1 2}."
  [cmp i1 i2 i3]
  (fn [datom1 datom2]
    (reduce (fn [result i]
              (if (not= result 0)
                (reduced result)
                (cmp (get datom1 i) (get datom2 i))))
            0 [i1 i2 i3])))

(defn select-datoms
  "Returns collection of sequential datoms matching `datom` from `index`."
  [index datom]
  (let [s #?(:clj (.seqFrom index datom true)
             :cljs (-sorted-seq-from index datom true))
        cmp (:select-compare (meta index))]
    (take-while (fn [next-datom] (= (cmp datom next-datom) 0)) s)))

(defn indexing? [db a]
  (or (db/is? db a :unique-identity)
      (db/is? db a :unique-value)
      (db/is? db a :reference)))

(defrecord SortedSetDb [eav aev ave schema eid]
  db/Db
  (new-eid [this]
    (let [{:keys [eid] :as db} (update-in this [:eid] inc)]
      [db eid]))

  (update-eid [this external-eid]
    (if (> external-eid eid)
      (assoc this :eid external-eid)
      this))

  (is? [this a x]
    (schema/is? schema a x))

  (delete [this datom]
    (if datom
      (assoc this
             :eav (disj eav datom)
             :ave (if (indexing? this (second datom)) (disj ave datom) ave)
             :aev (disj aev datom))
      this))

  (insert [this datom]
    (if datom
      (assoc this
             :eav (conj eav datom)
             :ave (if (indexing? this (second datom)) (conj ave datom) ave)
             :aev (conj aev datom))
      this))

  (select [this [e a v :as datom]]
    (cond
      (and e a) (select-datoms eav [e a v])
      (and a (some? v)
           (not (indexing? this a))) (->> (select-datoms eav [nil nil nil])
                                          (filter #(and (= a (get % 1))
                                                        (= v (get % 2)))))
      (and a (not (indexing? this a))) (->> (select-datoms eav [nil nil nil])
                                            (filter #(= a (get % 1))))
      a (->> (select-datoms ave [nil a v]))
      e (select-datoms eav [e nil nil])
      (some? v) (->> (select-datoms eav [nil nil nil])
                     (filter #(= v (get % 2))))
      ;; not throw?
      :else (select-datoms eav [nil nil nil])))

  (serialize [this]
    (binding [*print-length* nil
              *print-level* nil
              *print-namespace-maps* nil]
      (prn-str {:schema schema :eav eav :eid eid}))))

(defn sorted-datom-set
  "Returns a sorted-set (see `make-datom-comparator`).
  Inside the set datoms are sorted compared with the default `compare`.
  When selecting datoms from the set, nil values are
  treated as equal to anything. This allows selecting all datoms matching a
  partially, e.g. find all datoms with [1 :a nil] eid 1 & attribute :a."
  [i1 i2 i3]
  (let [ignore-nil-compare #(if (and (some? %1) (some? %2))
                              (compare %1 %2) 0)
        set (sorted-set-by (make-datom-comparator compare i1 i2 i3))
        select-compare (make-datom-comparator ignore-nil-compare i1 i2 i3)]
    (with-meta set {:select-compare select-compare})))

(defmethod db/open :sorted-set [_ schema]
  (->SortedSetDb (sorted-datom-set 0 1 2) ; eav
                 (sorted-datom-set 1 0 2) ; aev
                 (sorted-datom-set 1 2 0) ; ave
                 schema 0))

(defmethod db/deserialize :sorted-set [_ edn]
  (let [{:keys [eav schema eid]} (util/read-string {} edn)
        db (db/open :sorted-set schema)]
    (assoc db
           :eav (into (:eav db) eav)
           :aev (into (:aev db) eav)
           :ave (into (:ave db) (filter (fn [[_ a _]] (indexing? db a)) eav)))))
