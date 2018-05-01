(ns yadat.db.sorted-set-db
  (:require [yadat.db :as db]))

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
  (let [cmp (:select-compare (meta index))]
    (take-while (fn [next-datom] (= (cmp datom next-datom) 0))
                (.seqFrom index datom true))))

(defrecord SortedSetDb [eav aev ave schema eid]
  db/Db
  (new-eid [this]
    (let [{:keys [eid] :as db} (update-in this [:eid] inc)]
      [db eid]))
  (update-eid [this external-eid]
    (if (> external-eid eid)
      (assoc this :eid external-eid)
      this))
  (is? [this raw-a x]
    (let [a (if (db/reverse-ref? raw-a) (db/reversed-ref raw-a) raw-a)]
      (case x
        :many (= :db.cardinality/many (:db/cardinality (a schema)))
        :unique-identity (= :db.unique/identity (:db/unique (a schema)))
        :unique-value (= :db.unique/value (:db/unique (a schema)))
        :reference (= :db.type/ref (:db/type (a schema)))
        :component (:db/isComponent (a schema))
        (throw (ex-info "Invalid value" {:a raw-a :x x})))))
  (delete [this datom]
    (assoc this
           :eav (disj eav datom)
           :ave (disj ave datom)
           :aev (disj aev datom)))
  (insert [this [e a v :as datom]]
    (assoc this
           :eav (conj eav datom)
           :ave (conj ave datom)
           :aev (conj aev datom)))
  (select [this [e a v :as datom]]
    (cond
      (and e a (some? v)) (select-datoms eav [e a v])
      (and e a) (select-datoms eav [e a nil])
      (and a (some? v)) (->> (select-datoms ave [nil a nil])
                             (filter #(= v (get % 2))))
      a (select-datoms ave [nil a nil])
      e (select-datoms eav [e nil nil])
      (some? v) (->> (select-datoms eav [nil nil nil])
                     (filter #(= v (get % 2))))
      :else (throw (ex-info "Invalid select" {:datom datom})))))

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

(defmethod db/make-db :sorted-set [_ schema]
  (->SortedSetDb (sorted-datom-set 0 1 2) ; eav
                 (sorted-datom-set 1 0 2) ; aev
                 (sorted-datom-set 1 2 0) ; ave
                 schema 0))
