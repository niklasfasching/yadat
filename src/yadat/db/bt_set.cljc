(ns yadat.db.bt-set
  (:require [datascript.btset :as btset]
            [yadat.db :as db]
            [yadat.schema :as schema]))

(defn nil-compare [x y]
  (if (and (some? x) (some? y))
    (compare x y)
    0))

(def cmp-eav (db/datom-comparator nil-compare e a v))
(def cmp-aev (db/datom-comparator nil-compare a e v))
(def cmp-ave (db/datom-comparator nil-compare a v e))

(defn select-datoms
  "Returns collection of sequential datoms matching `datom` from `index`."
  [index datom]
  (btset/slice index datom))

(defrecord BtSetDb [eav aev ave schema eid]
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
             :eav (btset/btset-disj eav datom cmp-eav)
             :ave (if (db/is? this (.-a ^yadat.db.Datom datom) :index)
                    (btset/btset-disj ave datom cmp-ave)
                    ave)
             :aev (btset/btset-disj aev datom cmp-aev))
      this))

  (insert [this datom]
    (if datom
      (assoc this
             :eav (btset/btset-conj eav datom cmp-eav)
             :ave (if (db/is? this (.-a ^yadat.db.Datom datom) :index)
                    (btset/btset-conj ave datom cmp-ave)
                    ave)
             :aev (btset/btset-conj aev datom cmp-aev))
      this))

  (select [this {:keys [e a v] :as datom}]
    (cond
      (and e a)
      (select-datoms eav (db/datom e a v))

      (and a (some? v) (not (db/is? this a :index)))
      (->> (select-datoms eav (db/datom nil nil nil))
           (filter #(and (= a (.-a ^yadat.db.Datom %))
                         (= v (.-v ^yadat.db.Datom %)))))

      (and a (not (db/is? this a :index)))
      (->> (select-datoms eav (db/datom nil nil nil))
           (filter #(= a (.-a ^yadat.db.Datom %))))

      a
      (->> (select-datoms ave (db/datom nil a v)))

      e
      (select-datoms eav (db/datom e nil nil))

      (some? v)
      (->> (select-datoms eav (db/datom nil nil nil))
           (filter #(= v (.-v ^yadat.db.Datom %))))

      :else (select-datoms eav (db/datom nil nil nil))))

  (serialize [this]
    nil))


(defmethod db/create :bt-set [_ schema]
  (let [nil-compare #(if (and (some? %1) (some? %2))
                       (compare %1 %2) 0)]
    (->BtSetDb
     (btset/btset-by cmp-eav)
     (btset/btset-by cmp-aev)
     (btset/btset-by cmp-ave)
     schema
     0)))
