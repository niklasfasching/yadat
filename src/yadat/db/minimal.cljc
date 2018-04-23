(ns yadat.db.minimal
  (:require [yadat.db :as db]
            [yadat.util :as util]
            [yadat.schema :as schema]))

(defn cmp [^yadat.db.Datom d1 ^yadat.db.Datom d2]
  (let [nil= #(if (and (some? %1) (some? %2))
                (= %1 %2)
                true)]
    (if-let [r1 (nil= (.-e d1) (.-e d2))]
      (if-let [r2 (nil= (.-a d1) (.-a d2))]
        (if-let [r3 (nil= (.-v d1) (.-v d2))]
          true false) false) false)))

(defrecord MinimalDb [set schema eid]
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
    (update-in this [:set] disj datom))

  (insert [this datom]
    (update-in this [:set] conj datom))

  (select [this datom]
    (filter #(cmp datom %) set))

  (serialize [this]
    (binding [*print-length* nil
              *print-level* nil
              *print-namespace-maps* nil]
      (prn-str this))))


(defmethod db/create :minimal [_ schema]
  (->MinimalDb #{} schema 0))
