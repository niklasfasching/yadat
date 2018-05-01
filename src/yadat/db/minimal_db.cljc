(ns yadat.db.minimal-db
  (:require [yadat.db :as db]))

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
    (let [a (if (db/reverse-ref? a) (db/reversed-ref a) a)]
      (boolean (some #{x} (a schema)))))
  (delete [this datom]
    (update-in this [:set] disj datom))
  (insert [this [e a v :as datom]]
    (update-in this [:set] conj datom))
  (select [this datom]
    (filter #(reduce (fn [included? [x y]]
                       (cond
                         (not included?) (reduced included?)
                         (and (some? x) (some? y)) (= x y)
                         :else true))
                     true (map vector datom %)) set)))

(defmethod db/make-db :minimal [_ schema]
  (->MinimalDb #{} schema 0))
