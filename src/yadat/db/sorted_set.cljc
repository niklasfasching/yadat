(ns yadat.db.sorted-set
  (:require [yadat.db :as db]
            [yadat.schema :as schema]))

(defn select-datoms
  "Returns collection of sequential datoms matching `datom` from `index`."
  [^clojure.lang.PersistentTreeSet index datom]
  (let [s #?(:clj (.seqFrom index datom true)
             :cljs (-sorted-seq-from index datom true))
        cmp (:select-compare (meta index))]
    (take-while (fn [next-datom] (= (cmp datom next-datom) 0)) s)))

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
             :ave (if (db/is? this (.-a ^yadat.db.Datom datom) :index)
                    (disj ave datom)
                    ave)
             :aev (disj aev datom))
      this))

  (insert [this datom]
    (if datom
      (assoc this
             :eav (conj eav datom)
             :ave (if (db/is? this (.-a ^yadat.db.Datom datom) :index)
                    (conj ave datom)
                    ave)
             :aev (conj aev datom))
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
    (binding [*print-length* nil
              *print-level* nil
              *print-namespace-maps* nil]
      ;; nil secondary indices to reduce size
      (prn-str (assoc this :aev nil :ave nil)))))

(defmacro sorted-datom-set
  "Returns a sorted-set (see `db/datom-comparator`).
  Inside the set datoms are sorted compared with the default `compare`.
  When selecting datoms from the set, nil values are treated as equal to
  anything. This allows selecting all datoms matching a partially, e.g. find
  all datoms with eid 1 & attribute :a via (db/datom 1 :a nil)."
  [f1 f2 f3]
  `(let [
         set# (sorted-set-by (db/datom-comparator compare ~f1 ~f2 ~f3))
         select-compare# (db/datom-comparator nil-compare# ~f1 ~f2 ~f3)]
     (with-meta set# {:select-compare select-compare#})))

(defn nil-compare [x y]
  (if (and (some? x) (some? y))
    (compare x y)
    0))

(defn index [cmp select-cmp]
  (with-meta (sorted-set-by cmp) {:select-compare select-cmp}))

(defmethod db/create :sorted-set [_ schema]
  (->SortedSetDb (index (db/datom-comparator compare e a v)
                        (db/datom-comparator nil-compare e a v))
                 (index (db/datom-comparator compare a e v)
                        (db/datom-comparator nil-compare a e v))
                 (index (db/datom-comparator compare a v e)
                        (db/datom-comparator nil-compare a v e))
                 schema
                 0))

(defn deserialize [{:keys [eid schema eav] :as m}]
  (let [db (db/create :sorted-set schema)
        eav (into (:eav db) eav)
        aev (into (:aev db) eav)
        ave (into (:ave db) (filter #(db/is? db (.-a ^yadat.db.Datom %) :index)
                                    eav))]
    (assoc db :eid eid :eav eav :aev aev :ave ave)))
