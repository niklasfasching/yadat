(ns yadat.pull
  (:require [yadat.db :as db]
            [yadat.dsl :as dsl]
            [yadat.schema :as schema]))

(def default-pull-limit 1000)

(def ^:dynamic *pulled-eids*
  "The pull API allows recursiion. To prevent cycles we need to track which
  entities have already been pulled. Attempts to pull an already pulled entity
  will return only the :db/id and ignore the applied pattern."
  #{})

(def wildcard-pattern (dsl/->PullPattern [(dsl/->PullWildcard)]))

(defprotocol IPullPattern
  (resolve-pull-pattern [this sources eid]))

(defprotocol IPullElement
  (resolve-pull-element [this sources entity]))

(defn datom->value
  [sources {:keys [e a v] :as datom}]
  (let [db (get sources '$)
        x (if (schema/reverse-ref? a) e v)]
    (cond
      (nil? datom) nil
      (db/is? db a :component) (resolve-pull-pattern wildcard-pattern sources x)
      (db/is? db a :reference) {:db/id x}
      :else x)))

(defn extend-entity [sources entity a datoms options resolve-datom]
  (let [db (get sources '$)
        fwd-a (schema/forward-ref a)
        v-or-nil (if (or (and (not= a fwd-a) (db/is? db fwd-a :component))
                         (not (db/is? db fwd-a :many)))
                   (when-let [datom (first datoms)]
                     (resolve-datom sources datom))
                   (not-empty (mapv #(resolve-datom sources %) datoms)))
        v (if (some? v-or-nil) v-or-nil (:default options))
        k (get options :as a)]
    (if (some? v)
      (assoc entity k v)
      entity)))

(defn select-datoms
  [sources {:keys [db/id] :as entity} a options]
  (let [db (get sources '$)
        limit (get options :limit default-pull-limit)
        datom (cond
                (nil? a) (db/datom id nil nil)
                (schema/reverse-ref? a) (db/datom nil (schema/forward-ref a) id)
                :else (db/datom id a nil))]
    (if (some? limit)
      (take limit (db/select db datom))
      (db/select db datom))))

(extend-protocol IPullPattern
  yadat.dsl.PullPattern
  (resolve-pull-pattern [{:keys [elements]} sources eid]
    (if (contains? *pulled-eids* eid)
      {:db/id eid}
      (binding [*pulled-eids* (conj *pulled-eids* eid)]
        (reduce
         (fn [entity element] (resolve-pull-element element sources entity))
         {:db/id eid} elements)))))

(extend-protocol IPullElement
  yadat.dsl.PullWildcard
  (resolve-pull-element [_ sources entity]
    (let [datoms (select-datoms sources entity nil nil)]
      (reduce (fn [entity [a datoms]]
                (extend-entity sources entity a datoms nil datom->value))
              entity (group-by #(.-a ^yadat.db.Datom %) datoms))))

  yadat.dsl.PullAttribute
  (resolve-pull-element [{:keys [a options]} sources entity]
    (let [eid (:db/id entity)
          db (get sources '$)
          datoms (select-datoms sources entity a options)
          resolve-datom (if (schema/reverse-ref? a)
                          (fn [_ datom] {:db/id (.-e ^yadat.db.Datom datom)})
                          datom->value)]
      (extend-entity sources entity a datoms options resolve-datom)))

  yadat.dsl.PullMap
  (resolve-pull-element [{:keys [a options pattern]} sources entity]
    (let [eid (:db/id entity)
          db (get sources '$)
          pattern (dsl/parse-pull-pattern (get sources pattern pattern))
          datoms (select-datoms sources entity a options)
          resolve-datom (fn [_ datom]
                          (resolve-pull-pattern
                           pattern sources (.-e ^yadat.db.Datom datom)))]
      (extend-entity sources entity a datoms options resolve-datom))))


(defn pull [db eid pattern]
  (let [[_ eid] (cond
                  (db/lookup-ref? db eid) (db/resolve-lookup-ref-eid
                                           {:db db} eid)
                  (db/real-eid? db eid) [nil eid]
                  :else (throw (ex-info "Invalid eid" {:eid eid})))]
    (resolve-pull-pattern (dsl/parse-pull-pattern pattern) {'$ db} eid)))
