(ns yadat.pull
  (:require [yadat.db :as db]
            [yadat.dsl :as dsl]))

(def default-pull-limit 1000)

(defn datom->value [db f [e a v :as datom]]
  (cond
    (and (db/reverse-ref? a) f) (f e)
    (db/reverse-ref? a) {:db/id e}
    f (f v)
    (db/is? db a :component) (f (dsl/->PullWildcard) db {:db/id v})
    (db/is? db a :reference) {:db/id v}
    :else v))

(defn extend-entity
  [db entity a datoms options]
  (let [{:keys [resolve as default]} options
        v-or-nil (if (db/is? db a :many)
                   (not-empty (mapv #(datom->value db resolve %) datoms))
                   (datom->value db resolve (first datoms)))
        v (if (some? v-or-nil) v-or-nil default)
        k (or as a)]
    (assoc entity k v)))

(defn select-datoms [db entity a {:keys [limit] :as options}]
  (let [query-datom (if (db/reverse-ref? a)
                      [nil (db/reversed-ref a) (:db/id entity)]
                      [(:db/id entity) a nil])
        datoms (take (or limit default-pull-limit) (db/select db query-datom))]
    datoms))

(defprotocol IPullPattern
  (resolve-pull-pattern [this db eid]))

(defprotocol IPullElement
  (resolve-pull-element [this db entity]))

(extend-protocol IPullPattern
  yadat.dsl.PullPattern
  (resolve-pull-pattern [{:keys [elements]} db eid]
    (loop [entity {:db/id eid}
           [e & es] elements]
      (if (and (nil? e) (nil? es))
        entity
        (recur (resolve-pull-element e db entity) es)))))

(extend-protocol IPullElement
  yadat.dsl.PullWildcard
  (resolve-pull-element [_ db entity]
    (let [datoms (db/select db [(:db/id entity) nil nil])]
      (reduce (fn [entity [a datoms]]
                (extend-entity db entity a datoms nil))
              entity (group-by second datoms))))

  yadat.dsl.PullAttribute
  (resolve-pull-element [{:keys [a]} db entity]
    (let [datoms (select-datoms db entity a nil)]
      (extend-entity db entity a datoms nil)))

  ;; some of this stuff should be pulled out into the dsl
  ;; e.g. the [element pattern|recursion-limit part]
  ;; no idea how to go from here yet
  yadat.dsl.PullMap
  (resolve-pull-element [{:keys [m]} db entity]
    (let [[raw-spec pattern] (first (seq m))
          resolve (fn [eid] (resolve-pull-pattern pattern db eid))
          spec (if (keyword? raw-spec)
                 [raw-spec :resolve resolve]
                 (concat raw-spec [:resolve resolve]))]
      (resolve-pull-element spec db entity)))

  yadat.dsl.PullAttributeWithOptions
  (resolve-pull-element [{:keys [a options]} db entity]
    (let [datoms (select-datoms db entity a options)]
      (extend-entity db entity a datoms options))))

(defn pull [db eid pattern]
  (let [[_ eid] (cond
                  (db/lookup-ref? db eid) (db/resolve-lookup-ref-eid
                                           {:db db} eid)
                  (db/real-eid? db eid) [nil eid]
                  :else (throw (ex-info "Invalid eid" {:eid eid})))]
    (resolve-pull-pattern (dsl/pull-pattern pattern) db eid)))
