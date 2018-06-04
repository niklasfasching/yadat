(ns yadat.pull
  (:require [clojure.core.match :refer [match]]
            [yadat.db :as db]))

(def default-limit 1000)

(defprotocol PullElement
  (resolve-pull-element [_ db entity]))

(defn extend-entity
  [db entity a datoms options]
  (let [{:keys [resolve as default]} options
        vs (mapv (fn [[e _ v :as datom]]
                   (cond
                     (and (db/reverse-ref? a) resolve) (resolve e)
                     (db/reverse-ref? a) {:db/id e}
                     resolve (resolve v)
                     (db/is? db a :component) (resolve
                                               (->PullWildcard) db {:db/id v})
                     (db/is? db a :reference) {:db/id v}
                     :else v)) datoms)
        v-or-nil (if (db/is? db a :many) (not-empty vs) (first vs))
        v (if (some? v-or-nil) v-or-nil default)
        k (or as a)]
    (assoc entity k v)))

(defn select-datoms [db entity a {:keys [limit] :as options}]
  (let [query-datom (if (db/reverse-ref? a)
                      [nil (db/reversed-ref a) (:db/id entity)]
                      [(:db/id entity) a nil])
        datoms (take (or limit default-limit) (db/select db query-datom))]
    datoms))

(defn resolve-pull-pattern [db eid pattern]
  (loop [entity {:db/id eid}
         [x & xs] pattern]
    (if (and (nil? x) (nil? xs))
      entity
      (recur (resolve-pull-element x db entity) xs))))

(defn pull [db eid pattern]
  (let [[_ eid] (cond
                  (db/lookup-ref? db eid) (db/resolve-lookup-ref-eid
                                           {:db db} eid)
                  (db/real-eid? db eid) [nil eid]
                  :else (throw (ex-info "Invalid eid" {:eid eid})))]
    (resolve-pull-pattern db eid pattern)))

(defrecord PullWildcard []
  PullElement
  (resolve-pull-element [_ db entity]
    (let [datoms (db/select db [(:db/id entity) nil nil])]
      (reduce (fn [entity [a datoms]]
                (extend-entity db entity a datoms nil))
              entity (group-by second datoms)))))

(defrecord PullAttribute [a]
  PullElement
  (resolve-pull-element [_ db entity]
    (let [datoms (select-datoms db entity a nil)]
      (extend-entity db entity a datoms nil))))

(defrecord PullMap [m]
  PullElement
  (resolve-pull-element [_ db entity]
    (let [[raw-spec pattern] (first (seq m))
          resolve (fn [eid] (resolve-pull-pattern pattern db eid)) ;; TODO
          spec (if (keyword? raw-spec)
                 [raw-spec :resolve resolve]
                 (concat raw-spec [:resolve resolve]))]
      (resolve-pull-element db entity spec))))

(defrecord PullAttributeWithOptions [a options]
  PullElement
  (resolve-pull-element [_ db entity]
    (let [datoms (select-datoms db entity a options)]
      (extend-entity db entity a datoms options))))

;; (defrecord PullAttributeExpression [a options])

(defn ->PullElement [element]
  (match [element]
    ['*] (->PullWildcard)
    [(a :guard keyword?)] (->PullAttribute a)
    [(m :guard map?)] (->PullMap m) ;; actually resolve it further into a sub-pull-pattern
    [([(a :guard keyword?) & options] :seq)] (->PullAttributeWithOptions
                                              a (apply hash-map options))
    :else (throw (ex-info "Invalid pull element" {:element element}))))
