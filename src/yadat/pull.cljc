(ns yadat.pull
  (:require [yadat.db :as db]))

(def default-limit 1000)

(declare resolve-pull)

(defn attribute-spec-type [spec]
  (cond
    (= spec '*) :wildcard
    (keyword? spec) :attribute
    (map? spec) :map
    (and (sequential? spec) (keyword? (first spec))) :attribute-with-options
    (and (list? spec) (symbol? (first spec))) :attribute-expression
    :else (throw (ex-info "Invalid attribute-spec" {:spec spec}))))

(defn select-datoms [db entity a {:keys [limit] :as options}]
  (let [query-datom (if (db/reverse-ref? a)
                      [nil (db/reversed-ref a) (:db/id entity)]
                      [(:db/id entity) a nil])
        datoms (take (or limit default-limit) (db/select db query-datom))]
    datoms))

(defn extend-entity
  [db entity a datoms options]
  (let [{:keys [resolve as default]} options
        vs (mapv (fn [[e _ v :as datom]]
                   (cond
                     (and (db/reverse-ref? a) resolve) (resolve e)
                     (db/reverse-ref? a) {:db/id e}
                     resolve (resolve v)
                     (db/is? db a :component) (resolve-pull v '[*])
                     (db/is? db a :reference) {:db/id v}
                     :else v)) datoms)
        v-or-nil (if (db/is? db a :many) (not-empty vs) (first vs))
        v (if (some? v-or-nil) v-or-nil default)
        k (or as a)]
    (assoc entity k v)))

(defmulti resolve-attribute-spec
  (fn [db entity spec] (attribute-spec-type spec)))

(defmethod resolve-attribute-spec :wildcard [db entity _]
  (let [datoms (db/select db [(:db/id entity) nil nil])]
    (reduce (fn [entity [a datoms]]
              (extend-entity db entity a datoms nil))
            entity (group-by second datoms))))

(defmethod resolve-attribute-spec :attribute [db entity a]
  (let [datoms (select-datoms db entity a nil)]
    (extend-entity db entity a datoms nil)))

(defmethod resolve-attribute-spec :attribute-with-options [db entity spec]
  (let [[a & raw-options] spec
        options (apply hash-map raw-options)
        datoms (select-datoms db entity a options)]
    (extend-entity db entity a datoms options)))

(defmethod resolve-attribute-spec :map [db entity spec]
  (let [[raw-spec pattern] (first (seq spec))
        resolve (fn [eid] (resolve-pull db eid pattern))
        spec (if (keyword? raw-spec)
               [raw-spec :resolve resolve]
               (concat raw-spec [:resolve resolve]))]
    (resolve-attribute-spec db entity spec)))

(defn resolve-pull [db eid pattern]
  (loop [entity {:db/id eid}
         [x & xs] pattern]
    (if (and (nil? x) (nil? xs))
      entity
      (recur (resolve-attribute-spec db entity x) xs))))

(defn pull [db eid pattern]
  (let [[_ eid] (cond
                  (db/lookup-ref? db eid) (db/resolve-lookup-ref-eid
                                           {:db db} eid)
                  (db/real-eid? db eid) [nil eid]
                  :else (throw (ex-info "Invalid eid" {:eid eid})))]
    (resolve-pull db eid pattern)))
