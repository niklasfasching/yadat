(ns yadat.pull
  (:require [yadat.db :as db]))

(declare resolve-pull)

(def default-limit 1000)

(def ^:dynamic *transform* identity)

(defn select-datoms [db entity a]
  (let [query-datom [(:db/id entity) a nil]]
    (db/select db query-datom)))

(defn datoms->av [db a datoms]
  (let [vs (mapv (fn [[e a v]]
                   (cond
                     (db/is? db a :component) (resolve-pull v '[*])
                     (db/is? db a :reference) {:db/id v}
                     :else v)) datoms)]
    (if (db/is? db a :many)
      [a vs]
      [a (first vs)])))

(defn attribute-spec-type [spec]
  (cond
    (= spec '*) :wildcard
    (keyword? spec) :attribute
    (map? spec) :map
    (and (vector? spec) (keyword? (first spec))) :attribute-with-options
    (and (list? spec) (symbol? (first spec))) :attribute-expression ;; remove for complexity?
    :else (throw (ex-info "Invalid attribute-spec" {:spec spec}))))

(defmulti resolve-attribute-spec
  (fn [db entity spec] (attribute-spec-type spec)))

(defmethod resolve-attribute-spec :wildcard [db entity _]
  (let [datoms (select-datoms db entity nil)
        datoms-by-attribute (group-by second datoms)
        avs (map (fn [[a datoms]] (datoms->av db a datoms))
                 datoms-by-attribute)]
    (into entity avs)))

(defmethod resolve-attribute-spec :attribute [db entity attribute]
  (let [datoms (select-datoms db entity attribute)]
    (into entity [(datoms->av db attribute datoms)])))


(defmethod resolve-attribute-spec :attribute-with-options [db entity attribute]
  )

;; reverse ref: module/_major -> all modules that have the same major
;; pulls all modules for a major... why not use straight?

(defmethod resolve-attribute-spec :map [db entity spec]
  (let [[attribute-spec pattern] (first (seq spec))]
    ;; bind transform to pull
    (resolve-attribute-spec db entity attribute-spec)))

;; :limit & :as can be applied in select
;; :default has to be applied as part of transformation,
;; i.e either x or (t x) or default!

(defn resolve-pull [db eid pattern]
  (loop [entity {:db/id eid}
         [x & xs] pattern]
    (if (and (nil? x) (nil? xs))
      entity
      (recur (resolve-attribute-spec db entity x) xs))))

;; (defn pull [db eid pattern]
;;   ;; lookup-refs and stuff in the pull api outside query
;;   (let [[_ eid] (db/resolve-eid {:db db :temp-eids {}} eid)]))

;; as :map is the outer fn cannot use *limit* *default* *as* from :a-options
;; rather, bind a *transform* (defaults to identity) and handle each value that way
;; must be able to override :component
;; limit must have a default

;; *limit* and *default* should still be dynamic to allow deferring from :attribute-with-options to :attribuetc
