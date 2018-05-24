(ns yadat.pull
  (:require [yadat.db :as db]))

(declare resolve-pull)

(defn datoms->av [db a datoms]
  (let [vs (map (fn [[e a v]]
                  (cond
                    (db/is? db a :component) (resolve-pull v '[*])
                    (db/is? db a :reference) {:db/id v}
                    :else v)) datoms)]
    (if (db/is? db a :many)
      [a (set vs)]
      [a (first vs)])))

(defn attribute-spec-type [spec]
  (cond
    (= spec '*) :wildcard
    (keyword? spec) :attribute
    (map? spec) :map
    (and (vector? spec) (keyword? (first spec))) :attribute-with-options
    (and (list? spec) (keyword? (first spec))) :attribute-expression
    :else (throw (ex-info "Invalid attribute-spec" {:spec spec}))))

(defmulti resolve-attribute-spec
  (fn [db entity spec] (attribute-spec-type spec)))

(defmethod resolve-attribute-spec :wildcard [db entity _]
  (let [datoms (db/select db [(:db/id entity) nil nil])
        datoms-by-attribute (group-by second datoms)
        avs (map (fn [[a datoms]] (datoms->av db a datoms))
                 datoms-by-attribute)]
    (into entity avs)))

(defmethod resolve-attribute-spec :attribute [db entity attribute]
  (let [query-datom (if (db/reverse-ref? attribute)
                      [nil attribute (:db/id entity)]
                      [(:db/id entity) attribute nil])
        datoms (db/select db query-datom)]
    (into entity [(datoms->av db attribute datoms)])))

;; (defmethod resolve-attribute-spec :map [db entity spec]
;;   ;; map means sub-pull (?) - must take care of cardinality (many? in array, not replace)
;;   ;; assuming it's a ref
;;   (let [[a pattern] (first (seq x))
;;         [_ _ eid] (first (filter #(= (nth % 1) a) datoms))
;;         v (if eid (resolve-pull db eid pattern))
;;         entity (if (empty? v) entity (assoc entity a v))]
;;     (recur entity xs)))

(defn resolve-pull [db eid pattern]
  (loop [entity {:db/id eid}
         [x & xs] pattern]
    (if (and (nil? x) (nil? xs))
      entity
      (recur (resolve-attribute-spec db entity x) xs))))



;; (defn pull [db eid pattern]
;;   ;; lookup-refs and stuff in the pull api outside query
;;   (let [[_ eid] (db/resolve-eid {:db db :temp-eids {}} eid)]))
