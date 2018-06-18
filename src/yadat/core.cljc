(ns yadat.core
  (:refer-clojure :exclude [spit slurp])
  (:require [yadat.db :as db]
            [yadat.db.minimal]
            [yadat.db.sorted-set]
            [yadat.util :as util]
            [yadat.query :as query]
            [yadat.relation :as r]
            [yadat.pull :as pull]))

(defn insert
  "Insert `entities` into `connection`. Modifies `connection`."
  [connection entities]
  (swap! connection (fn [db]
                      (let [[transaction _] (db/transact db entities)]
                        (:db transaction)))))

(defn open
  "Create a new db of `type` with `schema`. Returns a connection.
  Optionally inserts `entities` into db.
  A connection is just an atom containing a db.
  By default the `type`s :minimal & :sorted-set are supported. More types
  can be added by extending `db/open`."
  ([type schema]
   (atom (db/open type schema)))
  ([type schema entities]
   (let [db (atom (db/open type schema))]
     (insert db entities)
     db)))

(defn query
  "Queries `connection` for `query` map. Optionally takes further `inputs`.
  In cljs query map must be provided as an edn string."
  [connection query & inputs]
  (let [db @connection]
    (query/query db query)))

(defn q
  "Queries `connection` for `query` map. Optionally takes further `inputs`.
  In cljs query map must be provided as an edn string."
  [query connection & inputs]
  (let [db @connection]
    (query/query db query)))

(defn pull [db eid pattern]
  (pull/pull db eid pattern))

;; (defn slurp
;;   "Read db of type `t` from `f`.
;;   See `clojure.java.io/reader` for a list of supported values for `f`."
;;   [t f]
;;   (let [edn (clojure.core/slurp f)
;;         connection (atom (db/deserialize t edn))]
;;     connection))

(defn spit
  "Spit `db` into `f`.
  See `clojure.java.io/writer` for a list of supported values for `f`."
  [db f]
  (clojure.core/spit f (db/serialize @db)))

(defn db-with [connection datoms]
  (swap! connection
         (fn [db] (reduce (fn [db datom] (db/insert db datom)) db datoms))))
