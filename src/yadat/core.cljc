(ns yadat.core
  (:refer-clojure :exclude [spit slurp])
  (:require [yadat.db :as db]
            [yadat.db.minimal]
            [yadat.db.sorted-set]
            [yadat.util :as util]
            [yadat.query :as query]
            [yadat.pull :as pull]
            [yadat.schema :as schema]))

(defn insert
  "Insert `entities` into `connection`. Modifies `connection`."
  [connection entities]
  (swap! connection (fn [db]
                      (let [[transaction _] (db/transact db entities)]
                        (:db transaction)))))

(defn open
  "Create a new db of `type` with `schema`. Returns a connection.
  `schema` format is defined through :schema/type key of `schema`.
  Optionally inserts `entities` into db.
  A connection is just an atom containing a db.
  By default the `type`s :minimal & :sorted-set are supported. More types
  can be added by extending `db/open`."
  ([type schema]
   (atom (db/open type (schema/create schema))))
  ([type schema entities]
   (let [db (atom (db/open type schema))]
     (insert db entities)
     db)))

(defn q
  "Queries `connection` for `query` map. Optionally takes further `inputs`.
  In cljs query map must be provided as an edn string."
  [query & inputs]
  (query/resolve-query query inputs))

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
