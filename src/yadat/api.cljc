(ns yadat.api
  (:refer-clojure :exclude [spit slurp])
  (:require [yadat.db :as db]
            [yadat.db.minimal-db]
            [yadat.db.sorted-set-db]
            [yadat.query :as query]
            [yadat.util :as util]))

(defn open
  "Create a new db of `type` with `schema`. Returns a connection.
  A connection is just an atom containing a db.
  By default the `type`s :minimal & :sorted-set-db are supported. More types
  can be added by extending `db/open`."
  [type schema]
  (atom (db/open type schema)))

(defn insert
  "Insert `entities` into `connection`. Modifies `connection`."
  [connection entities]
  (swap! connection (fn [db]
                      (let [[transaction _] (db/transact db entities)]
                        (:db transaction)))))

(defn query
  "Queries `connection` for `query` map.
  In cljs query map must be provided as an edn string."
  [connection query]
  (query/q @connection query))

(defn slurp [t f]
  (let [edn (clojure.core/slurp f)
        connection (atom (db/deserialize t edn))]
    connection))

(defn spit [db f]
  (clojure.core/spit f (db/serialize @db)))
