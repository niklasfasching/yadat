(ns yadat.api
  (:require #?(:cljs [cljs.reader])
            [yadat.db :as db]
            [yadat.db.minimal-db]
            [yadat.db.sorted-set-db]
            [yadat.query :as query]
            [yadat.util :as util]))

(defn ^:export open
  "Create a new db of `type` with `schema`. Returns a connection.
  A connection is just an atom containing a db.
  By default the `type`s :minimal & :sorted-set-db are supported. More types
  can be added by extending `db/make-db`."
  [type schema]
  (atom (db/make-db type (util/->clj schema))))

(defn ^:export insert
  "Insert `entities` into `connection`. Modifies `connection`."
  [connection entities]
  (swap! connection (fn [db]
                      (let [entities (util/->clj entities)
                            [transaction _] (db/transact db entities)]
                        (:db transaction)))))

(defn ^:export query
  "Queries `connection` for `query` map.
  In cljs query map must be provided as an edn string."
  [connection query]
  (let [query #?(:clj query
                 :cljs (cljs.reader/read-string query))
        results (query/q @connection query)]
    #?(:clj results
       :cljs (clj->js results))))
