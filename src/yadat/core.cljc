(ns yadat.core
  (:refer-clojure :exclude [spit slurp])
  (:require [yadat.db :as db]
            [yadat.db.minimal]
            [yadat.db.sorted-set]
            [yadat.util :as util]
            [yadat.pull :as pull]
            [yadat.query :as query]
            [yadat.schema :as schema]))

(defn transact
  "Insert `entities` to the db of  `connection`. Modifies `connection`."
  [connection entities]
  (swap! connection (fn [db] (:db-after (db/transact db entities)))))

(defn open
  "Create a new db of `type` with `schema`. Returns a connection.
  `schema` format is defined through :schema/type key of `schema`.
  Optionally inserts `entities` into db.
  A connection is just an atom containing a db.
  By default the `type`s :minimal & :sorted-set are supported. More types
  can be added by extending `db/open`."
  ([type schema]
   (atom (db/create type (schema/create schema))))
  ([type schema entities]
   (let [db (open type schema)]
     (transact db entities)
     db)))

(defn q
  "Queries `connection` for `query` map. Optionally takes further `inputs`.
  In cljs query map must be provided as an edn string."
  [query & inputs]
  (query/resolve-query query inputs))

(defn pull [db eid pattern]
  (pull/pull db eid pattern))

(defn slurp
  "Read db of type `t` from `uri`. Returns a promise for a db connection.
  For more information on supported values for uri see
  - `clojure.java.io/reader` in clojure
  - `js/fetch` in clojurescript
  (the returned value is a js-promise in cljs, future in clj)."
  [uri]
  #?(:cljs (-> (js/fetch uri)
               (.then (fn [response] (.text response)))
               (.then (fn [text] (atom (util/read-string text)))))
     :clj (future (atom (util/read-string (clojure.core/slurp uri))))))

(defn spit
  "Spit db of `connection` to `uri`. Returns a promise.
  For more information on supported values for uri see
  - `clojure.java.io/writer` in clojure
  - `js/fetch` in clojurescript
  (the returned value is a js-promise in cljs, future in clj)."
  [connection uri]
  #?(:cljs (js/fetch uri #js {:method "POST" :body (db/serialize @connection)})
     :clj (future (clojure.core/spit uri (db/serialize @connection)))))
