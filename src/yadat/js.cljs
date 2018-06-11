(ns yadat.js
  (:require [cljs.reader :as reader]
            [clojure.walk :as walk]
            [yadat.core :as core]
            [yadat.db :as db]
            [yadat.db.minimal]
            [yadat.db.sorted-set]
            [yadat.query :as query]
            [yadat.util :as util]))

(defn keywordize-kvs [m]
  (let [f (fn [[k v]] (if (string? v)
                        [(keyword k) (keyword v)]
                        [(keyword k) v]))]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn ^:export open [type schema]
  (let [schema (into {} (map (fn [[a ts]] [(keyword a) (map keyword ts)])
                             (js->clj schema)))]
    (core/open (keyword type) schema)))

(defn ^:export insert [connection entities]
  (let [entities (walk/postwalk walk/keywordize-keys (js->clj entities))]
    (core/insert connection entities)))

(defn ^:export query [connection query-string]
  (let [query (reader/read-string query-string)]
    (clj->js (query/q @connection query))))

(defn ^:export slurp [t f]
  (-> (js/fetch f)
      (.then (fn [response] (.text response)))
      (.then (fn [edn] (atom (db/deserialize (keyword t) edn))))))
