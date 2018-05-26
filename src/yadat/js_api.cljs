(ns yadat.js-api
  (:require [cljs.reader :as reader]
            [clojure.walk :as walk]
            [yadat.api :as api]
            [yadat.db :as db]
            [yadat.db.minimal-db]
            [yadat.db.sorted-set-db]
            [yadat.query :as query]
            [yadat.util :as util]))

(defn js->keywordized-clj [x]
  (walk/postwalk walk/keywordize-keys (js->clj x)))

(defn ^:export open [type schema]
  (api/open (keyword type) (js->keywordized-clj schema)))

(defn ^:export insert [connection entities]
  (api/insert connection (js->keywordized-clj entities)))

(defn ^:export query [connection query-string]
  (let [query (reader/read-string query-string)]
    (clj->js (query/q @connection query))))
