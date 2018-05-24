(ns yadat.api
  (:require [yadat.db :as db]
            [yadat.db.minimal-db]
            [yadat.db.sorted-set-db]
            [yadat.query :as query]))

(def ^:export make-db db/make-db)

(def ^:export q query/q)

(def ^:export transact db/transact)
