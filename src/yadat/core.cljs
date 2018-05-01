(ns yadat.core
  (:require [yadat.db :as db]
            [yadat.db.minimal-db]
            [cljs.reader :as reader]
            [yadat.query :as query]))

(def db (-> (db/make-db :minimal {:comments [:many :reference]})
            (db/transact [{:name "Hans"
                           :email "hans@example.com"
                           :comments [{:text "comment 1"}
                                      {:text "comment 2"}]}])
            first :db))

;; yadat.core.execute_query('{:find [(pull ?id [:name])] :where [[?id :name "Hans"]]}')
;; IT WORKS WTF
(defn ^:export execute-query [query-string]
  (prn (query/q db (reader/read-string query-string))))
