(ns yadat.api-test
  (:require [yadat.api :as api]
            [clojure.test :refer :all]
            [yadat.test-helper :as test-helper]
            [yadat.db :as db]))

(let [db (api/open :minimal {})]
  (api/insert db [{:name "Cerberus" :heads 3}
                  {:name "Cerberus" :heads 2}
                  {:name "Medusa" :heads 1}
                  {:name "Cyclops" :heads 1}
                  {:name "Chimera" :heads 1}])
  (api/query db '{:find [?name (sum ?heads)]
                  :with [?id]
                  :where [[?id :heads ?heads]
                          [?id :name ?name]]}))
