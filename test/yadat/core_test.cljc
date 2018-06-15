(ns yadat.core-test
  (:require [yadat.core :as core]
            [clojure.test :refer :all]
            [yadat.test-helper :as test-helper]
            [yadat.db :as db]))

(let [db (core/open :minimal {})]
  (core/insert db [{:name "Cerberus" :heads 3}
                   {:name "Medusa" :heads 1}
                   {:name "Cyclops" :heads 1}
                   {:name "Chimera" :heads 1}])
  (core/query db '{:find [(sum ?heads)]
                   :with [?id]
                   :where [[?id :heads ?heads]
                           [?id :name ?name]]}))
