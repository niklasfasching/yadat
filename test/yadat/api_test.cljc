(ns yadat.api-test
  (:require [yadat.api :as api]
            [clojure.test :refer :all]
            [yadat.test-helper :as test-helper]
            [yadat.db :as db]))

(let [db (api/open :minimal {})]
  (api/insert db [{:name "Cerberus" :heads 3}
                  {:name "Medusa" :heads 1}
                  {:name "Cyclops" :heads 1}
                  {:name "Chimera" :heads 1}])
  (api/query db '{:find [[(sum ?heads) ...]]
                  :with [?id]
                  :where [[?id :heads ?heads]]}))


;; why the fuck does
;; (mapv #(do (subvec % 0 result-arity)) resultset)
;; change anything?

;; something to do with the resultset collect thing
;; read up on that

(cond->> resultset
  (:with q)
  (mapv #(do (subvec % 0 result-arity)))
  (some dp/aggregate? find-elements)
  (aggregate find-elements context)
  (some dp/pull? find-elements)
  (pull find-elements context)
  true
  (-post-process find))
