(ns yadat.integration-test
  (:require [clojure.test :refer :all]
            [datascript.core :as datascript]
            [yadat.core :as yadat]
            [datomic.api :as datomic]))

(def results (atom []))

(def schema {})
(def datomic-db (do
                  (datomic/create-database "datomic:mem://temp")
                  (let [connection (datomic/connect "datomic:mem://temp")]
                    (datomic/transact connection schema)
                    (datomic/db connection))))
(def datascript-db @(datascript/create-conn schema))
(def yadat-db @(yadat/open :sorted-set schema))


;; also need to insert the rows into the dbs. so setup fns would make sense after all

(defmacro timed [& body]
  `(do
     (prn "Running" '~@body)
     (let [start# (. System (nanoTime))
           result# (do ~@body)
           time# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
       {:result result# :time time# })))

;; collect results in global var and print when all tests finished
(defn query-test [query-map]
  (let [datomic-result (timed (datomic/q query-map datomic-db))
        datascript-result (timed (datascript/q query-map datascript-db))
        yadat-result (timed (yadat/q query-map yadat-db))

        yadat-passed? (is (= (sort (:result datomic-result))
                             (sort (:result yadat-result))))
        datascript-passed? (is (= (sort (:result datomic-result))
                                  (sort (:result datascript-result))))
        result {:datomic {:pass true
                          :time (:time datomic-result)}
                :datascript {:pass datascript-passed?
                             :time (:time datascript-result)}
                :yadat {:pass yadat-passed?
                        :time (:time yadat-result)}}]
    (clojure.pprint/pprint result)
    (swap! results conj result)))

(deftest integration-test
  (testing "John Lennon"
    (query-test '{:find [?title ?album ?year]
                  :where [[?a :artist/name   "John Lennon"]
                          [?t :track/artists ?a]
                          [?t :track/name    ?title]
                          [?m :medium/tracks ?t]
                          [?r :release/media ?m]
                          [?r :release/name  ?album]
                          [?r :release/year  ?year]]})))

;; running tests (timeout: 60s)
;; | datomic | datascript | yadat    |
;; | / (10s) | / (10s)    | / (20 s) |
;; | / (10s) | / (10s)    | X (20 s) |
;; | / (10s) | / (10s)    | X (60 s) |
