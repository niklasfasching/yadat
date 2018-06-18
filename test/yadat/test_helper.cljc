(ns yadat.test-helper
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            ))


(def laureates (edn/read-string (slurp (io/resource "resources/nobel-prize-laureates.edn"))))

(def datomic-db
  (let [uri (str "datomic:mem://" (gensym))]
    (datomic/create-database uri)
    (let [connection (datomic/connect uri)]
      @(datomic/transact connection laureate-schema-datomic)
      @(datomic/transact connection laureates)
      (datomic/db connection))))

(def datascript-db
  (let [connection (datascript/create-conn laureate-schema-datascript)]
    (datascript/transact connection laureates)
    @connection))

(def yadat-db
  (let [connection (yadat/open :sorted-set laureate-schema-datascript)]
    (yadat/insert connection laureates)
    @connection))


(defn query [query-map & inputs]
  (let [datomic-result (timed (datomic/q query-map datomic-db))
        yadat-result (timed (yadat/q query-map (atom yadat-db)))]
    {:datomic (assoc datomic-result :pass true)
     :yadat (assoc yadat-result :pass
                   (compare-results datomic-result yadat-result))}))
