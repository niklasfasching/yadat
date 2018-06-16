(ns yadat.test-helper
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [datascript.core :as datascript]
            [datomic.api :as datomic]
            [yadat.core :as yadat]))

(def laureates (edn/read-string (slurp (io/resource "resources/nobel-prize-laureates.edn"))))

(def laureate-schema-datascript
  {:id {:db/unique :db.unique/value}
   :prizes {:db/valueType :db.type/ref
            :db/cardinality :db.cardinality/many
            :db/isComponent true}
   :affiliations {:db/valueType :db.type/ref
                  :db/cardinality :db.cardinality/many
                  :db/isComponent true}})

(def laureate-schema-datomic
  [{:db/ident :id
    :db/unique :db.unique/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :firstname
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :surname
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :born
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :bornCountry
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :bornCountryCode
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :bornCity
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :gender
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :died
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :diedCountry
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :diedCountryCode
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :diedCity
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :prizes
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

   {:db/ident :year
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :category
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :share
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :motivation
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :overallMotivation
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :affiliations
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/isComponent true}

   {:db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :city
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :country
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

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

(defmacro timed [& body]
  `(do
     (let [start# (. System (nanoTime))
           result# (do ~@body)
           time# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
       {:result result# :time time#})))

(defn compare-results [r1 r2]
  (if (and (coll? r1) (coll? r2))
    (is (= (frequencies (:result r1))
           (frequencies (:result r2))))
    (is (= (:result r1) (:result r2)))))

(defn query [query-map]
  (let [datomic-result (timed (datomic/q query-map datomic-db))
        datascript-result (timed (datascript/q query-map datascript-db))
        yadat-result (timed (yadat/q query-map (atom yadat-db)))]
    {:datomic (assoc datomic-result :pass true)
     :datascript (assoc datascript-result :pass
                        (compare-results datomic-result datascript-result))
     :yadat (assoc yadat-result :pass
                   (compare-results datomic-result datascript-result))}))

(defn log [{:keys [datomic datascript yadat]}]
  (clojure.pprint/print-table
   [{:datomic (:time datomic)
     :datascript (:time datascript)
     :yadat (:time yadat)}]))
