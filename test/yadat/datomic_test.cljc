(ns yadat.datomic-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [datomic.api :as datomic]))

(def uri "datomic:mem://tmp")
(datomic/create-database uri)
(def conn (datomic/connect uri))

(def schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :person/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value
    :db/doc "The email of the person"
    :db.install/_attribute :db.part/db}])

(datomic/transact conn schema)

(def add-emails
  [{:db/id #db/id[:db.part/user -1000001] :person/email "example@gmail.com"}
   {:db/id #db/id[:db.part/user -1000002] :person/email "john@gmail.com"}
   {:db/id #db/id[:db.part/user -1000003] :person/email "jane@gmail.com"}])

(datomic/transact conn add-emails)

(let [db (datomic/db conn)]
  (datomic/q '{:find [(pull ?e [:person/email])]
               :in [$]
               :where [[?e :person/email _]]}
             db))
