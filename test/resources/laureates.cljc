(ns resources.laureates)

;; store as edn and create dbs with it
;; inside integration test everything has to be done just once! only here is duplication
;; because schema & setup differ...



(def yadat-schema
  {:prizes {:db/valueType :db.type/ref
            :db/cardinality :db.cardinality/many}
   :affiliations {:db/valueType :db.type/ref
                  :db/cardinality :db.cardinality/many}})

(def datomic-schema
  [{:db/ident :firstname
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
    :db/cardinality :db.cardinality/many}

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
   {:db/ident :affiliations
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}

   {:db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :city
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :country
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])
