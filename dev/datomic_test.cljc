(ns yadat.datomic-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [datomic.api :as datomic]
            [datomic.db]
            ))


;; datum!
(datomic.db/->Datum e a v tOp)

;; uses attribute record! maybe i can copy that
(datomic.db/->Attribute )

;; db fn
(datomic.db/->Function )

;; indexes
(datomic.db/->IndexSet)

;; mhhh
(prn datomic.db/INDEX) 44
(prn datomic.db/CARDINALITY) 41
(prn datomic.db/CARDINALITY_MANY) 36

(datomic.db/asserting-datum 1 2 3 -10)
(datomic.db/retracting-datum 1 2 3 -10)

(require '[datomic.btset])
(datomic.btset/seek)


(require '[datomic.datalog])

(datomic.datalog/variable? :?a) ;; => true lol

;; maybe use intellij to have a look at the decompiled?
;; or just to explore the api
(datum )
(defrecord Foo [])
(extend datomic.db.IDatumImpl
  Foo
  )


;; get methods of interface
(clojure.pprint/pprint (.getMethods datomic.db.IDb))

(require '[datomic.query :as query])
(= (query/parse-query '{:find [?a]
                     :where [[?a _ "a"]]}))

(query/validate-query '{:find [?a ?b]
                        :where [[?a _ "a"]]})
(first query/query-cache) ;; => datomic.cache.WrappedGCache

(require '[datomic.cache :as cache])
(map #(.getName %) (.getInterfaces datomic.cache.WrappedGCache))
(clojure.pprint/pprint (.getMethods datomic.cache.CacheKeys))


(clojure.pprint/pprint (cache/cache-keys query/query-cache))
(clojure.pprint/pprint (cache/get-from-cache query/query-cache
                      (nth (cache/cache-keys query/query-cache) 3) :nf))
{:find (?id ?type ?gender),
 :in [$ $__in__2],
 :where
 [{:argvars nil,
   :fn #function[datomic.datalog/expr-clause/fn--6515],
   :clause [(ground $__in__2) ?name],
   :binds [?name],
   :bind-type :scalar,
   :needs-source true}
  [?e :artist/name ?name]
  [?e :artist/gid ?id]
  [?e :artist/type ?teid]
  [?teid :db/ident ?type]
  [?e :artist/gender ?geid]
  [?geid :db/ident ?gender]],
 :in-consts {?name $__in__2},
 :arules nil}


(map #(.getName %) (.getInterfaces datomic.datalog.PredRel))
(map #(.getName %) (.getInterfaces datomic.db.Datum))
(map #(.getName %) (.getMethods datomic.Datom))
(clojure.pprint/pprint (.getMethods clojure.lang.IType))

(query/pattern? '[(?a b) b])



;; there is also seekRAET - (reverse index?)





(datomic/create-database uri)
(datomic/connect uri)
(datomic/db connection) -> retrieve db from connection
(datomic/transact) -> retrieve db from connection


(datomic/create-database "datomic:mem://tmp")
(def datomic-connection (datomic/connect "datomic:mem://tmp"))
(def datomic-db (datomic/db datomic-connection))
(datomic/transact)
(datomic/with datomic-db (map #(do (concat [:db/add] %)) datoms))

(map #(do (concat [:db/add] %)) datoms)

(def uri "datomic:mem://tmp")

(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")
(def conn (datomic/connect uri))
(def db (datomic/db conn))

(datomic/q '[:find ?id ?type ?gender
             :in $ ?name
             :where
             [?e :artist/name ?name]
             [?e :artist/gid ?id]
             [?e :artist/type ?teid]
             [?teid :db/ident ?type]
             [?e :artist/gender ?geid]
             [?geid :db/ident ?gender]]
           db
           "Janis Joplin")



(datomic/q '[:find ?title
             :in $ ?artist-name
             :where
             [?a :artist/name ?artist-name]
             [?t :track/artists ?a]
             [?t :track/name ?title]]
           db
           "John Lennon")


(datomic/q '[:find ?title ?album ?year
             :in $ ?artist-name
             :where
             [?a :artist/name   ?artist-name]
             [?t :track/artists ?a]
             [?t :track/name    ?title]
             [?m :medium/tracks ?t]
             [?r :release/media ?m]
             [?r :release/name  ?album]
             [?r :release/year  ?year]
             [(< ?year 1970)]]
           db
           "John Lennon")


(count (take 1000000000 (datomic/datoms db :eavt))) ;; ~847000
;; reduce size of set and use that as test data?
(select-keys (first (datomic/datoms db :eavt)) [:e :a :v])



(def attributes (->> (datomic/datoms db :eavt)
                     (map :a)
                     distinct
                     (map #(do [% (:db/ident (datomic/entity db %))]))
                     (remove #(clojure.string/starts-with? (str (second %)) ":db"))
                     (into {})))

(defn ->yadat-datom [datum]
  (when-let [a (get attributes (:a datum))]
    [(:e datum) a (:v datum)]))


(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")
(def conn (datomic/connect uri))
(def db (datomic/db conn))

(def datoms (take 100000000 (datomic/datoms db :eavt)))
(binding [*print-length* nil]
  (spit "mbrainz-datomic-datoms.edn"
        (pr-str (take 100000000 (datomic/datoms db :eavt)))))


(def uri-mem "datomic:mem://foobar")
(datomic/create-database uri-mem)
(def conn-mem (datomic/connect uri-mem))
(def db-mem (datomic/db conn-mem))
(datomic/transact conn-mem datoms)
(require '[datomic.db])


(def datoms (clojure.edn/read-string (slurp "mbrainz-datoms.edn")))

(require '[yadat.core :as yadat])

(def yadat-connection (yadat/open :sorted-set {}))

(yadat/db-with yadat-connection datoms)

(time (yadat/query yadat-connection
                   '{:find [[?a ...]]
                     :where [[_ ?a _]]}))
(def yadat-results (time (yadat/query yadat-connection
                                      '{:find [?title ?album ?year]
                                        :where [[?a :artist/name   "John Lennon"]
                                                [?t :track/artists ?a]
                                                [?t :track/name    ?title]
                                                [?m :medium/tracks ?t]
                                                [?r :release/media ?m]
                                                [?r :release/name  ?album]
                                                [?r :release/year  ?year]]})))

(def datomic-results (time (datomic/q '{:find [?title ?album ?year]
                                        :where [[?a :artist/name   "John Lennon"]
                                                [?t :track/artists ?a]
                                                [?t :track/name    ?title]
                                                [?m :medium/tracks ?t]
                                                [?r :release/media ?m]
                                                [?r :release/name  ?album]
                                                [?r :release/year  ?year]]} db)))

(datomic/q '{:find [(pull ?a [* {:artist/type [*]} {:artist/gender [*]}])]
             :where [[?a :artist/name   "John Lennon"]]} db)
(first (datomic/q '{:find [(pull ?t [*])]
                    :where [[?a :artist/name "John Lennon"]
                            [?t :track/artists ?a]]} db))

(= (sort yadat-results) (sort datomic-results)) ;; => true... 500ms vs 10s though lol

;; now also datascript

;; it works but takes like 10 seconds

;; so...

(doc datomic.db.Datum)
(datomic.db/->Datum )
datomic.impl.db.IDatum

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

(datomic/q '{:find [?a]
             :where [[_ ?aid _]
                     [?aid :db/ident ?a]]}
           db)


;; example queries
;; https://github.com/Datomic/mbrainz-sample/wiki/Queries
;; aslo rules https://github.com/Datomic/mbrainz-sample/blob/master/resources/rules.edn
;; also day of datomic
;; also datascript tests