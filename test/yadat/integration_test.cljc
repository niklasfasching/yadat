(ns yadat.integration-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yadat.core :as yadat]
            [clojure.string :as string]
            [yadat.test-helper :as th])
  (:import (java.util Date)))

(def connection (atom th/yadat-db))

;; maybe just compare against datascript?
;; datomic would be nice but results seem too different - also much more setup
;; idk

;; aggregate
(is (= (yadat/q '{:find [?category (count ?id)]
                  :where [[?id :category ?category]]}
                connection)
       [["chemistry" 178]
        ["peace" 131]
        ["medicine" 214]
        ["physics" 207]
        ["economics" 79]
        ["literature" 114]]))

;; custom aggregate
(defn list [xs]
  (string/join ", " xs))

(is (= (yadat/q '{:find [(yadat.integration-test/list ?surname)]
                  :where [[?id :firstname "Paul"]
                          [?id :surname ?surname]]}
                connection)
       [["Modrich, Ehrlich, Karrer, Krugman, Berg, Greengard, Sabatier"]]))

;; custom predicate
(defn compare-dates [date-string1 date-string2]
  (let [->date (fn [[y m d]]
                 (Date. (Integer/parseInt y)
                        (Integer/parseInt m)
                        (Integer/parseInt d)))
        date1 (->date (string/split date-string1 #"-"))
        date2 (->date (string/split date-string2 #"-"))]
    (.before date1 date2)))

(is (= (yadat/q '{:find [?firstname ?born-date]
                  :where [[?id :firstname ?firstname]
                          [?id :born ?born-date]
                          [(yadat.integration-test/compare-dates "1990-01-01" ?born-date)]]}
                connection)
       [["Malala" "1997-07-12"]]))

;; function & custom function

nil

;; rules

nil

;; pull all

nil

;; predicate
(is (= (yadat/q '{:find [?surname]
                  :where [[?id :surname ?surname]
                          [(clojure.string/starts-with? ?surname "Ein")]]}
                connection)
       [["Einthoven"] ["Einstein"]]))

;; simple join, relation
(is (= (yadat/q '{:find [?firstname ?surname]
                  :where [[?id :firstname ?firstname]
                          [?id :surname ?surname]
                          [?id :prizes ?prize-id]
                          [?id :gender "female"]
                          [?prize-id :year "2003"]]}
                connection)
       [["Shirin" "Ebadi"]]))



;; running tests (timeout: 60s)
;; | datomic | datascript | yadat    |
;; | / (10s) | / (10s)    | / (20 s) |
;; | / (10s) | / (10s)    | X (20 s) |
;; | / (10s) | / (10s)    | X (60 s) |




;; https://github.com/Datomic/day-of-datomic/blob/master/tutorial/schema_queries.clj
;; https://github.com/Datomic/day-of-datomic/blob/master/tutorial/binding.clj




;; rules
[:find ?title ?album ?year
 :in $ % ?artist-name
 :where
 [?a :artist/name   ?artist-name]
 [?t :track/artists ?a]
 [?t :track/name    ?title]
 (track-release ?t ?r)
 [?r :release/name  ?album]
 [?r :release/year  ?year]]

;; rules: graph walk
[:find ?aname ?aname2
 :in $ % [?aname ...]
 :where (collab ?aname ?aname2)]

[:find ?aname2
 :in $ % ?aname
 :where (collab-net-2 ?aname ?aname2)]

;; more rules

[;; Given ?t bound to track entity-ids, binds ?r to the corresponding
 ;; set of album release entity-ids
 [(track-release ?t ?r)
  [?m :medium/tracks ?t]
  [?r :release/media ?m]]

 ;; Supply track entity-ids as ?t, and the other parameters will be
 ;; bound to the corresponding information about the tracks
 [(track-info ?t ?track-name ?artist-name ?album ?year)
  [?t :track/name    ?track-name]
  [?t :track/artists ?a]
  [?a :artist/name   ?artist-name]
  (track-release ?t ?r)
  [?r :release/name  ?album]
  [?r :release/year  ?year]]

 ;; Supply ?a (artist entity-ids) and and integer ?max track duration,
 ;; and ?t, ?len will be bound to track entity-ids and lengths
 ;; (respectively) of tracks shorter than the given ?max
 [(short-track ?a ?t ?len ?max)
  [?t :track/artists ?a]
  [?t :track/duration ?len]
  [(< ?len ?max)]]

 ;; Fulltext search on track.  Supply the query string ?q, and ?track
 ;; will be bound to entity-ids of tracks whose title matches the
 ;; search.
 [(track-search ?q ?track)
  [(fulltext $ :track/name ?q) [[?track ?tname]]]]

 ;; Generic transitive network walking, used by collaboration network
 ;; rule below

 ;; Supply:
 ;; ?e1 -- an entity-id
 ;; ?attr -- an attribute ident
 ;; and ?e2 will be bound to entity-ids such that ?e1 and ?e2 are both
 ;; values of the given attribute for some entity (?x)
 [(transitive-net-1 ?attr ?e1 ?e2)
  [?x ?attr ?e1]
  [?x ?attr ?e2]
  [(!= ?e1 ?e2)]]

 ;; Same as transitive-net-1, but search one more level of depth.  We
 ;; define this rule twice, once for each case, and the rule
 ;; represents the union of the two cases:
 ;; - The entities are directly related via the attribute
 ;; - The entities are related to the given depth (in this case 2) via the attribute
 [(transitive-net-2 ?attr ?e1 ?e2)
  (transitive-net-1 ?attr ?e1 ?e2)]
 [(transitive-net-2 ?attr ?e1 ?e2)
  (transitive-net-1 ?attr ?e1 ?x)
  (transitive-net-1 ?attr ?x ?e2)
  [(!= ?e1 ?e2)]]

 ;; Same as transitive-net-2 but to depth 3
 [(transitive-net-3 ?attr ?e1 ?e2)
  (transitive-net-1 ?attr ?e1 ?e2)]
 [(transitive-net-3 ?attr ?e1 ?e2)
  (transitive-net-2 ?attr ?e1 ?x)
  (transitive-net-2 ?attr ?x ?e2)
  [(!= ?e1 ?e2)]]

 ;; Same as transitive-net-2 but to depth 4
 [(transitive-net-4 ?attr ?e1 ?e2)
  (transitive-net-1 ?attr ?e1 ?e2)]
 [(transitive-net-4 ?attr ?e1 ?e2)
  (transitive-net-3 ?attr ?e1 ?x)
  (transitive-net-3 ?attr ?x ?e2)
  [(!= ?e1 ?e2)]]

 ;; Artist collaboration graph-walking rules, based on generic
 ;; graph-walk rule above

 ;; Supply an artist name as ?artist-name-1, an ?artist-name-2 will be
 ;; bound to the names of artists who directly collaborated with the
 ;; artist(s) having that name
 [(collab ?artist-name-1 ?artist-name-2)
  [?a1 :artist/name ?artist-name-1]
  (transitive-net-1 :track/artists ?a1 ?a2)
  [?a2 :artist/name ?artist-name-2]]

 ;; Collaboration network walk to depth 2
 [(collab-net-2 ?artist-name-1 ?artist-name-2)
  [?a1 :artist/name ?artist-name-1]
  (transitive-net-2 :track/artists ?a1 ?a2)
  [?a2 :artist/name ?artist-name-2]]

 ]


(d/q '[:find ?track-name ?minutes
       :in $ ?artist-name
       :where [?artist :artist/name ?artist-name]
       [?track :track/artists ?artist]
       [?track :track/duration ?millis]
       [(quot ?millis 60000) ?minutes]
       [?track :track/name ?track-name]]
     db "John Lennon")

(d/q '[:find ?celsius .
       :in ?fahrenheit
       :where [(- ?fahrenheit 32) ?f-32]
              [(/ ?f-32 1.8) ?celsius]]
     212)



;; fixes previous query
(d/q '[:find (sum ?heads) .
       :with ?monster
       :in [[?monster ?heads]]]
     [["Cerberus" 3]
      ["Medusa" 1]
      ["Cyclops" 1]
      ["Chimera" 1]])


(d/q '[:find [(min ?dur) (max ?dur)]
        :where [_ :track/duration ?dur]]
     db)

(d/q '[:find [(count ?name) (count-distinct ?name)]
       :with ?artist
       :where [?artist :artist/name ?name]]
     db)

(d/q '[:find (count ?track)
       :where [?track :track/name]]
     db)



;; This query leads with a where clause that must consider *all* releases
;; in the database.  SLOW.
(dotimes [_ 5]
  (time
   (d/q '[:find [?name ...]
          :in $ ?artist
          :where [?release :release/name ?name]
                 [?release :release/artists ?artist]]
        db
        mccartney)))

;; The same query, but reordered with a more selective where clause first.
;; 50 times faster.
(dotimes [_ 5]
  (time
   (d/q '[:find [?name ...]
          :in $ ?artist
          :where [?release :release/artists ?artist]
                 [?release :release/name ?name]]
        db
        mccartney)))

;; attribute name
(d/pull db [:artist/name :artist/startYear] led-zeppelin)

;; reverse lookup
(d/pull db [:artist/_country] :country/GB)

;; component defaults
(d/pull db [:release/media] dark-side-of-the-moon)

;; noncomponent defaults (same example as "reverse lookup")
(d/pull db [:artist/_country] :country/GB)

;; reverse component lookup
(d/pull db [:release/_media] dylan-harrison-cd)

;; map specifications
(d/pull db [:track/name {:track/artists [:db/id :artist/name]}] ghost-riders)

;; nested map specifications
(d/pull db
        [{:release/media
          [{:medium/tracks
            [:track/name {:track/artists [:artist/name]}]}]}]
        concert-for-bangla-desh)

;; wildcard specification
(d/pull db '[*] concert-for-bangla-desh)

;; wildcard + map specification
(d/pull db '[* {:track/artists [:artist/name]}] ghost-riders)

;; default option
(d/pull db '[:artist/name (:artist/endYear :default 0)] mccartney)


;; default option with different type
(d/pull db '[:artist/name (:artist/endYear :default "N/A")] mccartney)

;; absent attributes are omitted from results
(d/pull db '[:artist/name :died-in-1966?] mccartney)

;; explicit limit
(d/pull db '[(:track/_artists :limit 10)] led-zeppelin)

;; limit + subspec
(d/pull db '[{(:track/_artists :limit 10) [:track/name]}]
        led-zeppelin)

;; limit + subspec + :as option
(d/pull db '[{(:track/_artists :limit 10 :as "Tracks") [:track/name]}]
        led-zeppelin)
;; no limit
(d/pull db '[(:track/_artists :limit nil)] led-zeppelin)

;; empty results
(d/pull db '[:penguins] led-zeppelin)

;; empty results in a collection
(d/pull db '[{:track/artists [:penguins]}] ghost-riders)

;; dynamic pattern input
(d/q '[:find [(pull ?e pattern) ...]
       :in $ ?artist pattern
       :where [?e :release/artists ?artist]]
     db
     led-zeppelin
     [:release/name])

;; use pull to traverse the graph from anne through recursion:
;; a depth of 1
(d/pull db '[[:person/name :as :name] {[:person/friend :as :pals] 1}] anne-id)

;; a depth of 2
(d/pull db '[[:person/name :as :name] {[:person/friend :as :pals] 2}] anne-id)

;; expand all nodes reachable from anne
(d/pull db '[:person/name {:person/friend ...}] anne-id)

;; we can also traverse the graph in reverse (reverse ref in pull pattern)
(d/pull db '[:person/name {[:person/_friend :as :pals] 1}] anne-id)
(d/pull db '[:person/name {[:person/_friend :as :pals] ...}] anne-id)
