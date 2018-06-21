(ns yadat.integration-test
  "Datomic is always right.
  Writing test cases with expected data is annoying.
  Writing tests can be simplified a lot by just comparing the result to datomic
  and saying datomic is always right though. So here we are...

  Also test performance. Yadat is a few magnitudes slower :D"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            #?(:clj [clojure.pprint :as pprint]
               :cljs [cljs.pprint :as pprint])
            [datascript.core :as datascript]
            [datomic.api :as datomic]
            [yadat.core :as yadat])
  (:import (java.util.HashSet)))

(def laureates
  (edn/read-string (slurp (io/resource "resources/nobel-prize-laureates.edn"))))

(def laureate-schema-datascript
  {:id {:db/unique :db.unique/value}
   :prizes {:db/valueType :db.type/ref
            :db/cardinality :db.cardinality/many
            :db/isComponent true}
   :affiliations {:db/valueType :db.type/ref
                  :db/cardinality :db.cardinality/many
                  :db/isComponent true}})

(def laureate-schema-datomic
  (->> laureate-schema-datascript
       (merge {:firstname {}
               :surname {}
               :born {}
               :bornCountry {}
               :bornCountryCode {}
               :bornCity {}
               :gender {}
               :died {}
               :diedCountry {}
               :diedCountryCode {}
               :diedCity {}
               :year {}
               :category {}
               :share {}
               :motivation {}
               :overallMotivation {}
               :name {}
               :city {}
               :country {}})
       (map (fn [[name options]]
              (merge {:db/ident name
                      :db/cardinality :db.cardinality/one
                      :db/valueType :db.type/string}
                     options)))))

(def datomic
  {:name "datomic"
   :q datomic/q
   :db (let [uri (str "datomic:mem://" (gensym))]
         (datomic/create-database uri)
         (let [connection (datomic/connect uri)]
           @(datomic/transact connection laureate-schema-datomic)
           @(datomic/transact connection laureates)
           (datomic/db connection)))})

(def datascript
  {:name "datascript"
   :q datascript/q
   :db (let [connection (datascript/create-conn laureate-schema-datascript)]
         (datascript/transact connection laureates)
         @connection)})

(def yadat
  {:name "yadat"
   :q yadat/q
   :db (let [connection (yadat/open :sorted-set laureate-schema-datascript)]
         (yadat/insert connection laureates)
         @connection)})

(def cases
  "Each case is a map containing :name, :query & :inputs.
  Cases are executed for datomic, datascript & yadat. Results are validated by
  comparing them against the result of datomic. The value of :inputs-fn is fn
  taking the db as its only argument and returns the list of used inputs."
  [
   {:name "predicate"
    :query '{:find [?surname]
             :where [[?id :surname ?surname]
                     [(clojure.string/starts-with? ?surname "Ein")]]}
    :inputs-fn (fn [db] [db])}

   {:name "function"
    :query '{:find [?upper-surname]
             :where [[?id :surname ?surname]
                     [(clojure.string/upper-case ?surname) ?upper-surname]]}
    :inputs-fn (fn [db] [db])}

   {:name "aggregate"
    :query '{:find [?category (count ?id)]
             :where [[?id :category ?category]]}
    :inputs-fn (fn [db] [db])}

   {:name "simple join"
    :query '{:find [?firstname ?surname]
             :where [[?id :firstname ?firstname]
                     [?id :surname ?surname]
                     [?id :prizes ?prize-id]
                     [?id :gender "female"]
                     [?prize-id :year "2003"]]}
    :inputs-fn (fn [db] [db])}

   ;; TODO resolve only works in clj!
   {:name "custom predicate (resolve)"
    :query '{:find [?firstname ?born-date]
             :where [[?id :firstname ?firstname]
                     [?id :born ?born-date]
                     [(yadat.integration-test/born-in-year
                       ?born-date "1997")]]}
    :inputs-fn (fn [db] [db])}

   {:name "custom aggregate"
    :query '{:find [(yadat.integration-test/sorted-interpose ?surname)]
             :where [[?id :firstname "Paul"]
                     [?id :surname ?surname]]}
    :inputs-fn (fn [db] [db])}

   {:name "without db"
    :query '{:find [?celsius]
             :in [?fahrenheit]
             :where [[(- ?fahrenheit 32) ?f-32]
                     [(/ ?f-32 1.8) ?celsius]]}
    :inputs-fn (fn [_] [212])}

   {:name "monsters"
    :query '{:find [(sum ?heads) .]
             :with [?monster]
             :in [[[?monster ?heads]]]}
    :inputs-fn (fn [_] [[["Cerberus" 3]
                         ["Medusa" 1]
                         ["Cyclops" 1]
                         ["Chimera" 1]]])}
   ])


(defn sorted-interpose [xs]
  (interpose "," (sort xs)))

(defn born-in-year [born-date-string yyyy-string]
  (string/starts-with? born-date-string yyyy-string))

(defn run-case
  [{:keys [q db] :as engine} {:keys [name query inputs-fn] :as case}]
  (apply q (concat [query] (inputs-fn db))))

(defn compare-results [result1 result2]
  (if (or (instance? java.util.HashSet result1)
          (and (coll? result1) (not (map? result1))))
    (= (frequencies result1) (frequencies result2))
    (= result1 result2)))

(defn test-case [case reference-engine engines]
  (let [reference-result (run-case reference-engine case)]
    (doseq [engine engines]
      (is (compare-results reference-result (run-case engine case))
          (format "Comparing %s against %s in:\n %s"
                  (:name reference-engine)
                  (:name engine)
                  (with-out-str (pprint/pprint (:query case))))))))

(defn run-cases [reference-engine engines cases]
  (doseq [case cases]
    (test-case case reference-engine engines)))

(deftest integration-test
  (run-cases datomic [datascript yadat] cases))

(run-case datascript (first (filter #(= (:name %) "custom aggregate") cases)))


(run-case yadat {:query '{:find [?a ?b]
                          :where [[?a :name ?b]]
                          }
                 :inputs-fn (fn [db] [db])})



(comment
  (run-case yadat (first (filter #(= (:name %) "custom aggregate") cases)))
  (run-case datomic (first (filter #(= (:name %) "custom aggregate") cases)))

  #_(defmacro timed [& body]
      `(do
         (let [start# (. System (nanoTime))
               result# (do ~@body)
               time# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
           {:result result# :time time#})))

  ;; rules
  nil

  ;; pull all
  nil

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
  )
