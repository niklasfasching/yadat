(ns yadat.integration-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            #?(:clj [clojure.edn :as edn])
            #?(:clj [clojure.java.io :as io])
            [clojure.string :as string]
            [clojure.pprint :as pprint]
            [datascript.core :as datascript]
            #?(:clj [datomic.api :as datomic])
            [yadat.core :as yadat])
  #?(:clj (:import (java.util.HashSet))))

(defmacro def-edn
  "Defines a var named `name` with datasctructure parsed from edn at `path`.
   As cljs does not support slurping we have get the test data via a macro,
   which is executed in clj."
  [name path]
  `(def ~name (edn/read-string (slurp (io/resource ~path)))))

(declare laureates)
(def-edn laureates "resources/nobel-prize-laureates.edn")

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

#?(:clj
   (def datomic
     {:name "datomic"
      :q datomic/q
      :db (let [uri (str "datomic:mem://" (gensym))]
            (datomic/create-database uri)
            (let [connection (datomic/connect uri)]
              @(datomic/transact connection laureate-schema-datomic)
              @(datomic/transact connection laureates)
              (datomic/db connection)))}))

(def datascript
   {:name "datascript"
    :q datascript/q
    :db (let [connection (datascript/create-conn laureate-schema-datascript)]
          (datascript/transact connection laureates)
          @connection)})

(def yadat
   {:name "yadat"
    :q yadat/q
    :db (let [schema (assoc laureate-schema-datascript :schema/type :datomish)
              connection (yadat/open :sorted-set schema)]
          (yadat/transact connection laureates)
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
  (time (apply q (concat [query] (inputs-fn db)))))

(defn compare-results [result1 result2]
  (if (or (instance? java.util.HashSet result1)
          (and (coll? result1) (not (map? result1))))
    (= (frequencies result1) (frequencies result2))
    (= result1 result2)))

(defn test-case [case reference-engine engines]
  (let [reference-result (run-case reference-engine case)]
    (doseq [engine engines]
      (is (compare-results reference-result (run-case engine case))
          (str "Comparing " (:name reference-engine) " against\n"
               (:name engine) " in:\n"
               (with-out-str (pprint/pprint (:query case))))))))

(defn run-cases [reference-engine engines cases]
  (doseq [case cases]
    (test-case case reference-engine engines)))

(deftest integration-test
  #?(:clj (run-cases datomic [datascript yadat] cases)
     :cljs (run-cases datascript [yadat] cases)))

(comment

  (run-case datomic {:query '{:find [(pull ?id [(:affiliations :limit 1 :default [])])]
                              :where [[?id :year "2002"]]}
                     :inputs-fn (fn [db] [db])})



  (run-case datomic {:query '{:find [(pull ?id [:_prizes])]
                              :where [[?id :year "2002"]
                                      ]}
                     :inputs-fn (fn [db] [db])})

  (run-case yadat {:query '{:find [(?sum ?b)]
                            :in [$ ?predicate ?sum]
                            :where [[?a :year ?b]
                                    [(?predicate ?b)]]
                            }
                   :inputs-fn (fn [db] [db #(re-find #"199" %) #(first %)])})

  (run-case yadat {:query '{:find [(pull ?id ?pattern)]
                            :in [$ ?pattern]
                            :where [[?id :year "1990"]]
                            }
                   :inputs-fn (fn [db] [db '[*]])})

  ;; everything still x100 slower than datascript, and even more compared to
  ;; datomic ...

  (run-case yadat (first (filter #(= (:name %) "custom aggregate") cases)))
  (run-case datomic (first (filter #(= (:name %) "custom aggregate") cases)))

  ;; https://github.com/Datomic/day-of-datomic/blob/master/tutorial/schema_queries.clj
  ;; https://github.com/Datomic/day-of-datomic/blob/master/tutorial/binding.clj
  )
