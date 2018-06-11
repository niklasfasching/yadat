(ns yadat.dsl.minimal-test
  (:require [yadat.dsl.minimal :as minimal]
            #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]]
               [clojure.string :as string]
               [yadat.dsl.minimal :as dslm]
               [yadat.relation :as r]
               [yadat.test-helper :as test-helper]
               [yadat.dsl :as dsl])))

(def food-rows '[{?id 1 ?name "Bread"}
                 {?id 2 ?name "Banana"}
                 {?id 3 ?name "Butter"}])

(deftest resolve-find-spec-test
  (testing "relation"
    (let [find-spec (dsl/find-spec '[?id ?name])
          result (query/resolve-find-spec find-spec nil food-rows)]
      (is (= result [[1 "Bread"] [2 "Banana"] [3 "Butter"]]))))

  (testing "scalar"
    (let [find-spec (dsl/find-spec '[?name .])
          result (query/resolve-find-spec find-spec nil food-rows)]
      (is (= result "Bread"))))

  (testing "collection"
    (let [find-spec (dsl/find-spec '[[?name ...]])
          result (query/resolve-find-spec find-spec nil food-rows)]
      (is (= result ["Bread" "Banana" "Butter"]))))

  (testing "tuple"
    (let [find-spec (dsl/find-spec '[[?id ?name]])
          result (query/resolve-find-spec find-spec nil food-rows)]
      (is (= result [1 "Bread"]))))

  (testing "aggregate"
    (let [rows '[{?name "Cerberus" ?head-id 1}
                 {?name "Cerberus" ?head-id 2}
                 {?name "Cerberus" ?head-id 3}
                 {?name "Medusa" ?head-id 4}
                 {?name "Cyclops" ?head-id 5}]
          find-spec (dsl/find-spec '[?name (count ?head-id)])
          result (query/resolve-find-spec find-spec nil rows)]
      (is (= result [["Cerberus" 3] ["Medusa" 1] ["Cyclops" 1]])))))

(deftest resolve-find-element-test
  (testing "variable"
    (let [find-element (dsl/find-element '?id)
          id (query/resolve-find-element find-element nil (first food-rows))]
      (is (= id 1))))

  (testing "pull"
    (let [db (test-helper/recipe-db)
          find-element (dsl/find-element '(pull ?eid [:author/name]))
          result (query/resolve-find-element find-element db '{?eid 10})]
      (is (= result {:db/id 10 :author/name "Adam"}))))

  (testing "aggregate"
    (let [find-element (dsl/find-element '(count ?eid))
          result (query/resolve-find-element find-element nil '{?eid 1})]
      (is (= result 1)))))

(deftest resolve-clause-test
  (testing "pattern"
    (let [db (test-helper/recipe-db)
          clause1 (dsl/where-clause '[_ :recipe/name ?name])
          clause2 (dsl/where-clause '[?id :author/name ?name])
          [r1] (query/resolve-clause clause1 db [])
          [r2] (query/resolve-clause clause2 db [])]
      (is (= (:columns r1) '#{?name}))
      (is (= (:rows r1) '#{{?name "Banana bread sandwhich"}
                           {?name "Spaghetti with tomato sauce"}
                           {?name "Bread with butter"}}))
      (is (= (:columns r2) '#{?id ?name}))
      (is (= (:rows r2) '#{{?id 10, ?name "Adam"}
                           {?id 20, ?name "Eve"}
                           {?id 30, ?name "Theo"}}))))

  (testing "predicate"
    (let [relation (r/relation '#{?id ?name} food-rows)
          clause (dsl/where-clause '[(re-find #"Bread|Butter" ?name)])
          [relation] (query/resolve-clause clause nil [relation])]
      (is (= (:columns relation) '#{?id ?name}))
      (is (= (:rows relation) '#{{?id 3, ?name "Butter"}
                                 {?id 1, ?name "Bread"}}))))

  (testing "function"
    (let [relation (r/relation '#{?id ?name} food-rows)
          clause (dsl/where-clause
                  '[(clojure.string/upper-case ?name) ?NAME])
          [relation] (query/resolve-clause clause nil [relation])]
      (is (= (:columns relation) '#{?id ?name ?NAME}))
      (is (= (:rows relation) '#{{?id 1, ?name "Bread", ?NAME "BREAD"}
                                 {?id 2, ?name "Banana", ?NAME "BANANA"}
                                 {?id 3, ?name "Butter", ?NAME "BUTTER"}}))))

  (testing "or"
    (let [db (test-helper/recipe-db)
          clause (dsl/where-clause '(or [?id :author/name "Adam"]
                                           [?id :author/name "Eve"]))
          [relation] (query/resolve-clause clause db [])]
      (is (= (:columns relation) '#{?id}))
      (is (= (:rows relation) '#{{?id 10} {?id 20}}))))

  (testing "and"
    (let [db (test-helper/recipe-db)
          clause (dsl/where-clause '(and [?id :food/name "Banana"]
                                            [?id :food/category "Fruit"]))
          [relation] (query/resolve-clause clause db [])]
      (is (= (:columns relation) '#{?id}))
      (is (= (:rows relation) '#{{?id 100}}))))

  (testing "not"
    (let [db (test-helper/recipe-db)
          relation (r/relation '#{?name} '#{{?name "Banana"}
                                            {?name "Bread"}
                                            {?name "Apple"}})
          clause (dsl/where-clause '(not [_ :food/name ?name]))
          [relation] (query/resolve-clause clause db [relation])]
      (is (= (:columns relation) '#{?name}))
      (is (= (:rows relation) '#{{?name "Apple"}})))))

(deftest resolve-clauses-test
  (testing "pattern + predicate"
    (let [db (test-helper/recipe-db)
          clause (dsl/where-clauses
                  '[[?r-id :recipe/name ?r-name]
                    [?r-id :recipe/ingredients ?i-id]
                    [?i-id :ingredient/food ?f-id]
                    [?f-id :food/name ?f-name]
                    [(re-find #"Bread" ?f-name)]])
          relations (query/resolve-clause clause db [])
          relation (r/merge r/inner-join relations)]
      (is (= (:columns relation) '#{?r-id ?r-name ?i-id ?f-id ?f-name}))
      (is (= (map #(get % '?r-name) (:rows relation))
             ["Banana bread sandwhich" "Bread with butter"]))))

  (testing "pattern + not + or"
    (let [db (test-helper/recipe-db)
          clause (dsl/where-clauses
                  '[[?r-id :recipe/name ?r-name]
                    (not (or [?r-id :recipe/name "Bread with butter"]
                             [?r-id :recipe/name "Banana bread sandwhich"]))])
          relations (query/resolve-clause clause db [])
          relation (r/merge r/inner-join relations)]
      (is (= (:columns relation) '#{?r-id ?r-name}))
      (is (= (map #(get % '?r-name) (:rows relation))
             ["Spaghetti with tomato sauce"])))))

(deftest resolve-pull-element-test
  (testing "wildcard"
    (let [db (test-helper/recipe-db)
          pull-element (dsl/pull-element '*)
          entity (query/resolve-pull-element pull-element db {:db/id 10})]
      (is (= entity {:db/id 10
                     :author/name "Adam"
                     :author/gender "M"}))))

  (testing "attribute"
    (let [db (test-helper/recipe-db)
          pull-element (dsl/pull-element ':author/name)
          entity (query/resolve-pull-element pull-element db {:db/id 10})]
      (is (= entity {:db/id 10
                     :author/name "Adam"}))))

  (testing "map"
    (let [db (test-helper/recipe-db)
          pull-element (dsl/pull-element '{:recipe/author [*]})
          entity (query/resolve-pull-element pull-element db {:db/id 42})]
      (is (= entity {:db/id 42
                     :recipe/author {:db/id 30
                                     :author/name "Theo"
                                     :author/gender "F"}})))))

(deftest pull-test
  (testing "resolve lookup-reference"
    (let [db (test-helper/recipe-db)
          entities nil #_(query/pull db
                                     [:author/name "Adam"]
                                     '[:author/name {:_recipe/author [*]}])]
      entities)))
