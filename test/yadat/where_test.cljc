(ns yadat.where-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [yadat.where :as where]
            [yadat.test-helper :as test-helper]
            [yadat.util :as util]
            [yadat.relation :as r]))

(deftest resolve-clause-test
  (testing "pattern"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [2 :name "Bar"]
                                    [3 :name "Baz"]])
          clause1 '[?id :name "Foo"]
          clause2 '[?id _ "Foo"]
          [r1] (where/resolve-clause db [] clause1)
          [r2] (where/resolve-clause db [] clause2)]
      (is (= (select-keys r1 [:columns :rows])
             (select-keys r2 [:columns :rows])))
      (is (= (select-keys r1 [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 1}}}))))

  (testing "predicate"
    (let [relation (r/relation '#{?id ?name} '#{{?id 1 ?name "Foo"}
                                                {?id 2 ?name "Bar"}
                                                {?id 3 ?name "Baz"}})
          clause '[(re-find #"Bar|Baz" ?name)]
          [relation] (where/resolve-clause nil [relation] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id ?name}
              :rows '#{{?id 2 ?name "Bar"}
                       {?id 3 ?name "Baz"}}}))))

  (testing "function"
    (let [relation (r/relation '#{?id ?name} '#{{?id 1 ?name "Foo"}
                                                {?id 2 ?name "Bar"}})
          clause '[(clojure.string/upper-case ?name) ?upper-name]
          [relation] (where/resolve-clause nil [relation] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id ?name ?upper-name}
              :rows '#{{?id 1 ?name "Foo" ?upper-name "FOO"}
                       {?id 2 ?name "Bar" ?upper-name "BAR"}}}))))

  (testing "or"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [2 :name "Bar"]
                                    [3 :name "Baz"]])
          clause '(or [?id :name "Foo"]
                      [?id :name "Bar"])
          [relation] (where/resolve-clause db [] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 1} {?id 2}}}))
      ))

  (testing "and"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [1 :key :Foo]
                                    [2 :key :Bar]
                                    [3 :name "Baz"]])
          clause '(and [?id :name "Foo"]
                       [?id :key :Foo])
          [relation] (where/resolve-clause db [] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 1}}}))))

  (testing "not"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [2 :name "Bar"]
                                    [3 :name "Baz"]])
          relation (r/relation '#{?id} '#{{?id 1} {?id 2} {?id 3}})
          clause '(not [?id :name "Foo"])
          [relation] (where/resolve-clause db [relation] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 2} {?id 3}}})))))

(deftest resolve-clauses-test
  (testing "pattern + predicate"
    (let [db (test-helper/recipe-db)
          clauses '[[?r-id :recipe/name ?r-name]
                    [?r-id :recipe/ingredients ?i-id]
                    [?i-id :ingredient/food ?f-id]
                    [?f-id :food/name ?f-name]
                    [(re-find #"Bread" ?f-name)]]
          relations (where/resolve-clauses db [] clauses)
          relation (r/merge relations r/inner-join)]
      (is (= (:columns relation) '#{?r-id ?r-name ?i-id ?f-id ?f-name}))
      (is (= (map #(get % '?r-name) (:rows relation))
             ["Banana bread sandwhich" "Bread with butter"]))))

  (testing "pattern + not + or"
    (let [db (test-helper/recipe-db)
          clauses '[[?r-id :recipe/name ?r-name]
                    (not (or [?r-id :recipe/name "Bread with butter"]
                             [?r-id :recipe/name "Banana bread sandwhich"]))]
          relations (where/resolve-clauses db [] clauses)
          relation (r/merge relations r/inner-join)]
      (is (= (:columns relation) '#{?r-id ?r-name}))
      (is (= (map #(get % '?r-name) (:rows relation))
             ["Spaghetti with tomato sauce"])))))
