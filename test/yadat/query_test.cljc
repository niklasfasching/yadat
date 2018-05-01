(ns yadat.query-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [yadat.query :as query]
            [yadat.test-helper :as test-helper]
            [yadat.util :as util]
            [yadat.relation :as r]))

(deftest resolve-where-clause-test
  (testing "pattern"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [2 :name "Bar"]
                                    [3 :name "Baz"]])
          clause1 '[?id :name "Foo"]
          clause2 '[?id _ "Foo"]
          [r1] (query/resolve-where-clause db [] clause1)
          [r2] (query/resolve-where-clause db [] clause2)]
      (is (= (select-keys r1 [:columns :rows])
             (select-keys r2 [:columns :rows])))
      (is (= (select-keys r1 [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 1}}}))))

  (testing "predicate"
    (let [relation (r/relation '#{?id ?name} '#{{?id 1 ?name "Foo"}
                                                {?id 2 ?name "Bar"}
                                                {?id 3 ?name "Baz"}})
          clause '[(re-find #"Bar|Baz" ?name)]
          [relation] (query/resolve-where-clause nil [relation] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id ?name}
              :rows '#{{?id 2 ?name "Bar"}
                       {?id 3 ?name "Baz"}}}))))

  (testing "function"
    (let [relation (r/relation '#{?id ?name} '#{{?id 1 ?name "Foo"}
                                                {?id 2 ?name "Bar"}})
          clause '[(clojure.string/upper-case ?name) ?upper-name]
          [relation] (query/resolve-where-clause nil [relation] clause)]
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
          [relation] (query/resolve-where-clause db [] clause)]
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
          [relation] (query/resolve-where-clause db [] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 1}}}))))

  (testing "not"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [2 :name "Bar"]
                                    [3 :name "Baz"]])
          relation (r/relation '#{?id} '#{{?id 1} {?id 2} {?id 3}})
          clause '(not [?id :name "Foo"])
          [relation] (query/resolve-where-clause db [relation] clause)]
      (is (= (select-keys relation [:columns :rows])
             {:columns '#{?id} :rows '#{{?id 2} {?id 3}}})))))

(deftest resolve-where-clauses-test
  (testing "pattern + predicate"
    (let [db (test-helper/recipe-db)
          clauses '[[?r-id :recipe/name ?r-name]
                    [?r-id :recipe/ingredients ?i-id]
                    [?i-id :ingredient/food ?f-id]
                    [?f-id :food/name ?f-name]
                    [(re-find #"Bread" ?f-name)]]
          relations (query/resolve-where-clauses db [] clauses)
          relation (r/merge relations r/inner-join)]
      (is (= (:columns relation) '#{?r-id ?r-name ?i-id ?f-id ?f-name}))
      (is (= (map #(get % '?r-name) (:rows relation))
             ["Banana bread sandwhich" "Bread with butter"]))))

  (testing "pattern + not + or"
    (let [db (test-helper/recipe-db)
          clauses '[[?r-id :recipe/name ?r-name]
                    (not (or [?r-id :recipe/name "Bread with butter"]
                             [?r-id :recipe/name "Banana bread sandwhich"]))]
          relations (query/resolve-where-clauses db [] clauses)
          relation (r/merge relations r/inner-join)]
      (is (= (:columns relation) '#{?r-id ?r-name}))
      (is (= (map #(get % '?r-name) (:rows relation))
             ["Spaghetti with tomato sauce"])))))

(deftest resolve-find-test
  (testing "relation"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (query/resolve-find-spec nil relation '[?id ?name])]
      (is (= result #{[1 "Foo"] [2 "Bar"] [3 "Baz"]}))))

  (testing "scalar"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (query/resolve-find-spec nil relation '[?name .])]
      (is (= result "Baz"))))

  (testing "collection"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (query/resolve-find-spec nil relation '[[?name ...]])]
      (is (= result ["Baz" "Bar" "Foo"]))))

  (testing "tuple"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (query/resolve-find-spec nil relation '[[?id ?name]])]
      (is (= result [3 "Baz"])))))


(deftest resolve-find-element-test
  (testing "variable"
    (let [id (query/resolve-find-element nil '{?id 1} '?id)]
      (is (= id 1))))

  (testing "pull"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [1 :lastname "Bar"]
                                    [1 :email "foo@example.com"]])
          e-* (query/resolve-find-element db '{?eid 1} '(pull ?eid [*]))
          e-attr (query/resolve-find-element db '{?eid 1} '(pull ?eid [:name]))]
      (is (= e-* {:db/id 1
                  :name "Foo"
                  :lastname "Bar"
                  :email "foo@example.com"}))
      (is (= e-attr {:db/id 1
                     :name "Foo"}))))

  (testing "aggregate"))
