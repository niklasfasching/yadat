(ns yadat.query-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [yadat.query :as query]
            [yadat.test-helper :as test-helper]
            [yadat.util :as util]
            [yadat.relation :as r]))

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

(let [db (test-helper/recipe-db)]
  (query/q db '{:find [[(sum ?id) ...]]
                :where [[?id :recipe/name ?name]]}))
