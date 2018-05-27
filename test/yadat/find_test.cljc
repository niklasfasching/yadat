(ns yadat.find-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [yadat.find :as find]
            [yadat.relation :as r]
            [yadat.test-helper :as test-helper]))

(deftest resolve-spec-test
  (testing "relation"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (find/resolve-spec nil relation '[?id ?name])]
      (is (= result #{[1 "Foo"] [2 "Bar"] [3 "Baz"]}))))

  (testing "scalar"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (find/resolve-spec nil relation '[?name .])]
      (is (= result "Baz"))))

  (testing "collection"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (find/resolve-spec nil relation '[[?name ...]])]
      (is (= result ["Baz" "Bar" "Foo"]))))

  (testing "tuple"
    (let [relation (r/relation '#{?id ?name ?key}
                               '#{{?id 1 ?name "Foo" ?key :Foo}
                                  {?id 2 ?name "Bar" ?key :Bar}
                                  {?id 3 ?name "Baz" ?key :Baz}})
          result (find/resolve-spec nil relation '[[?id ?name]])]
      (is (= result [3 "Baz"])))))

(deftest resolve-element-test
  (testing "variable"
    (let [id (find/resolve-element nil '{?id 1} '?id)]
      (is (= id 1))))

  (testing "pull"
    (let [db (test-helper/db-of {} [[1 :name "Foo"]
                                    [1 :lastname "Bar"]
                                    [1 :email "foo@example.com"]])
          e-* (find/resolve-element db '{?eid 1} '(pull ?eid [*]))
          e-attr (find/resolve-element db '{?eid 1} '(pull ?eid [:name]))]
      (is (= e-* {:db/id 1
                  :name "Foo"
                  :lastname "Bar"
                  :email "foo@example.com"}))
      (is (= e-attr {:db/id 1
                     :name "Foo"}))))

  (testing "aggregate"))

;; (let [db (test-helper/recipe-db)]
;;   (find/q db '{:find [?id]
;;                :where [[?id :recipe/name ?name]]}))
