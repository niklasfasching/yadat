(ns yadat.pull-test
  (:require [yadat.pull :as pull]
            [clojure.test :refer :all]
            [yadat.test-helper :as test-helper]))

(deftest resolve-attribute-spec-test
  (testing "wildcard"
    (let [db (test-helper/db-of {:comments [:reference :many]
                                 :address [:reference]}
                                [[1 :name "Foo"]
                                 [1 :lastname "Bar"]
                                 [1 :email "foo@example.com"]
                                 [1 :comments 2]
                                 [1 :comments 3]
                                 [1 :address 4]])
          entity (pull/resolve-attribute-spec db {:db/id 1} '*)]
      (is (= entity {:db/id 1
                     :name "Foo"
                     :lastname "Bar"
                     :email "foo@example.com"
                     :address {:db/id 4}
                     :comments #{{:db/id 2} {:db/id 3}}}))))

  (testing "attribute"
    (let [db (test-helper/db-of {:comments [:reference :many]
                                 :address [:reference]}
                                [[1 :name "Foo"]
                                 [1 :lastname "Bar"]
                                 [1 :email "foo@example.com"]
                                 [1 :comments 2]
                                 [1 :comments 3]
                                 [1 :address 4]])
          entity-name (pull/resolve-attribute-spec db {:db/id 1} :name)
          entity-comments (pull/resolve-attribute-spec db {:db/id 1} :comments)]
      (is (= entity-name {:db/id 1
                          :name "Foo"}))
      (is (= entity-comments {:db/id 1
                              :comments #{{:db/id 2} {:db/id 3}}})))))

(let [db (test-helper/db-of {:comments [:reference :many]
                             :address [:reference]}
                            [[1 :name "Foo"]
                             [1 :lastname "Bar"]
                             [1 :email "foo@example.com"]
                             [1 :comments 2]
                             [1 :comments 3]
                             [3 :content "comment 3"]
                             [2 :content "comment 2"]
                             [1 :address 4]])
      entities (pull/resolve-attribute-spec db {:db/id 1} '{:comments [*]})
      ]
  entities)
