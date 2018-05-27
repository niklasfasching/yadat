(ns yadat.db-test
  (:require [clojure.test :refer :all]
            [yadat.test-helper :as test-helper]
            [yadat.db :as db]
            [yadat.db.minimal-db]))

(deftest resolve-eid-test
  (testing "real eid"
    (let [transaction {:db (db/open :minimal {}) :temp-eids {}}
          [transaction eid] (db/resolve-eid transaction 1)]
      (is (= eid 1))))

  (testing "temp eid"
    (let [transaction {:db (db/open :minimal {}) :temp-eids {}}
          [transaction eid1a] (db/resolve-eid transaction -1)
          [transaction eid2a] (db/resolve-eid transaction -2)
          [transaction eid1b] (db/resolve-eid transaction -1)]
      (is (= (:temp-eids transaction) {-1 1 -2 2}))
      (is (= eid1a 1))
      (is (= eid2a 2))
      (is (= eid1a eid1b))))

  (testing "lookup ref"
    (let [db (db/open :minimal {:user/name [:unique-identity]})
          db (db/insert db [9001 :user/name "Hans"])
          transaction {:db db :temp-eids {}}
          [transaction eid] (db/resolve-eid transaction [:user/name "Hans"])]
      (is (= eid 9001))
      (is (thrown? Exception
                   (db/resolve-eid transaction [:user/name "Heidi"])))))

  (testing "unique identity attribute"
    (let [db (db/open :minimal {:user/name [:unique-identity]})
          db (db/insert db [9001 :user/name "Hans"])
          transaction {:db db :temp-eids {}}
          [transaction eid] (db/resolve-eid transaction [[:user/name "Hans"]]
                                            nil)]
      (is (= eid 9001))
      (is (thrown? Exception
                   (db/resolve-eid transaction [:user/name "Hans"] 9002))))))

(deftest store-entity-test
  (testing "cardinality many"
    (let [db (db/open :minimal {:email [:many]})
          [transaction eid] (db/store-entity {:db db} 1
                                             [[:email "a@example.com"]
                                              [:email "b@example.com"]])]
      (is (= (set (db/select (:db transaction) [1 :email]))
             #{[1 :email "a@example.com"]
               [1 :email "b@example.com"]}))))

  (testing "cardinality one"
    (let [db (db/open :minimal {:email [:one]})
          [transaction eid] (db/store-entity {:db db} 1
                                             [[:email "a@example.com"]
                                              [:email "b@example.com"]])]
      (is (= #{[1 :email "b@example.com"]}
             (set (db/select (:db transaction) [1 :email]))))))

  (testing "reverse reference"
    (let [db (db/open :minimal {:recipe/ingredient [:reference]})
          [transaction eid] (db/store-entity {:db db} 1
                                             [[:recipe/_ingredient 2]])]
      (is (= (set (db/select (:db transaction) [2 :recipe/ingredient]))
             #{[2 :recipe/ingredient 1]}))
      (is (thrown? Exception
                   (db/store-entity {:db db} 1 [[:recipe/_not-a-ref 2]]))))))


(deftest add-entity-test
  (testing "entity with db/id"
    (let [db (db/open :minimal {:user/comments [:reference :many]})
          entity {:db/id 2
                  :user/name "Hans"
                  :user/comments [{:comment/text "A"}
                                  {:comment/text "B"}]}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= #{[2 :user/name "Hans"]
               [2 :user/comments 3]
               [2 :user/comments 4]}
             (set (db/select (:db transaction) [2]))))))

  (testing "entity with many references"
    (let [db (db/open :minimal {:user/comments [:reference :many]})
          entity {:db/id 9001
                  :user/name "Hans"
                  :user/comments [{:comment/text "A"}
                                  {:comment/text "B"}]}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= #{[9001 :user/name "Hans"]
               [9001 :user/comments 9002]
               [9001 :user/comments 9003]}
             (set (db/select (:db transaction) [9001]))))))

  (testing "entity with many references (unwrapped lookup-ref)"
    (let [db (db/open :minimal {:user/comments [:reference :many]
                                   :comment/text [:unique-identity]})
          [transaction eid] (db/store-entity {:db db} 42 [[:comment/text "A"]])
          entity {:db/id 9001
                  :user/name "Hans"
                  :user/comments [:comment/text "A"]}
          [transaction eid] (db/add-entity transaction entity)]
      (is (= #{[9001 :user/name "Hans"]
               [9001 :user/comments 42]}
             (set (db/select (:db transaction) [9001]))))))

  (testing "entity with one reference"
    (let [db (db/open :minimal {:user/comments [:reference :one]})
          entity {:db/id 9001
                  :user/name "Hans"
                  :user/comments {:comment/text "A"}}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= #{[9001 :user/name "Hans"]
               [9001 :user/comments 9002]}
             (set (db/select (:db transaction) [9001]))))))

  (testing "entity with many (non-reference) values"
    (let [db (db/open :minimal {:user/comments [:many]})
          entity {:db/id 9001
                  :user/name "Hans"
                  :user/comments [{:comment/text "A"}
                                  {:comment/text "B"}]}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= #{[9001 :user/name "Hans"]
               [9001 :user/comments {:comment/text "A"}]
               [9001 :user/comments {:comment/text "B"}]}
             (set (db/select (:db transaction) [9001])))))))

(deftest transact-test
  (let [db (test-helper/recipe-db)]
    (is (= 3 (count (db/select db [nil :author/name nil]))))
    (is (= 3 (count (db/select db [nil :recipe/name nil]))))
    (is (= 5 (count (db/select db [nil :food/name nil]))))
    (is (= 6 (count (db/select db [nil :ingredient/food nil]))))))
