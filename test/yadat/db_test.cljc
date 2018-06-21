(ns yadat.db-test
  (:require [clojure.test :refer :all]
            [yadat.db :as db]
            [yadat.db.minimal]))

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
    (let [db (db/open :minimal {:name [:unique-identity]})
          db (db/insert db [9001 :name "A"])
          transaction {:db db :temp-eids {}}
          [transaction eid] (db/resolve-eid transaction [:name "A"])]
      (is (= eid 9001))
      (is (thrown? Exception (db/resolve-eid transaction [:name "B"])))))

  (testing "unique identity attribute"
    (let [db (db/open :minimal {:name [:unique-identity]})
          db (db/insert db [9001 :name "A"])
          transaction {:db db :temp-eids {}}
          [transaction eid] (db/resolve-eid transaction [[:name "A"]] nil)]
      (is (= eid 9001))
      (is (thrown? Exception
                   (db/resolve-eid transaction [[:name "A"]] 9002))))))

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
      (is (= (set (db/select (:db transaction) [1 :email]))
             #{[1 :email "b@example.com"]}))))

  (testing "reverse reference"
    (let [db (db/open :minimal {:ingredient [:reference]})
          [transaction eid] (db/store-entity {:db db} 1
                                             [[:_ingredient 2]])]
      (is (= (set (db/select (:db transaction) [2 :ingredient]))
             #{[2 :ingredient 1]}))
      (is (thrown? Exception (db/store-entity {:db db} 1 [[:_not-a-ref 2]]))))))


(deftest add-entity-test
  (testing "entity with db/id"
    (let [db (db/open :minimal {:comments [:reference :many]})
          entity {:db/id 2
                  :name "A"
                  :comments [{:text "x"} {:text "y"}]}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= (set (db/select (:db transaction) [2]))
             #{[2 :name "A"] [2 :comments 3] [2 :comments 4]}))))

  (testing "entity with many references"
    (let [db (db/open :minimal {:comments [:reference :many]})
          entity {:db/id 9001 :name "A"
                  :comments [{:text "x"} {:text "y"}]}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= (set (db/select (:db transaction) [9001]))
             #{[9001 :name "A"] [9001 :comments 9002] [9001 :comments 9003]}))))

  (testing "entity with many references (unwrapped lookup-ref)"
    (let [db (db/open :minimal {:comments [:reference :many]
                                :text [:unique-identity]})
          [transaction eid] (db/store-entity {:db db} 42 [[:text "x"]])
          entity {:db/id 9001 :name "A" :comments [:text "x"]}
          [transaction eid] (db/add-entity transaction entity)]
      (is (= (set (db/select (:db transaction) [9001]))
             #{[9001 :name "A"] [9001 :comments 42]}))))

  (testing "entity with one reference"
    (let [db (db/open :minimal {:comments [:reference :one]})
          entity {:db/id 9001 :name "A"
                  :comments {:text "x"}}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= (set (db/select (:db transaction) [9001]))
             #{[9001 :name "A"] [9001 :comments 9002]}))))

  (testing "entity with many (non-reference) values"
    (let [db (db/open :minimal {:comments [:many]})
          entity {:db/id 9001 :name "A"
                  :comments [{:text "x"} {:text "y"}]}
          [transaction eid] (db/add-entity {:db db} entity)]
      (is (= (set (db/select (:db transaction) [9001]))
             #{[9001 :name "A"]
               [9001 :comments {:text "x"}]
               [9001 :comments {:text "y"}]})))))

(deftest transact-test
  ;; TODO
  )
