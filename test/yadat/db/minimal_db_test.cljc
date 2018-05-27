(ns yadat.db.minimal-db-test
  (:require [clojure.test :refer :all]
            [yadat.db :as db]
            [yadat.db.minimal-db :as mdb]))

(deftest insert-test
  (let [db (-> (db/open :minimal {})
               (db/insert [1 :name "Hans"])
               (db/insert [2 :name "Heidi"]))]
    (is (= (set (db/select db [1 nil nil]))
           #{[1 :name "Hans"]}))
    (is (= (set (db/select db [2 nil nil]))
           #{[2 :name "Heidi"]}))))

(deftest delete-test
  (let [db (-> (db/open :minimal {})
               (db/insert [1 :name "Hans"])
               (db/insert [2 :name "Heidi"]))]
    (is (= (set (db/select db [1 nil nil]))
           #{[1 :name "Hans"]}))
    (is (= (set (db/select db [2 nil nil]))
           #{[2 :name "Heidi"]})))
  )

(deftest select-test
  )
