(ns yadat.db.minimal-test
  (:require [clojure.test :refer :all]
            [yadat.db :as db]
            [yadat.db.minimal :as mdb]))

(deftest insert-test
  (let [db (-> (db/open :minimal {})
               (db/insert [1 :name "A"])
               (db/insert [2 :name "B"]))]
    (is (= (set (db/select db [1 nil nil])) #{[1 :name "A"]}))
    (is (= (set (db/select db [2 nil nil])) #{[2 :name "B"]}))))

(deftest delete-test
  (let [db (-> (db/open :minimal {})
               (db/insert [1 :name "A"])
               (db/insert [2 :name "B"]))]
    (is (= (set (db/select db [1 nil nil])) #{[1 :name "A"]}))
    (is (= (set (db/select db [2 nil nil])) #{[2 :name "B"]}))))

(deftest select-test
  ;; TODO
  )
