(ns yadat.db.sorted-set-test
  (:require [yadat.db :as db]
            [yadat.db.sorted-set :as ssdb]
            [clojure.test :refer :all]))

(deftest make-datom-comparator-test
  (let [datom-1c3 [1 :c 3]
        datom-2b2 [2 :b 2]
        datom-3a1 [3 :a 1]
        cmp-eav (ssdb/make-datom-comparator compare 0 1 2)
        cmp-aev (ssdb/make-datom-comparator compare 1 0 2)
        cmp-ave (ssdb/make-datom-comparator compare 1 2 0)]

    (testing "eav"
      (is (neg? (cmp-eav datom-1c3 datom-2b2)))
      (is (neg? (cmp-eav datom-1c3 datom-3a1)))
      (is (neg? (cmp-eav datom-2b2 datom-3a1))))

    (testing "aev"
      (is (pos? (cmp-aev datom-1c3 datom-2b2)))
      (is (pos? (cmp-aev datom-1c3 datom-3a1)))
      (is (pos? (cmp-aev datom-2b2 datom-3a1))))

    (testing "ave"
      (is (pos? (cmp-ave datom-1c3 datom-2b2)))
      (is (pos? (cmp-ave datom-1c3 datom-3a1)))
      (is (pos? (cmp-ave datom-2b2 datom-3a1))))))

(deftest insert-test
  ;; TODO
  )

(deftest delete-test
  (let [datom1 [1 :name "foo"]
        datom2 [1 :email "foo@example.com"]
        db (-> (db/open :sorted-set {})
               (db/insert datom1)
               (db/insert datom2))
        db (db/delete db datom1)]
    (is (= (db/select db datom1) []))
    (is (= (db/select db datom2) [datom2]))))
