(ns yadat.relation-test
  (:require  [clojure.test :refer :all]
             [yadat.relation :as r]))

(deftest intersect:-test
  (let [r1 (r/relation #{:a :b} nil)
        r2 (r/relation #{:b :c} nil)
        r3 (r/relation #{:d :e} nil)]
    (is (= (r/intersect? r1 r2) true))
    (is (= (r/intersect? r2 r3) false))
    (is (= (r/intersect? r1 r3) false))))

(deftest cross-join-test
  (let [r1 (r/relation #{:a} #{{:a 1} {:a 2}})
        r2 (r/relation #{:b} #{{:b 1} {:b 2}})
        {:keys [columns rows] :as r-out} (r/cross-join r1 r2)]
    (is (= columns #{:a :b}))
    (is (= rows #{{:a 1 :b 1}
                  {:a 1 :b 2}
                  {:a 2 :b 1}
                  {:a 2 :b 2}}))))

(deftest inner-join-test
  (let [r1 (r/relation #{:a :b} #{{:a 1 :b 1} {:a 2 :b 2}})
        r2 (r/relation #{:b :c} #{{:b 1 :c 1} {:b 2 :c 2}})
        {:keys [columns rows] :as r-out} (r/inner-join r1 r2)]
    (is (= columns #{:a :b :c}))
    (is (= rows #{{:a 1 :b 1 :c 1}
                  {:a 2 :b 2 :c 2}}))))

(deftest union-test
  (let [r1 (r/relation #{:a :b} #{{:a 1 :b 1} {:a 2 :b 2}})
        r2 (r/relation #{:a :b} #{{:a 3 :b 3} {:a 4 :b 4}})
        {:keys [columns rows] :as r-out} (r/union r1 r2)]
    (is (= columns '#{:a :b}))
    (is (= rows '#{{:a 1 :b 1}
                   {:a 2 :b 2}
                   {:a 3 :b 3}
                   {:a 4 :b 4}}))))

(deftest merge-test
  (let [r1 (r/relation #{:a :b} #{{:a 1 :b 1} {:a 2 :b 2}})
        r2 (r/relation #{:b :c} #{{:b 1 :c 1} {:b 2 :c 2}})
        r3 (r/relation #{:c :d} #{{:c 1 :d 1} {:c 2 :d 2}})
        {:keys [columns rows] :as r-out} (r/merge [r1 r2 r3] r/inner-join)]
    (is (= columns #{:a :b :c :d}))
    (is (= rows #{{:a 1 :b 1 :c 1 :d 1}
                  {:a 2 :b 2 :c 2 :d 2}}))))

(deftest split-test
  (let [r1 (r/relation #{:a :b} #{{:a 1 :b 1} {:a 2 :b 2}})
        r2 (r/relation #{:b :c} #{{:b 1 :c 1} {:b 2 :c 2}})
        r3 (r/relation #{:c :d} #{{:c 1 :d 1} {:c 2 :d 2}})
        [rs r] (r/split [r1 r2 r3] '#{:b})]
    (is (= rs [r3]))
    (is (= (:columns r) #{:a :b :c}))
    (is (= (:rows r) #{{:a 1 :b 1 :c 1}
                       {:a 2 :b 2 :c 2}}))))
