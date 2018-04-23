(ns yadat.pull-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yadat.core :as core]
            [yadat.dsl :as dsl]
            [yadat.pull :as pull]))

(def db @(core/open :minimal {:schema/type :minimal
                              :comments [:reference :many]
                              :websites [:component :reference :many]}
                    [{:db/id 11
                      :name "A"
                      :comments [{:db/id 12 :text "a"}
                                 {:db/id 13 :text "b"}]
                      :websites [{:db/id 14 :url "a.org"}]}
                     {:db/id 21
                      :name "B"
                      :comments [{:db/id 22 :text "c"}]}]))

(deftest pull-element-test
  (let [check (fn [id form expected]
                (is (= (pull/resolve-pull-pattern
                        (dsl/parse-pull-pattern form)
                        {'$ db} id)
                       expected)))]

    (testing "wildcard"
      (check 11 '[*]
             {:db/id 11
              :name "A"
              :comments [{:db/id 12} {:db/id 13}]
              :websites [{:db/id 14, :url "a.org"}]}))

    (testing "attribute"
      (check 11 [:name]
             {:db/id 11 :name "A"})

      (testing "reverse attribute many returns collection"
        (check 12 [:_comments]
               {:db/id 12 :_comments [{:db/id 11}]}))

      (testing "reverse attribute many component returns entity"
        (check 14 [:_websites]
               {:db/id 14 :_websites {:db/id 11}}))

      (check 11 [:not-a-known-attribute]
             {:db/id 11}))

    (testing "attribute with options"
      (check 11 '[(:not-a-known-attribute :default "x")]
             {:db/id 11 :not-a-known-attribute "x"})

      (check 11 '[(:comments :limit 1)]
             {:db/id 11 :comments [{:db/id 12}]})

      (check 11 '[(:comments :limit 0)]
             {:db/id 11})

      (with-redefs [pull/default-pull-limit 1]
        (check 11 '[(:comments :limit nil)]
               {:db/id 11 :comments [{:db/id 12} {:db/id 13}]})))


    (testing "map"
      (check 14 '[{:_websites [:websites :comments]}]
             {:db/id 14,
              :_websites {:db/id 11
                          :comments [{:db/id 12} {:db/id 13}]
                          :websites [{:db/id 14}]}})

      (check 14 '[{(:_websites :as :user) [:name]}]
             {:db/id 14,
              :user {:db/id 11 :name "A"}})

      (check 9001 '[:_websites]
             {:db/id 9001}))))

(comment
  (pull/resolve-pull-pattern
   (dsl/parse-pull-pattern '[{:_websites [*]}])
   {'$ db} 24)

  (pull/resolve-pull-element
   (dsl/parse-pull-element :_websites)
   {'$ db} {:db/id 14}))
