(ns yadat.schema-test
  (:require #?(:clj [clojure.test :refer [deftest testing are is]]
               :cljs [cljs.test :refer-macros [deftest testing are is]])
            [yadat.schema :as schema]))

(deftest reversed-ref-test
  (is (= (schema/reversed-ref :_a) :a))
  (is (= (schema/reversed-ref :a) :_a))
  (is (= (schema/reversed-ref :ns/_a) :ns/a))
  (is (= (schema/reversed-ref :ns/a) :ns/_a)))


(deftest is?-test
  (let [datomish (schema/create
                  {:schema/type :datomish
                   :name {:db/unique :db.unique/identity}
                   :uuid {:db/unique :db.unique/value}
                   :comments {:db/valueType :db.type/ref
                              :db/cardinality :db.cardinality/many}
                   :website {:db/isComponent true
                             :db/valueType :db.type/ref}})
        minimal (schema/create
                 {:schema/type :minimal
                  :name [:unique-identity]
                  :uuid [:unique-value]
                  :comments [:reference :many]
                  :website [:component :reference]})
        check (fn [a x expected]
                (is (= (schema/is? datomish a x) expected)
                    (pr-str {:schema :datomish :a a :x x}))
                (is (= (schema/is? minimal a x) expected)
                    (pr-str {:schema :minimal :a a :x x})))]

    (check :name :unique-identity true)
    (check :name :unique-value false)
    (check :uuid :unique-value true)
    (check :comments :reference true)
    (check :website :component true)
    (check :website :many false)
    (check :comments :many true)))


(deftest validate-test
  (testing "many attributes must not be unique"
    (is (thrown? #?(:cljs js/Error :clj Exception)
                 (schema/create
                  {:schema/type :minimal
                   :name [:unique-identity :many]}))))

  (testing "component attributes must be references"
    (is (thrown? #?(:cljs js/Error :clj Exception)
                 (schema/create
                  {:schema/type :minimal
                   :name [:component]})))))
