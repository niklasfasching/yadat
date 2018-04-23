(ns yadat.db-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yadat.db :as db]
            [yadat.db.minimal :as mdb]
            [yadat.db.sorted-set :as ssdb]
            [yadat.db.bt-set :as btdb]
            [yadat.util :as util]
            [yadat.schema :as schema]))

(defn create-transaction
  ([schema]
   (let [schema (schema/create (assoc schema :schema/type :minimal))
         db (db/create :minimal schema)]
     (db/transact db [])))
  ([schema datoms]
   (reduce
    (fn [transaction [eid a v]] (db/add-datom transaction eid a v))
    (create-transaction schema) datoms)))

(deftest resolve-eid-test
  (testing "real eid"
    (let [transaction (create-transaction {})
          [transaction eid] (db/resolve-eid transaction 1)]
      (is (= eid 1))))

  (testing "temp eid"
    (let [transaction (create-transaction {})
          [transaction eid1a] (db/resolve-eid transaction -1)
          [transaction eid2a] (db/resolve-eid transaction -2)
          [transaction eid1b] (db/resolve-eid transaction -1)]
      (is (= (:temp-eids transaction) {-1 1 -2 2}))
      (is (= eid1a 1))
      (is (= eid2a 2))
      (is (= eid1a eid1b))))

  (testing "lookup ref"
    (let [transaction (create-transaction {:name [:unique-identity]}
                                          [[9001 :name "A"]])
          [transaction eid] (db/resolve-eid transaction [:name "A"])]
      (is (= eid 9001))
      (is (thrown? #?(:cljs js/Error :clj Exception)
                   (db/resolve-eid transaction [:name "B"])))))

  (testing "unique identity attribute"
    (let [transaction (create-transaction {:name [:unique-identity]}
                                          [[9001 :name "A"]])
          [transaction eid] (db/resolve-eid transaction nil [[:name "A"]])]
      (is (= eid 9001))
      (is (thrown? #?(:cljs js/Error :clj Exception)
                   (db/resolve-eid transaction [[:name "A"]] 42))))))


(deftest store-entity-test
  (testing "cardinality many"
    (let [transaction (create-transaction {:email [:many]})
          [{:keys [db-after]} eid] (db/store-entity transaction 1
                                                    [[:email "a@example.com"]
                                                     [:email "b@example.com"]])]
      (is (= eid 1))
      (is (= (set (db/select db-after (db/datom 1 :email nil)))
             #{(db/datom 1 :email "a@example.com")
               (db/datom 1 :email "b@example.com")}))))

  (testing "cardinality one"
    (let [transaction (create-transaction {:email []})
          [{:keys [db-after]} eid] (db/store-entity transaction 1
                                                    [[:email "a@example.com"]
                                                     [:email "b@example.com"]])]
      (is (= eid 1))
      (is (= (set (db/select db-after (db/datom 1 :email nil)))
             #{(db/datom 1 :email "b@example.com")}))))

  (testing "reverse reference"
    (let [transaction (create-transaction {:x [:reference]})
          [{:keys [db-after]} eid] (db/store-entity transaction 1 [[:_x 2]])]
      (is (= eid 1))
      (is (= (set (db/select db-after (db/datom 2 :x nil)))
             #{(db/datom 2 :x 1)})))))


(deftest add-entity-test
  (testing "entity with db/id"
    (let [transaction (create-transaction {:comments [:many :reference]})
          entity {:db/id 2
                  :name "A"
                  :comments [{:text "x"} {:text "y"}]}
          [{:keys [db-after]} eid] (db/add-entity transaction entity)]
      (is (= eid 2))
      (is (= (set (db/select db-after (db/datom 2 nil nil)))
             #{(db/datom 2 :name "A")
               (db/datom 2 :comments 3)
               (db/datom 2 :comments 4)}))))

  (testing "entity with many references"
    (let [transaction (create-transaction {:comments [:many :reference]})
          entity {:db/id 9001 :name "A"
                  :comments [{:text "x"} {:text "y"}]}
          [{:keys [db-after]} eid] (db/add-entity transaction entity)]
      (is (= eid 9001))
      (is (= (set (db/select db-after (db/datom 9001 nil nil)))
             #{(db/datom 9001 :name "A")
               (db/datom 9001 :comments 9002)
               (db/datom 9001 :comments 9003)}))))

  (testing "entity with many references (unwrapped lookup-ref)"
    (let [transaction (create-transaction {:comments [:many :reference]
                                           :text [:unique-value]})
          [transaction eid1] (db/store-entity transaction 42 [[:text "x"]])
          entity {:db/id 9001 :name "A" :comments [:text "x"]}
          [{:keys [db-after]} eid2] (db/add-entity transaction entity)]
      (is (= eid1 42))
      (is (= eid2 9001))
      (is (= (set (db/select db-after (db/datom 9001 nil nil)))
             #{(db/datom 9001 :name "A")
               (db/datom 9001 :comments 42)}))))

  (testing "entity with one reference"
    (let [transaction (create-transaction {:comment [:reference]})
          entity {:db/id 9001 :name "A"
                  :comment {:text "x"}}
          [{:keys [db-after]} eid] (db/add-entity transaction entity)]
      (is (= eid 9001))
      (is (= (set (db/select db-after (db/datom 9001 nil nil)))
             #{(db/datom 9001 :name "A")
               (db/datom 9001 :comment 9002)}))))

  (testing "entity with many (non-reference) values"
    ;; this is only possible with non sorted-set based dbs as maps cannot
    ;; because compared. (1 :a {:x 1}) (1 :a {:x 2}) thus throws
    (let [transaction (create-transaction {:comments [:many]})
          entity {:db/id 9001 :name "A"
                  :comments [{:text "x"} {:text "y"}]}
          [{:keys [db-after]} eid] (db/add-entity transaction entity)]
      (is (= eid 9001))
      (is (= (set (db/select db-after (db/datom 9001 nil nil)))
             #{(db/datom 9001 :name "A")
               (db/datom 9001 :comments {:text "x"})
               (db/datom 9001 :comments {:text "y"})})))))


(deftest transact-test
  (testing "lookup-ref -> eid"
    (let [schema (schema/create {:schema/type :minimal
                                 :name [:unique-identity]})
          datoms (-> (db/create :minimal schema)
                     (db/transact [{:name "A"} {:name "B"}])
                     :db-after
                     (db/transact [{:db/id [:name "B"]
                                    :email "b@example.com"}])
                     :db-after
                     :set)]
      (is (= datoms
             #{(db/datom 1 :name "A")
               (db/datom 2 :name "B")
               (db/datom 2 :email "b@example.com")}))))

  (testing "temp-eid -> eid"
    (let [datoms (-> (db/create :minimal (schema/create {}))
                     (db/transact [{:db/id -20 :name "A"}
                                   {:db/id -10 :name "B"}
                                   {:db/id -10 :email "b@example.com"}
                                   {:db/id -20 :email "a@example.com"}])
                     :db-after
                     :set)]
      (is (= datoms
             #{(db/datom 1 :name "A")
               (db/datom 2 :name "B")
               (db/datom 1 :email "a@example.com")
               (db/datom 2 :email "b@example.com")})))

    (let [schema (schema/create {:schema/type :minimal
                                 :name [:unique-identity]})
          datoms (-> (db/create :minimal schema)
                     (db/transact [[:db/add  1 :name "A"]
                                   [:db/add -1 :name "B"]
                                   [:db/add -1 :email "b@example.com"]
                                   [:db/add [:name "A"] :email "a@example.com"]
                                   ])
                     :db-after
                     :set)]
      (is (= datoms
             #{(db/datom 1 :name "A")
               (db/datom 2 :name "B")
               (db/datom 1 :email "a@example.com")
               (db/datom 2 :email "b@example.com")})))))


(deftest add-datom-test
  (let [{:keys [db-after]} (-> (create-transaction {:many [:many]})
                               (db/add-datom 1 :one "1")
                               (db/add-datom 1 :one "2")
                               (db/add-datom 1 :many "1")
                               (db/add-datom 1 :many "2"))]
    (is (= (set (db/select db-after (db/datom 1 nil nil)))
           #{(db/datom 1 :one "2")
             (db/datom 1 :many "1")
             (db/datom 1 :many "2")}))))


(deftest retract-datom-test
  (let [{:keys [db-after]} (-> (create-transaction {:many [:many]})
                               (db/add-datom 1 :one "1")
                               (db/add-datom 1 :one "2")
                               (db/add-datom 1 :many "1")
                               (db/add-datom 1 :many "2")
                               (db/retract-datom 1 :one "2")
                               (db/retract-datom 1 :many "1"))]
    (is (= (set (db/select db-after (db/datom 1 nil nil)))
           #{(db/datom 1 :many "2")}))))


(deftest serialize-test
  (let [entities [{:name "A" :email "a@example.com"}
                  {:name "B" :email "b@example.com"}]
        schemas [{:schema/type :minimal
                  :name [:unique-identity]}
                 {:schema/type :datomish
                  :name {:db/unique  :db.unique/identity}}]
        db-types [:minimal :sorted-set]]
    (doall
     (for [db-type db-types
           schema schemas]
       (testing (str "db: " db-type ", schema: " (:schema/type schema))
         (let [schema (schema/create schema)
               {db :db-after} (db/transact (db/create db-type schema) entities)]
           (is (= (util/read-string (db/serialize db))
                  db))))))))


(deftest make-datom-comparator-test
  (let [datom-1c3 (db/datom 1 :c 3)
        datom-2b2 (db/datom 2 :b 2)
        datom-3a1 (db/datom 3 :a 1)
        cmp-eav (db/datom-comparator compare e a v)
        cmp-aev (db/datom-comparator compare a e v)
        cmp-ave (db/datom-comparator compare a v e)]

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
