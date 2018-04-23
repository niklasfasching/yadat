(ns yadat.query-test
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer-macros [deftest testing is]])
            [yadat.core :as core]
            [yadat.dsl :as dsl]
            [yadat.query :as query]
            [yadat.relation :as r]))

(defn check [parse resolve args form expected]
  (let [result (apply resolve (concat [(parse form)] args))]
    (is (= result expected))))

(deftest resolve-find-spec-test
  (let [rows '[{?x 1 ?y 1 ?z 1}
               {?x 2 ?y 2 ?z 2}
               {?x 3 ?y 3 ?z 3}]
        check (partial check dsl/parse-find-spec
                       query/resolve-find-spec [{} rows])]

    (testing "relation"
      (check '[?x ?y]
             [[1 1]
              [2 2]
              [3 3]]))

    (testing "scalar"
      (check '[?x .]
             1))

    (testing "collection"
      (check '[[?x ...]]
             [1 2 3]))

    (testing "tuple"
      (check '[[?x ?y]]
             [1 1]))

    (testing "aggregate"
      (check '[(count ?x)]
             [[3]]))))


(deftest resolve-find-element-test
  (let [db (-> (core/open :minimal {})
               (core/transact [{:db/id 1 :x "A" :y "a"}]))
        row '{?x 1 ?y 1 ?z 1}
        check (partial check dsl/parse-find-element
                       query/resolve-find-element [{'$ db} row])]

    (testing "variable"
      (check '?x 1))

    (testing "pull"
      (check '(pull $ ?x [:x]) {:db/id 1 :x "A"})

      (binding [query/*default-source* db]
        (check '(pull ?x [:x]) {:db/id 1 :x "A"})))

    (testing "aggregate"
      (check '(count ?x) 1))))


(deftest resolve-clause-test
  (let [db (-> (core/open :minimal {:schema/type :minimal
                                    :z [:reference :unique-identity]})
               (core/transact [{:db/id 1 :x "A" :y "a" :z 1}
                               {:db/id 2 :x "B" :y "b" :z 2}
                               {:db/id 3 :x "C" :y "c" :z 3}]))
        relations [(r/relation '#{?x ?y}
                               '#{{?x "D" ?y "d"}
                                  {?x "E" ?y "e"}
                                  {?x "F" ?y "f"}})]
        cmp (fn [result expected]
              (is (= (map #(select-keys % [:columns :rows]) result)
                     (map #(select-keys % [:columns :rows]) expected))))
        resolve (fn [form relations]
                  (-> (dsl/parse-clause form)
                      (query/resolve-clause {'$ db} relations)))]

    (testing "pattern"
      (cmp (resolve '[$ _ :x ?v] [])
           [(r/relation '#{?v} '#{{?v "A"}
                                  {?v "B"}
                                  {?v "C"}})])

      (cmp (resolve '[$ ?eid :x ?v] [])
           [(r/relation '#{?eid ?v} '#{{?eid 1 ?v "A"}
                                       {?eid 2 ?v "B"}
                                       {?eid 3 ?v "C"}})])

      (cmp (resolve '[$ [:z 1] :x ?v] [])
           [(r/relation '#{?v} '#{{?v "A"}})])

      (cmp (resolve '[$ ?eid :z [:z 1]] [])
           [(r/relation '#{?eid} '#{{?eid 1}})]))
    (testing "predicate"
      (cmp (resolve '[(re-find #"D|E" ?x)] relations)
           [(r/relation '#{?x ?y} '#{{?x "E" ?y "e"}
                                     {?x "D" ?y "d"}})]))

    (testing "function"
      (cmp (resolve '[(clojure.string/upper-case ?y) ?Y] relations)
           [(r/relation '#{?x ?y ?Y} '#{{?x "D" ?y "d" ?Y "D"}
                                        {?x "E" ?y "e" ?Y "E"}
                                        {?x "F" ?y "f" ?Y "F"}})]))

    (testing "or"
      (cmp (resolve '(or $
                         [?id :x "A"]
                         [?id :x "B"]) [])
           [(r/relation '#{?id} '#{{?id 1}
                                   {?id 2}})])

      (binding [query/*default-source* db]
        (cmp (resolve '(or [?id :x "A"]
                           [?id :x "B"]) [])
             [(r/relation '#{?id} '#{{?id 1}
                                     {?id 2}})])))

    (testing "and"
      (cmp (resolve '(and $
                          [?id :x ?v]
                          [(re-find #"A" ?v)]) [])
           [(r/relation '#{?id ?v} '#{{?id 1 ?v "A"}})])

      (binding [query/*default-source* db]
        (cmp (resolve '(and [?id :x ?v]
                            [(re-find #"A" ?v)]) [])
             [(r/relation '#{?id ?v} '#{{?id 1 ?v "A"}})])))

    (testing "not"
      (cmp (resolve '(not $ (or [(re-find #"D" ?x)]
                                [(re-find #"E" ?x)])) relations)
           [(r/relation '#{?x ?y} '#{{?x "F" ?y "f"}})])

      (binding [query/*default-source* db]
        (cmp (resolve '(not (or [(re-find #"D" ?x)]
                                [(re-find #"E" ?x)])) relations)
             [(r/relation '#{?x ?y} '#{{?x "F" ?y "f"}})])

        (cmp (resolve '(not [(re-find #"E" ?x)]
                            [(re-find #"e" ?y)]) relations)
             [(r/relation '#{?x ?y} '#{{?x "D" ?y "d"}
                                       {?x "F" ?y "f"}})])))))



(let [db (-> (core/open :minimal {:schema/type :minimal
                                  :x []
                                  :z [:unique-value :reference]})
               (core/transact [{:db/id 1 :x "A" :y "a" :z 1}
                               {:db/id 2 :x "B" :y "b" :z 2}
                               {:db/id 3 :x "C" :y "c" :z 3}]))
        relations [(r/relation '#{?x ?y}
                               '#{{?x "D" ?y "d"}
                                  {?x "E" ?y "e"}
                                  {?x "F" ?y "f"}})]
        cmp (fn [result expected]
              (is (= (map #(select-keys % [:columns :rows]) result)
                     (map #(select-keys % [:columns :rows]) expected))))
        resolve (fn [form relations]
                  (-> (dsl/parse-clause form)
                      (query/resolve-clause {'$ db} relations)))]




  (resolve '[$ ?eid :z [:z 2]] [])

  )
