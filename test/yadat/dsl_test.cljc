(ns yadat.dsl-test
  (:require #?(:clj [clojure.test :refer [deftest testing are is]]
               :cljs [cljs.test :refer-macros [deftest testing are is]])
            [yadat.dsl :as dsl]))

(deftest parse-in-test
  (are [form result] (= (dsl/parse-input form) result)
    '$a
    (dsl/->InputSource '$a)

    '%
    (dsl/->InputRules '%)

    '?x
    (dsl/->InputScalar '?x)

    '[?x ?y]
    (dsl/->InputTuple '[?x ?y])

    '[[?x ?y]]
    (dsl/->InputRelation '[?x ?y])

    '[?x ...]
    (dsl/->InputCollection '?x)))


(deftest parse-find-element-test
  (are [form result] (= (dsl/parse-find-element form) result)
    '?x
    (dsl/->FindVariable '?x)

    '(pull ?x [*])
    (dsl/->FindPull nil '?x '[*])

    '(pull $a ?x ?pattern)
    (dsl/->FindPull '$a '?x '?pattern)

    '(?aggregate "a" ?x ?y)
    (dsl/->FindAggregate '?aggregate '["a" ?x ?y])))


(deftest parse-find-spec-test
  (are [form result] (= (dsl/parse-find-spec form) result)
    '[?x]
    (dsl/->FindRelation [(dsl/->FindVariable '?x)])

    '[?x .]
    (dsl/->FindScalar (dsl/->FindVariable '?x))

    '[[?x ?y]]
    (dsl/->FindTuple [(dsl/->FindVariable '?x) (dsl/->FindVariable '?y)])

    '[[?x ...]]
    (dsl/->FindCollection (dsl/->FindVariable '?x))))


(deftest parse-clause-test
  (are [form result] (= (dsl/parse-clause form) result)
    '[?eid :attribute ?value]
    (dsl/->PatternClause nil '[?eid :attribute ?value])

    '[$ ?eid :attribute ?value]
    (dsl/->PatternClause '$ '[?eid :attribute ?value])

    '[(?fn 42 ?x) ?y ?z]
    (dsl/->FunctionClause '?fn '[42 ?x] '[?y ?z])

    '[(?fn ?x ?y)]
    (dsl/->PredicateClause '?fn '[?x ?y])

    '(and [?eid :a1 ?v1]
          [?eid :a2 ?v2])
    (dsl/->AndClause nil [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                          (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(and $a
          [?eid :a1 ?v1]
          [?eid :a2 ?v2])
    (dsl/->AndClause '$a [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                          (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(or [?eid :a1 ?v1]
         [?eid :a2 ?v2])
    (dsl/->OrClause nil [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                         (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(or $a
         [?eid :a1 ?v1]
         [?eid :a2 ?v2])
    (dsl/->OrClause '$a [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                         (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(or-join $a
              [?eid]
              [?eid :a1 ?v1]
              [?eid :a2 ?v2])
    (dsl/->OrJoinClause '$a '[?eid]
                        [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                         (dsl/->PatternClause nil '[?eid :a2 ?v2])])


    '(or-join [?eid]
              [?eid :a1 ?v1]
              [?eid :a2 ?v2])
    (dsl/->OrJoinClause nil '[?eid]
                        [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                         (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(not [?eid :a1 ?v1]
          [?eid :a2 ?v2])
    (dsl/->NotClause nil [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                          (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(not $a
          [?eid :a1 ?v1]
          [?eid :a2 ?v2])
    (dsl/->NotClause '$a [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                          (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(not-join [?eid]
               [?eid :a1 ?v1]
               [?eid :a2 ?v2])
    (dsl/->NotJoinClause nil '[?eid]
                         [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                          (dsl/->PatternClause nil '[?eid :a2 ?v2])])

    '(not-join $a
               [?eid]
               [?eid :a1 ?v1]
               [?eid :a2 ?v2])
    (dsl/->NotJoinClause '$a '[?eid]
                         [(dsl/->PatternClause nil '[?eid :a1 ?v1])
                          (dsl/->PatternClause nil '[?eid :a2 ?v2])])))


(deftest parse-pull-element-test
  (are [form result] (= (dsl/parse-pull-element form) result)
    '*
    (dsl/->PullWildcard)

    ':a
    (dsl/->PullAttribute :a nil)

    '(:a :limit 9001 :default 42)
    (dsl/->PullAttribute :a {:limit 9001 :default 42})

    '{:a [*]}
    (dsl/->PullMap :a nil '[*])

    '{(:a :default "a" :limit 1) [*]}
    (dsl/->PullMap :a {:default "a" :limit 1} '[*])))


(deftest parse-pull-pattern-test
  (are [form result] (= (dsl/parse-pull-pattern form) result)
    '[* :a (:b :limit 1000)]
    (dsl/->PullPattern [(dsl/->PullWildcard)
                        (dsl/->PullAttribute :a nil)
                        (dsl/->PullAttribute :b {:limit 1000})])))


(deftest parse-rule-test
  (are [form result] (= (dsl/parse-rule form) result)
    '[(rule1 [?x])
      [?eid :a]
      [?eid :b]]
    (dsl/->Rule 'rule1 '[?x] []
                [(dsl/->PatternClause nil '[?eid :a])
                 (dsl/->PatternClause nil '[?eid :b])])

    '[(rule2 [?x ?y] ?z)
      [?eid :a]]
    (dsl/->Rule 'rule2 '[?x ?y] '[?z]
                [(dsl/->PatternClause nil '[?eid :a])])))


;; TODO branches
(deftest parse-rules-test
  (are [form result] (= (dsl/parse-rules form) result)
    '[[(rule1 ?x)
       [?eid :a]]]
    (dsl/->Rules
     [(dsl/->Rule 'rule1 nil '[?x] [(dsl/->PatternClause nil '[?eid :a])])])))


(deftest parse-query-test
  (are [form result] (= (dsl/parse-query form) result)
    '{:find [?a]
      :with [?b]
      :in [$]
      :where [[?eid :a ?a]
              [?eid :b ?b]]}
    (dsl/->Query
     (dsl/->FindRelation [(dsl/->FindVariable '?a)])
     (dsl/->AndClause nil [(dsl/->PatternClause nil '[?eid :a ?a])
                           (dsl/->PatternClause nil '[?eid :b ?b])])
     (dsl/->Inputs [(dsl/->InputSource '$)])
     (dsl/->With '[?b]))))
