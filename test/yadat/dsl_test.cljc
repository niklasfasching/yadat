(ns yadat.dsl-test
  (:require [yadat.dsl :as dsl]
            #?(:clj [clojure.test :as t]
               :cljs [cljs.test :as t :include-macros true])))


(t/deftest misc
  (t/testing "foo"
    )
  )

(dsl/clause '[(a "" ?bar) ?a ?b ?c])
