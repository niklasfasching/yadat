(ns yadat.parser-test
  (:require [yadat.parser :as parser]
            #?(:clj [clojure.test :as t]
               :cljs [cljs.test :as t :include-macros true])))


(t/deftest misc
  (t/testing "foo"
    )
  )

(parser/clause '[(a "" ?bar) ?a ?b ?c])
