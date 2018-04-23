(ns yadat.test
  (:require [clojure.test :as t]
            [yadat.query-test]))

(enable-console-print!)

(defn ^:export run []
  (t/run-all-tests #"yadat.*-test"))
