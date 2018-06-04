(ns yadat.pull
  (:require [clojure.core.match :refer [match]]
            [yadat.db :as db]))

(def default-limit 1000)

(defprotocol PullElement
  (resolve-pull-element [_ db entity]))
