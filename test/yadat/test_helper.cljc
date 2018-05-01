(ns yadat.test-helper
  (:require  [clojure.test :as t]
             [yadat.db :as db]))

(def recipe-schema {:recipe/author [:reference]
                    :recipe/ingredients [:reference :many]
                    :ingredient/food [:reference]
                    :food/name [:unique-identity]})

(def recipes [{:recipe/name "Spaghetti with tomato sauce"
               :recipe/author {:author/name "Adam"
                               :author/gender "M"}
               :recipe/ingredients [{:ingredient/food {:food/name "Spaghetti"
                                                       :food/category "Noodles"}
                                     :ingredient/quantity 1
                                     :ingredient/unit "package"}
                                    {:ingredient/food {:food/name "Tomato Sauce"
                                                       :food/category "Sauce"}
                                     :ingredient/quantity 2
                                     :ingredient/unit "package"}]}
              {:recipe/name "Bread with butter"
               :recipe/author {:author/name "Eve"
                               :author/gender "F"}
               :recipe/ingredients [{:ingredient/food {:food/name "Bread"
                                                       :food/category "Bread"}
                                     :ingredient/quantity 1
                                     :ingredient/unit "slice"}
                                    {:ingredient/food {:food/name "Butter"
                                                       :food/category "Fat"}
                                     :ingredient/quantity 3
                                     :ingredient/unit "scoops"}]}
              {:recipe/name "Banana bread sandwhich"
               :recipe/author {:author/name "Theo"
                               :author/gender "F"}
               :recipe/ingredients [{:ingredient/food {:food/name "Bread"
                                                       :food/category "Bread"}
                                     :ingredient/quantity 2
                                     :ingredient/unit "slice"}
                                    {:ingredient/food {:food/name "Banana"
                                                       :food/category "Fruit"}
                                     :ingredient/quantity 1
                                     :ingredient/unit "piece"}]}])

(defn recipe-db []
  (let [db (db/make-db :minimal recipe-schema)
        [transaction eids] (db/transact db recipes)]
    (:db transaction)))

(defn db-of [schema datoms]
  (let [db (db/make-db :minimal schema)]
    (reduce db/insert db datoms)))
