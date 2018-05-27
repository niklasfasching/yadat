(ns yadat.integration-test
  (:require  [clojure.test :refer :all]
             [datascript.core :as datascript]
             [yadat.test-helper :as test-helper]
             [yadat.query :as datalog]
             [yadat.db :as db]
             [yadat.db.minimal-db]
             [yadat.db.sorted-set-db]))

(let [connection (datascript/create-conn test-helper/recipe-schema)
      query '{:find [?recipe-name]
              :where [[?recipe-id :recipe/name ?recipe-name]
                      [?recipe-id :recipe/ingredients ?ingredient-id]
                      [?ingredient-id :ingredient/food ?food-id]
                      [?food-id :food/name "Bread"]]}]
  (datascript/transact! connection test-helper/recipes)
  (let [result (datascript/q query @connection)]))

(let [connection (datascript/create-conn test-helper/recipe-schema)
      query '{:find [?food-name]
              :where [[?recipe-id :recipe/name ?recipe-name]
                      [?recipe-id :recipe/ingredients ?ingredient-id]
                      [?ingredient-id :ingredient/food ?food-id]
                      [?food-id :food/name ?food-name]
                      [(= ?food-id 10)]]}]
  (datascript/transact! connection test-helper/recipes)
  (datascript/q query @connection))

(let [connection (datascript/create-conn test-helper/recipe-schema)
      query '{:find [[?food-id ?food-name]]
              :where [[?recipe-id :recipe/name ?recipe-name]
                      [?recipe-id :recipe/ingredients ?ingredient-id]
                      [?ingredient-id :ingredient/food ?food-id]
                      [?food-id :food/name ?food-name]]}]
  (datascript/transact! connection test-helper/recipes)
  (datascript/q query @connection))

(let [connection (datascript/create-conn test-helper/recipe-schema)
      query '{:find [[?food-id ?food-name]]
              :where [[?food-id :food/name ?food-name]]}]
  (datascript/transact! connection test-helper/recipes)
  (datascript/q query @connection))



(def schema {:major/id {:db/unique :db.unique/identity}
             :major/sections {:db/type :db.type/ref
                              ;; :db/isComponent true
                              :db/cardinality :db.cardinality/many}
             :section/modules {:db/type :db.type/ref
                               :db/cardinality :db.cardinality/many}
             :module/id {:db/unique :db.unique/identity}
             :module/courses {:db/type :db.type/ref
                              ;; :db/isComponent true
                              :db/cardinality :db.cardinality/many}})

(def query '{:find [?name]
             :where [[?module-id :module/name ?module-name]
                     [(re-find #"Entscheidung" ?module-name)]
                     [?module-id :module/courses ?course-link-id]
                     [?course-link-id :course-link/name ?name]]})

(binding [*print-namespace-maps* false]
  (spit "human-factors-major.edn" (with-out-str (clojure.pprint/pprint human-factors))))

(def human-factors (read-string (slurp "human-factors.edn")))

(def human-factors-2 (read-string (slurp "human-factors.edn")))

;; right now ~ 1.5 slower (470 datascript, 620 me)
;; datalog 140% of datascript runtime

(time (dotimes [i 20]
        (let [db (db/open :minimal schema)
              [transaction eids] (db/transact db [human-factors])]
          (datalog/q (:db transaction) query))))


(let [connection (datascript/create-conn schema)]
  (datascript/transact! connection [human-factors])
  (time (dotimes [i 20] (datascript/q query @connection))))

(time (dotimes [i 20]
        (let [connection (datascript/create-conn schema)]
          (datascript/transact! connection [human-factors])
          (datascript/q query @connection))))

;; the querying is like ... 5 times slower lol
;; 1900
(let [db (db/open :sorted-set schema)
      [{:keys [db]} eids] (time (db/transact db [human-factors]))]
  (time (dotimes [i 1000]
          (datalog/q db query))))

(let [db (db/open :minimal schema)
      [{:keys [db]} eids] (time (db/transact db [human-factors]))]
  (datalog/q db '{:find [?name]
                  :where [[?module-id :module/name ?module-name]
                          [?course-link-id :course-link/name ?name]]})
  (datalog/q db '{:find [?name]
                  :where [[?module-id :module/name ?name]]}))

;; 400
q(let [connection (datascript/create-conn schema)]
  (time (datascript/transact! connection [human-factors]))
  (datascript/q '{:find [(pull ?module-id [*])]
                  :where [[?module-id :module/name ?module-name]
                          [(re-find #"Entscheidung" ?module-name)]
                          [?module-id :module/courses ?course-link-id]
                          [?course-link-id :course-link/name ?name]]}
                @connection))


(let [connection (datascript/create-conn test-helper/recipe-schema)
      query '{:find [(pull ?recipe-id [:recipe/name {:recipe/author [*]}]) .]
              :where [[?recipe-id :recipe/name _]]}]
  (datascript/transact! connection test-helper/recipes)
  (datascript/q query @connection))

(def simple-schema {:major/id [:unique-identity]
                    :major/sections [:reference :many]
                    :section/modules [:reference :many]
                    :module/id [:unique-identity]
                    :module/courses [:reference :many]})

(time (let [db (db/open :minimal simple-schema)
            [transaction] (db/transact db [human-factors])
            db (:db transaction)]
        (time (datalog/q db query))))

(time (let [db (db/open :sorted-set schema)
            [transaction] (db/transact db [human-factors])
            db (:db transaction)]
        (time (datalog/q db query))))

(time (let [connection (datascript/create-conn schema)]
        (datascript/transact! connection [human-factors])
        (time (datascript/q query @connection))))

(let [db (db/open :sorted-set schema)
      [transaction] (db/transact db [human-factors])
      db (:db transaction)]
  (first (datalog/q db '{:find [(pull ?module-id [*])]
                         :where [[?module-id :module/name ?module-name]
                                 [(re-find #".*" ?module-name)]
                                 [?module-id :module/courses ?course-link-id]
                                 [?course-link-id :course-link/name ?name]]})))
