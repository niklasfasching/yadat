(ns yadat.parser
  (:require [clojure.spec.alpha :as s]
            [yadat.util :as util]))

;; conforming is not as pretty as i hoped
;; conformed value is not tagged so not that easy to dispatch..
;; buuuuuut tagged when resolved via alt - so i got it after all
;; question now is whether i want to turn it into records?
;; i guess could try that - maybe some overhead but pretty?
;; to convert to records or not to...
;; not converting seems like much less code

(filter util/var? (apply concat '[(pull ?foo [(a :as ?bar)])]))
(flatten)
(mapcat identity [[1 2]])

(defn flatten-1 [x]
  (mapcat #(if (sequential? %) % [%]) x))

(defn element-variables [[t element]]
  (if (= t :variable)
    element
    (:variable element)))

(defn spec-variables [[t spec]]
  (if (= t :scalar)
    (element-variables (:element spec))
    (mapcat element-variables (:elements (:elements spec)))))

;; should i rewrite based on spec or write my own parser into defrecords and rewrite with that?
;; i'm for the parser...
(defn resolve-spec)


(defmacro spec [name spec conformer]
  `(s/def ~name (s/and ~spec (s/conformer ~conformer))))

(defrecord Or [])

(spec ::or (s/cat :type #{'or} :clauses (s/spec (s/+ ::clause)))
      map->Or)

(s/def ::or (s/cat :type #{'or} :clauses (s/spec (s/+ ::clause))))

(s/assert ::or '(or [?a ?b 1]))

(s/def ::or int?)

(s/and ::or
       (s/conformer (fn [])))

(defn or-clause [v]
  (let [v (s/conform ::or v)]

    )
  (->OrClause clauses)
  )


(defrecord OrClause [fn arguments clauses]
  Clause
  )


(s/def ::and (s/cat :type #{'and} :clauses (s/+ ::clause)))
(s/def ::fun (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                    :arguments (s/+ any?)
                    :variables (s/+ util/var?)))

(s/def ::clause
  (s/alt :or (s/cat :type #{'or} :clauses (s/spec (s/+ ::clause)))
         :and (s/cat :type #{'and} :clauses (s/+ ::clause))
         :not (s/cat :type #{'not} :clauses (s/+ ::clause))
         :function (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                          :arguments (s/+ any?)
                          :variables (s/+ util/var?))
         :predicate (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                           :arguments (s/and (s/+ any?)
                                             #(some util/var? %)))
         :pattern (s/& (s/+ any?) (fn [pattern]

                                    (and (some util/var? pattern)
                                         (<= (count pattern) 3)
                                         (not-any? #(and (symbol? %) (not (util/var? %))) pattern)
                                         )))))

(s/conform ::clause '(a ?b 1 2 ?a))


(s/def ::spec
  (s/alt :scalar (s/cat :element ::element :dot #{'.})
         :collection (s/spec (s/cat :elements (s/+ ::element) :dots #{'...}))
         :tuple (s/spec (s/cat :elements (s/+ ::element)))
         :relation (s/cat :elements (s/+ ::element))))

(s/def ::element
  (s/alt :variable util/var?
         :pull (s/cat :type #{'pull} :variable util/var? :spec any?)
         :aggregate (s/cat :fn (s/or :variable util/var? :symbol symbol?)
                           :constants (s/* (complement util/var?))
                           :variable util/var?)))


(defn x-integer? [x]
  (if (integer? x)
    x
    (if (string? x)
      (try
        (Integer/parseInt x)
        (catch Exception e
          :clojure.spec/invalid))
      :clojure.spec/invalid)))

(s/def :user/name string?)



(s/def :user/age (s/conformer ))



(s/conform :user/age nil)

(s/def ::user (s/keys :req [:user/name :user/age]))

(s/conform ::user {:user/name "juho" :user/age 9001})
(s/conform ::user {:user/name "juho" :user/age "9001"})
;; => {:user/name "juho", :user/age 9001}
(s/conform ::user {::user/name "juho" ::user/age "x9001"})
;; => :clojure.spec/invalid




;; to build my very own parser all i need is cat + * and or
;; but how to throw a sensible error?

;; spec is weird and i actually mostly want the conform part
;; -> write my own parser i guess..
;; spec error messages suck so hard
;; otherwise
