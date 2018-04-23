(ns yadat.schema
  (:require [clojure.set :as set]))

(def validation-options
  "Map of option to [other-option expected?]. The boolean expected specifies
  whether option and other-option
  - are required to be set in combination (true)
  - must not be set in combination (false)"
  {:component {:reference true}
   :unique {:many false}})

(defn reverse-ref? [a]
  (and (keyword? a)
       (= (first (name a)) \_)))

(defn reversed-ref [a]
  (if (reverse-ref? a)
    (keyword (namespace a) (subs (name a) 1))
    (keyword (namespace a) (str "_" (name a)))))

(defn forward-ref [a]
  (if (reverse-ref? a)
    (keyword (namespace a) (subs (name a) 1))
    a))

(defn is?
  "Checks whether `normalized-schema` defines attribute `a` to be a `x`.
  `x` can be one of the following keywords:
  - :many
  - :unique-identity
  - :unique-value
  - :index
  - :reference
  - :component"
  [normalized-schema a x]
  (contains? (x normalized-schema) a))

(defmulti normalize
  "Returns normalized schema of :schema/type for map `m`. Defaults to :datomish.
  By default two schema types are provided:

  - :minimal
    The bare minimum needed to allow fulfilling the Schema protocol.
    e.g. {:a [:component]}. Only attributes that should be treated in a special
    way need to be defined. Format is {attribute [& options]}, where options are
    the keywords supported by the Schema/is? protocol method for `x`.

  - :datomish
    A condensed version losely based on the datomic schema specification.
    e.g. {:a {:db.valueType :db.type/ref}}
    Attributes need not be defined in the schema unless they should be treated
    in a specific way. The following options of the datomic schema specification
    can be used:
    - {:db/cardinality ...}
    - {:db/unique ...}
    - {:db/isComponent true}
    - {:db/valueType :db.type/ref}"
  :schema/type :default :datomish)

(defmethod normalize :datomish [schema]
  (->> (dissoc schema :schema/type)
       (mapcat (fn [[a options]]
                 (cond-> []
                   (= (:db/valueType options) :db.type/ref)
                   (conj {:reference #{a} :index #{a}})

                   (= (:db/isComponent options) true)
                   (conj {:component #{a}})

                   (= (:db/cardinality options) :db.cardinality/many)
                   (conj {:many #{a}})

                   (= (:db/unique options) :db.unique/identity)
                   (conj {:unique #{a} :unique-identity #{a} :index #{a}})

                   (= (:db/unique options) :db.unique/value)
                   (conj {:unique #{a} :unique-value #{a} :index #{a}}))))
       (apply merge-with set/union)))

(defmethod normalize :minimal [schema]
  (->> (dissoc schema :schema/type)
       (mapcat (fn [[a options]]
                 (cond-> []
                   true
                   (concat (map (fn [option] {option #{a}}) options))

                   (some #{:unique-value :unique-identity} options)
                   (conj {:unique #{a}})

                   (some #{:unique-identity :unique-value :reference} options)
                   (conj {:index #{a}}))))
       (apply merge-with set/union)))

(defn validate
  "Validates `normalized-schema` and throws on error. Returns the schema.
  See `validation-options`."
  [normalized-schema]
  (doseq [[option options] validation-options
          [other-option expected] options
          a (get normalized-schema option)]
    (if-not (= (contains? (get normalized-schema other-option) a) expected)
      (let [message (if expected
                      (str "cannot be " option " without being " other-option)
                      (str "cannot be both " option " and " other-option))]
        (throw (ex-info (str "Invalid schema: Attribute " message) {:a a})))))
  normalized-schema)

(defn create
  "Returns a normalized schema for map `m`. See `normalize`."
  [m]
  (validate (normalize m)))
