(ns yadat.schema)

(defprotocol Schema
  "A database for datoms."
  (is? [schema a x]
    "Checks whether the `schema` defines attribute `a` to be a `x`.
     `x` can be one of:
     - :many
     - :unique-identity
     - :unique-value
     - :reference
     - :component"))

(defn reverse-ref? [a]
  (and (keyword? a) (= (first (name a)) \_)))

(defn reversed-ref [a]
  (if (reverse-ref? a)
    (keyword (namespace a) (subs (name a) 1))
    (keyword (namespace a) (str "_" (name a)))))

(defrecord MinimalSchema [schema]
  Schema
  (is? [_ a x]
    (let [a (if (reverse-ref? a) (reversed-ref a) a)]
      (boolean (some #{x} (a schema))))))

(defrecord DatomishSchema [schema]
  Schema
  (is? [_ a x]
    (let [a (if (reverse-ref? a) (reversed-ref a) a)]
      (case x
        :many (= :db.cardinality/many (:db/cardinality (a schema)))
        :unique-identity (= :db.unique/identity (:db/unique (a schema)))
        :unique-value (= :db.unique/value (:db/unique (a schema)))
        :reference (= :db.type/ref (:db/valueType (a schema)))
        :component (:db/isComponent (a schema))
        (throw (ex-info "Invalid value" {:a a :x x}))))))

(defn create
  "Returns a new schema instance of type :schema/type."
  [m]
  (let [t (:schema/type m)
        m (dissoc m :schema/type)]
    (case t
      :minimal (->MinimalSchema m)
      :datomish (->DatomishSchema m)
      (->DatomishSchema m))))
