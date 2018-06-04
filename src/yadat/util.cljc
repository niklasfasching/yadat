(ns yadat.util
  (:refer-clojure :exclude [read-string var?])
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.walk :as walk]))

(defn var? [x]
  (and (symbol? x) (= \? (first (name x)))))

(def built-ins
  {'avg (fn avg [xs] (/ (reduce + xs) (count xs)))
   'sum (fn sum [xs] (reduce + xs))
   'count count
   'count-distinct (fn count-distinct [xs] (count (distinct xs)))
   'distinct distinct
   'min (fn min [xs] (reduce (fn [y x] (if (neg? (compare x y)) x y)) xs))
   'max (fn max [xs] (reduce (fn [y x] (if (pos? (compare x y)) x y)) xs))
   'sample (fn sample [n xs] (take n (shuffle xs)))})

(defn resolve-symbol
  "symbol needs to be namespaced as resolve looks in the current environment and
  is otherwise not able to reslve the symbol.
  (get-thread-bindings) can help but only if executed in the same thread
  (not in tests)"
  [sym]
  (if-let [f #?(:cljs (or (get built-ins sym)
                          (throw (ex-info "resolve is not supported in cljs"
                                          {:symbol sym})))
                :clj (or (get built-ins sym)
                         (resolve sym)))]
    f
    (throw (ex-info "Could not resolve symbol" {:sym sym}))))

(defn read-string [readers edn]
  (edn/read-string {:readers readers} edn))

(defn flatten-1 [x]
  (mapcat #(if (sequential? %) % [%]) x))

(defn apply-function [rows raw-f raw-args raw-vars]
  (let [f (resolve-symbol raw-f)]
    (map (fn [r] (let [args (mapv #(get r % %) raw-args)
                       result (apply f args)]
                   (if (= (count raw-vars) 1)
                     (into r [[(first raw-vars) result]])
                     (into r (map vector result raw-vars))))) rows)))

(defn apply-predicate [rows raw-f raw-args]
  (let [f (resolve-symbol raw-f)]
    (filter (fn [b] (apply f (mapv #(get b % %) raw-args))) rows)))


(defprotocol Resolve
  (resolve [this db x]
    "Resolves `clause` into a relation based on `db` and `relations`.
  Returns a list of relations of which the first relation must be the resolved
  relation. We cannot just return the resolved relation as there are clauses
  that modify the input `relations`."))
