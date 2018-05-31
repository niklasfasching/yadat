(ns yadat.util
  (:refer-clojure :exclude [read-string])
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.walk :as walk]
            [expound.alpha :as expound]
            [clojure.spec.alpha :as s]))

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

(defn conform [spec value]
  (let [conformed (s/conform spec value)]
    (if (= conformed :clojure.spec.alpha/invalid)
      (throw (Exception. (expound/expound-str spec value)))
      conformed)))
