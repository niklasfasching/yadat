(ns yadat.util
  (:refer-clojure :exclude [read-string var?])
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])))

(defn var? [x]
  (and (symbol? x) (= \? (first (name x)))))

(defn src? [x]
  (and (symbol? x) (= \$ (first (name x)))))

(def built-ins
  {'avg (fn avg [xs] (/ (reduce + xs) (count xs)))
   'sum (fn sum [xs] (reduce + xs))
   '- -
   '+ +
   '/ /
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

(defn read-string
  "Read `edn` string into data structure.
  As we are using the edn reader in clj, we have to pass `*data-readers*`
  populated via `data_readers.cljc` via the :readers option. The edn reader
  does not use `*data-readers*` as those could eval code and be unsafe.
  Using `data_readers.cljc` rather than specifying the readers here is useful
  as it prevents dealing with circular dependencies and is open for extension."
  [edn]
  (edn/read-string {:readers *data-readers*} edn))
