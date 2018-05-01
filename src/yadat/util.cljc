(ns yadat.util)

(defn resolve-symbol
  "symbol needs to be namespaced as resolve looks in the current environment and
  is otherwise not able to reslve the symbol.
  (get-thread-bindings) can help but only if executed in the same thread
  (not in tests)"
  [fn-symbol]

  (if-let [f #?(:cljs (throw (ex-info "resolve is not supported in cljs"
                                      {:symbol fn-symbol}))
                :clj (resolve fn-symbol))]
    f
    (throw (ex-info "Could not resolve function" {:f fn-symbol}))))
