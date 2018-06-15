(defproject org.clojars.niklasfasching/yadat "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.10.238"]
                 [org.clojure/core.match "0.3.0-alpha5"]]
  :profiles {:dev {:resource-paths ["src/test/resources"]
                   :dependencies [[datascript "0.16.5"]
                                  [com.datomic/datomic-free "0.9.5697"]]}}
  :target-path "target/%s"

  ;; cljs
  :plugins [[lein-cljsbuild "1.1.7"]]
  :cljsbuild {:builds {:release {:source-paths ["src"]
                                 :compiler {:main yadat.core
                                            :optimizations :advanced
                                            :output-to "target/yadat.js"}}
                       :dev {:source-paths ["src"]
                             :compiler {:main yadat.core
                                        :optimizations :none
                                        :source-map true
                                        :output-to "target/yadat.js"
                                        :output-dir "target/cljs"}}}})
