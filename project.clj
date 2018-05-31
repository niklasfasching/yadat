(defproject org.clojars.niklasfasching/yadat "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.10.238"]
                 [expound "0.7.0"]
                 [datascript "0.16.5"]]
  :plugins [[lein-cljsbuild "1.1.7"]]
  :cljsbuild {:builds {:main {:source-paths ["src"]
                              :compiler {:main yadat.api
                                         :optimizations :advanced
                                         :pretty-print true
                                         :output-to "target/main.js"}}
                       :js {:source-paths ["src"]
                            :compiler {:main yadat.js-api
                                       :optimizations :advanced
                                       :pretty-print true
                                       :output-to "resources/main.js"}}}}
  :target-path "target/%s")
