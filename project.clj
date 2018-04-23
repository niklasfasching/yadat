(defproject org.clojars.niklasfasching/yadat "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/clojurescript "1.10.238"]
                 [org.clojure/core.match "0.3.0-alpha5"]]
  :global-vars {*warn-on-reflection* true}
  :profiles {:dev {:resource-paths ["src/test/resources"]
                   :dependencies [[com.taoensso/tufte "2.0.1"]
                                  [datascript "0.16.5"]
                                  [com.datomic/datomic-free "0.9.5697"
                                   :exclusions [com.google.guava/guava]]]}}
  :target-path "target/%s"
  :plugins [[lein-cljsbuild "1.1.7"]]
  :cljsbuild {:builds {;; :release {:source-paths ["src"]
                       ;;           :compiler {:main yadat.core
                       ;;                      :optimizations :advanced
                       ;;                      :output-to "target/release/yadat.js"}}
                       ;; :dev {:source-paths ["src"]
                       ;;       :compiler {:main yadat.core
                       ;;                  :optimizations :none
                       ;;                  :source-map true
                       ;;                  :output-to "target/dev/yadat.js"
                       ;;                  :output-dir "target/dev/cljs"}}
                       :test {:source-paths ["src" "test"]
                              :compiler {:output-to "target/test/yadat.js"
                                         :optimizations :simple}}}
              :test-commands {"node" ["node" "--eval"
                                      "require('./target/test/yadat.js').yadat.test.run()"]}})
