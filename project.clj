(defproject clojure-sree "0.1.0-SNAPSHOT"
  :description "Connecting to databases."
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies  [[org.clojure/clojure "1.10.1"]
                  [mysql/mysql-connector-java "5.1.44" ]
                  [org.clojure/java.jdbc "0.7.12"]
                  [de.ubercode.clostache/clostache "1.3.1"]
                  [org.postgresql/postgresql "42.1.3"]
                  [org.clojure/data.json "1.0.0"]
                  [clj-http "3.11.0"]
                  [doric "0.9.0"]
                  [stencil "0.5.0"]]
  :main ^:skip-aot clojure-sree.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
