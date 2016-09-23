(defproject dfs-clj "0.1.7"
  :description "Library for using Prismatic Schema, Pail and Cascalog."
  :url "http://github.com/EricGebhart/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :min-lein-version "2.0.0"

  :source-paths ["src" "src/clj" "src/clj/dfs-clj"]
  :java-source-paths ["src/jvm"]


  :dependencies [[org.clojure/clojure "1.7.0"]
                 ;;[org.clojure/clojure "1.5.1"]
                 [com.backtype/dfs-datastores "1.3.4"]
                 [org.apache.hadoop/hadoop-core "1.2.0" ]
                 [com.backtype/dfs-datastores "1.3.6" :exclusions [org.slf4j/slf4j-api]]
                 [com.backtype/dfs-datastores-cascading "1.3.6"
                  :exclusions [cascading/cascading-core cascading/cascading-hadoop]]
                 [cascalog/cascalog-core "3.0.0" :exclusions [[org.slf4j/slf4j-log4j12] [log4j]]]
                                        ;[clj-thrift "0.1.3"]
                                        ;[cascalog "3.0.0" ]
                 [byte-streams "0.1.7"]
                 [org.clojure/data.fressian "0.2.1"]
                 [prismatic/schema "1.1.1"]]

                                        ;:aot [pail-schema.data-unit-pail-structure]

  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}

             ;;:provided {:dependencies [[org.slf4j/slf4j-log4j12 "1.7.4"]]}

             :lint {:global-vars {*warn-on-reflection* true}}

             :dev {:dependencies [[midje "1.5.1"]]
                   :source-paths ["src/test"]
                   :aot [dfs-clj.fakes.structure]
                   :plugins [[lein-midje "3.0.1"]]}}

  :aliases {"lint" ["with-profile" "+lint" "midje"]}

  :deploy-repositories [["releases" {:url "https://clojars.org/repo" :username :gpg :password :gpg}]
                        ["snapshots" {:url "https://clojars.org/repo" :username :gpg :password :gpg}]])
