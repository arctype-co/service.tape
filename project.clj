(defproject arctype/service.tape "0.1.0-SNAPSHOT"
  :repositories
  {"sonatype-snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"}}
  :dependencies 
  [[org.clojure/clojure "1.8.0"]
   [org.clojure/core.async "0.4.474"]
   [arctype/service "0.1.0-SNAPSHOT"]
   [com.squareup.tape2/tape "2.0.0-SNAPSHOT"]])
