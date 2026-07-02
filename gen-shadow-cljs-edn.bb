#!/usr/bin/env bb
;; Generates shadow-cljs.edn's :source-paths from `clojure -Spath` -- the
;; same git/sha deps.edn dependencies `clojure -M:test` already resolves,
;; so the cljs build tests against the exact pinned versions, not a
;; hand-duplicated list that can drift. Jar entries (Clojure/core.specs/
;; spec.alpha on the JVM classpath) are filtered out; shadow-cljs
;; :source-paths wants directories only.
(require '[clojure.string :as str]
         '[clojure.java.shell :refer [sh]]
         '[clojure.java.io :as io])

(def cp (-> (sh "clojure" "-Spath") :out str/trim))
(def dirs (->> (str/split cp #":")
               (remove str/blank?)
               (filter #(.isDirectory (io/file %)))))

(spit "shadow-cljs.edn"
      (str "{:source-paths " (pr-str (vec (concat ["test"] dirs))) "\n"
           " :builds\n"
           " {:test {:target :node-test\n"
           "         :output-to \"out/test.js\"\n"
           "         :ns-regexp \"-test$\"}}}\n"))

(println "wrote shadow-cljs.edn with" (count dirs) "source dirs from clojure -Spath")
