#!/usr/bin/env nbb
;; Generates shadow-cljs.edn's :source-paths from `clojure -Spath` -- the
;; same git/sha deps.edn dependencies `clojure -M:test` already resolves,
;; so the cljs build tests against the exact pinned versions, not a
;; hand-duplicated list that can drift. Jar entries (Clojure/core.specs/
;; spec.alpha on the JVM classpath) are filtered out; shadow-cljs
;; :source-paths wants directories only.
;;
;; nbb port of the babashka original (ADR-2607173000, bb binary retired as
;; the fleet task/script host). Standalone -- no dependency on the
;; superproject's scripts/nbb_compat shim.
(require '[clojure.string :as str])

(def fs (js/require "node:fs"))
(def cp-mod (js/require "node:child_process"))

(def cp
  (str/trim (.toString (.execSync cp-mod "clojure -Spath") "utf8")))

(def dirs
  (->> (str/split cp #":")
       (remove str/blank?)
       (filter #(try (.isDirectory (.statSync fs %)) (catch :default _ false)))))

(.writeFileSync fs "shadow-cljs.edn"
                (str "{:source-paths " (pr-str (vec (concat ["test" "bench"] dirs))) "\n"
                     " :builds\n"
                     " {:test {:target :node-test\n"
                     "         :output-to \"out/test.js\"\n"
                     "         :ns-regexp \"-test$\"}\n"
                     "  :view-e2e {:target :browser\n"
                     "             :output-dir \"out/view-e2e\"\n"
                     "             :asset-path \"/\"\n"
                     "             :modules {:view-e2e {:init-fn kotobase-peer.browser-view-e2e/run}}}}}\n"))

(println "wrote shadow-cljs.edn with" (count dirs) "source dirs from clojure -Spath")
