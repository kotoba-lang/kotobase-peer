#!/usr/bin/env nbb
;; Runs this repo's test suite through nbb (Node Babashka -- a SCI-interpreted
;; ClojureScript environment, no build step) as a genuine 3rd platform tier
;; alongside JVM (`clojure -M:test`) and self-hosted cljs (`npm run
;; test:cljs`). Auto-discovers every `*_test.cljc` under test/ (same
;; -test$-suffix convention shadow-cljs.edn's `:ns-regexp "-test$"` already
;; assumes) and resolves the classpath from `clojure -Spath`, filtered to
;; directories only (jars are JVM-only; nbb can't load them) -- the same
;; approach gen-shadow-cljs-edn.cljs already uses for the cljs build, so nbb
;; tests against the exact pinned deps.edn dependencies too, not a
;; hand-duplicated list that can drift.
;;
;; Exit code is wired through cljs.test's :end-run-tests report hook (NOT a
;; naive "parse the printed summary text" heuristic) -- nbb's own process
;; only exits non-zero if this namespace explicitly calls `js/process.exit`,
;; since a failed `is` assertion is just printed output, not a thrown
;; exception; without this hook every CI failure here would silently report
;; success.
;;
;; nbb port of the babashka original (ADR-2607173000, bb binary retired as
;; the fleet task/script host). This outer script still shells out to a
;; second `npx nbb` subprocess for the actual test run, same as the bb
;; original -- nbb (like bb) can't dynamically `require` namespace symbols
;; computed at runtime, so generating a static-require entry file and
;; invoking nbb on it as a separate process is the same necessary workaround
;; here as it was in bb, not a leftover of the old host.
;; ## Why the string calls here are JS interop and not `kotoba.lang.text`
;;
;; npm invokes this as bare `nbb run-nbb-tests.cljs`, with NO classpath --
;; because THIS SCRIPT is what computes the classpath (below) for the inner
;; process. It therefore cannot require a library to do it; the bootstrap is
;; circular. Measured 2026-09-09, requiring `kotoba.lang.text` here made
;; `npm run test:nbb` die with `Could not find namespace: kotoba.lang.text`
;; before discovering a single test -- and `clojure.string` would have failed
;; identically. Having ANY require in this file is the defect, not which
;; library it names.
;;
;; Everything below operates on paths and one classpath string. The tests
;; themselves run in the inner nbb process, which does get the classpath.
(def fs (js/require "node:fs"))
(def path (js/require "node:path"))
(def cp-mod (js/require "node:child_process"))

(defn- classpath []
  (let [cp (.trim (.toString (.execSync cp-mod "clojure -Spath") "utf8"))]
    (->> (.split cp ":")
         (remove #(= "" (.trim %)))
         (filter #(try (.isDirectory (.statSync fs %)) (catch :default _ false)))
         (cons "test")
         (#(.join (into-array %) ":")))))

(defn- path->ns [p]
  ;; Every replacement is GLOBAL. JS `.replace` with a string or an unflagged
  ;; regex replaces the FIRST match only, unlike clojure.string/replace, so the
  ;; `g` flag is not decoration here: `test/kotobase_peer/foo_bar_test.cljc` has
  ;; three underscores and two slashes, and a first-match-only translation
  ;; produces a namespace that does not exist and a test suite that reports
  ;; nothing missing.
  (-> p
      (.replace (js/RegExp. "\\\\" "g") "/")
      (.replace (js/RegExp. "^test/") "")
      (.replace (js/RegExp. "\\.clj[sc]$") "")
      (.replace (js/RegExp. "_" "g") "-")
      (.replace (js/RegExp. "/" "g") ".")))

(defn- find-test-files [dir]
  (let [entries (try (.readdirSync fs dir #js {:withFileTypes true}) (catch :default _ #js []))]
    (mapcat (fn [ent]
              (let [name (.-name ent)
                    p (.join path dir name)]
                (cond
                  (.isDirectory ent) (find-test-files p)
                  ;; `.cljs` too, and that is the point rather than a
                  ;; convenience. A `.cljs` test cannot run on the JVM, so a
                  ;; runner that discovers only `_test.cljc` leaves it running
                  ;; NOWHERE. Measured 2026-08-25: this repository's two
                  ;; `.cljs` suites were in that state, and running them turned
                  ;; up three real `ipld/encode` sites that always threw.
                  (or (.endsWith name "_test.cljc")
                      (.endsWith name "_test.cljs")) [p]
                  :else [])))
            (array-seq entries))))

(defn- test-namespaces []
  (->> (find-test-files "test")
       (map path->ns)
       sort))

(defn -main []
  (let [nss (test-namespaces)]
    (when (empty? nss)
      (println "run-nbb-tests: no *_test.cljc files found under test/")
      (js/process.exit 1))
    (let [entry-file "out-nbb-entry.cljs"
          requires (.join (into-array (map #(str "[" % "]") nss)) " ")
          run-args (.join (into-array (map #(str "'" %) nss)) " ")
          entry (str "(ns nbb-test-entry (:require [cljs.test :as t] " requires "))\n"
                     "(defmethod t/report [:cljs.test/default :end-run-tests] [m]\n"
                     "  (js/process.exit (if (t/successful? m) 0 1)))\n"
                     "(t/run-tests " run-args ")")]
      (.mkdirSync fs "out" #js {:recursive true})
      (.writeFileSync fs (str "out/" entry-file) entry)
      (println "nbb classpath:" (classpath))
      (println "test namespaces:" nss)
      (let [result (.spawnSync cp-mod "npx" #js ["nbb" "-cp" (str (classpath) ":out") (str "out/" entry-file)]
                                #js {:stdio "inherit"})]
        (js/process.exit (or (.-status result) 1))))))

(-main)
