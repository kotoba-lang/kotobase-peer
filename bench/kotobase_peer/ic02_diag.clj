(ns kotobase-peer.ic02-diag
  "Why did LDBC IC02 regress? 7.7x -> 14.7x -> 19.2x of Neo4j across three runs
  (bench/results/2026-08-02-ldbc-snb-ic02-hypothesis-refuted.edn), monotone, so
  not noise -- and my first explanation for it did not survive its own test.

  That failure is the reason this harness exists. I reasoned about the cause,
  shipped a fix, and the fix made IC02 slower. The discipline that worked for
  IC09 was the opposite: instrument first, and let the numbers name the term.

  So this measures the two join strategies DIRECTLY, in one process, against
  the same db and the same real bindings, interleaved:

    per-binding  -- one scan per binding (what arrangement did before batching)
    grouped      -- one scan per DISTINCT substituted pattern (what it does now)

  Both are reimplemented here from the same primitives arrangement uses, so
  the comparison needs no second checkout and carries no host variance between
  versions: they run microseconds apart on the same JVM, same heap, same
  caches. Whatever separates them is the code.

  It also breaks IC02 down per join STEP, because the regression has to live in
  one of them, and reports how much dedup each step actually had available --
  the batching win is exactly bindings/distinct-patterns, and a step where that
  ratio is 1 pays for grouping and collects nothing.

  Usage: clojure -M:ic02-diag <ldbc-dynamic-dir> [posts]"
  (:require [clojure.string :as str]
            [kotobase-peer.core :as eng]
            [kotobase-peer.ldbc-snb-bench :as ldbc]
            [arrangement.query :as kqe]))

(def ^:private everything (constantly true))

(defn- ms [f]
  (let [t (System/nanoTime) r (f)]
    {:ms (/ (- (System/nanoTime) t) 1e6) :result r}))

;; ── the two strategies, reimplemented from arrangement's own primitives ─────
;; `substitute` and `unify-positional` are private there; these are the same
;; two operations, and the equality assertion below is what keeps them honest.

(defn- lvar? [x] (and (symbol? x) (not= x '_) (= \? (first (name x)))))

(defn- subst [term binding]
  (cond (= term '_) nil
        (lvar? term) (get binding term)
        :else term))

(defn- unify [binding clause values]
  (reduce (fn [b [term v]]
            (cond (= term '_) b
                  (lvar? term) (if (contains? b term)
                                 (if (= (get b term) v) b (reduced nil))
                                 (assoc b term v))
                  (= term v) b
                  :else (reduced nil)))
          binding (map vector clause values)))

(defn- per-binding-step [bindings clause db]
  (into #{}
        (mapcat (fn [binding]
                  (let [pattern (mapv #(subst % binding) clause)]
                    (keep #(unify binding clause [(:s %) (:p %) (:o %)])
                          (kqe/query db pattern everything)))))
        bindings))

(defn- grouped-step [bindings clause db]
  (let [groups (group-by (fn [binding] (mapv #(subst % binding) clause)) bindings)]
    (into #{}
          (mapcat (fn [[pattern group]]
                    (let [rows (kqe/query db pattern everything)]
                      (mapcat (fn [binding]
                                (keep #(unify binding clause [(:s %) (:p %) (:o %)]) rows))
                              group))))
          groups)))

;; ── per-step walk of a real query ───────────────────────────────────────────

(defn- walk-steps
  "Run `clauses` in order, timing BOTH strategies at each step and reporting
  what each step had to work with. `:dedup-ratio` is bindings/distinct-patterns
  -- the exact factor grouping can win, so 1.0 means grouping had nothing to
  collect and only cost something."
  [db clauses initial]
  (loop [remaining clauses bindings initial acc []]
    (if (empty? remaining)
      acc
      (let [clause (first remaining)
            groups (count (group-by (fn [b] (mapv #(subst % b) clause)) bindings))
            ;; alternate which runs first across steps so neither strategy
            ;; systematically enjoys a warmer cache
            first-grouped? (even? (count acc))
            a (if first-grouped?
                (ms #(grouped-step bindings clause db))
                (ms #(per-binding-step bindings clause db)))
            b (if first-grouped?
                (ms #(per-binding-step bindings clause db))
                (ms #(grouped-step bindings clause db)))
            [g p] (if first-grouped? [a b] [b a])]
        (recur (rest remaining)
               (:result g)
               (conj acc {:step (count acc)
                          :clause clause
                          :bindings-in (count bindings)
                          :distinct-patterns groups
                          :dedup-ratio (double (/ (count bindings) (max 1 groups)))
                          :bindings-out (count (:result g))
                          :grouped-ms (:ms g)
                          :per-binding-ms (:ms p)
                          :grouped-faster-by (double (/ (:ms p) (max 0.0001 (:ms g))))
                          :answers-equal (= (:result g) (:result p))}))))))

(def ^:private ic02
  '[[?person "knows" ?f]
    [?msg "hasCreator" ?f]
    [?msg "message/creationDate" ?date]])

(def ^:private ic09-two-hop
  '[[?person "knows" ?f1]
    [?f1 "knows" ?f2]
    [?msg "hasCreator" ?f2]
    [?msg "message/creationDate" ?date]])

(defn -main [& args]
  (let [dir (first args)
        n-posts (parse-long (or (second args) "3000"))
        data (#'ldbc/load-subset dir n-posts)
        {:keys [db quads]} (#'ldbc/build-kotobase data)
        p (first (#'ldbc/pick-persons data 3))]
    ;; warm both strategies once so JIT is not billed to whichever ran first
    (walk-steps db ic02 #{{'?person p}})
    (prn
     {:schema 1
      :receipt/type :ic02-diagnosis
      :implementation "kotobase-peer.ic02-diag"
      :adr "ADR-2608021000 §6-5-1 -- diagnose IC02 by measuring, after reasoning about it produced a fix that made it worse"
      :dataset {:subset-posts n-posts :quads quads
                :knows-quads (* 2 (count (:knows data)))
                :messages (count (:messages data))}
      :start-person p
      :ic02-steps (walk-steps db ic02 #{{'?person p}})
      :ic09-steps (walk-steps db ic09-two-hop #{{'?person p}})
      :reading
      (str "Per step: :dedup-ratio is how much grouping COULD win (bindings per "
           "distinct pattern) and :grouped-faster-by is how much it DID. A step "
           "with ratio 1.0 and grouped-faster-by below 1.0 is paying for the "
           "group-by and collecting nothing -- which is the shape IC02 was "
           "accused of having. If IC02's steps do not look like that, the "
           "regression is somewhere else and the first explanation was wrong "
           "for a second reason.")
      :caveats
      ["Both strategies are reimplemented here from the same primitives arrangement uses (substitute / unify-positional are private there). :answers-equal per step is what keeps that honest -- if a step ever reports false, this harness is measuring something other than the engine."
       "Single run per step on one start person. This is a diagnosis, not a benchmark; the percentiles are in the LDBC receipts."
       "The two strategies alternate which one runs first across steps, so neither systematically benefits from the other having warmed the same rows."]})))
