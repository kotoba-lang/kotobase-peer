(ns kotobase-peer.scan-instrument
  "Close the 30x that 2026-08-02-ic02-diagnosis.edn opened.

  That harness reimplemented the two join strategies and ran IC09's four steps
  per-binding in ~1.19 s, while 2026-08-01-ic09-diagnosis.edn measured the real
  engine's per-binding path at 35.6 s on the same data. The ~11.5x attributed to
  join batching rests on that 35.6 s, so it is in question until this is closed.

  The lesson from the last attempt is that reimplementing answers what the
  STRATEGIES cost, not what the ENGINE does. So this instruments the engine
  itself: `arrangement.datalog/scan*` is the single function every clause
  resolution goes through, and it is wrapped here to count calls, rows and time.
  Nothing is reimplemented and nothing in the library changes -- the query runs
  exactly as it always does, with a counter around one function.

  Counts are the product. A call count does not move when the host is busy, so
  running this against two arrangement pins on different days still compares.

  Usage: clojure -M:scan-instrument <ldbc-dynamic-dir> [posts]"
  (:require [kotobase-peer.core :as eng]
            [kotobase-peer.ldbc-snb-bench :as ldbc]
            [arrangement.datalog :as datalog]))

(def ^:private everything (constantly true))

(defn- instrumented
  "Run `f` with `scan*` counted. Returns {:ms :result :scans :rows :scan-ms
  :by-pattern-shape}. `by-pattern-shape` groups calls by which positions were
  bound, which is what distinguishes a keyed seek from a broad scan without
  recording every literal."
  [f]
  (let [calls (atom 0) rows (atom 0) nanos (atom 0) shapes (atom {})
        orig @#'datalog/scan*]
    (with-redefs-fn {#'datalog/scan*
                     (fn [db pattern visible?]
                       (let [t (System/nanoTime)
                             r (orig db pattern visible?)]
                         (swap! nanos + (- (System/nanoTime) t))
                         (swap! calls inc)
                         (swap! rows + (count r))
                         (swap! shapes update (mapv some? pattern) (fnil inc 0))
                         r))}
      (fn []
        (let [t (System/nanoTime) result (f)]
          {:ms (/ (- (System/nanoTime) t) 1e6)
           :result result
           :scans @calls
           :rows-returned @rows
           :scan-ms (/ @nanos 1e6)
           :by-pattern-shape @shapes})))))

(def ^:private queries
  {:ic02 '{:find [?f ?msg ?date] :in [?person]
           :where [[?person "knows" ?f]
                   [?msg "hasCreator" ?f]
                   [?msg "message/creationDate" ?date]]}
   :ic09-two-hop '{:find [?f2 ?msg ?date] :in [?person]
                   :where [[?person "knows" ?f1]
                           [?f1 "knows" ?f2]
                           [?msg "hasCreator" ?f2]
                           [?msg "message/creationDate" ?date]]}})

(defn- analyse [db label query person]
  (let [planned (eng/datalog-query-plan db query everything [person])
        plan-q (:query planned)
        ;; plan and execute measured separately: the planner runs its own
        ;; scans to estimate, and lumping them together is how the earlier
        ;; receipts ended up unable to say which half moved.
        planning (instrumented #(eng/datalog-query-plan db query everything [person]))
        executing (instrumented #(datalog/q db plan-q everything [person]))]
    {:query label
     :planning (dissoc planning :result)
     :executing (-> executing (dissoc :result) (assoc :result-rows (count (:result executing))))
     :scan-share-of-execute
     (when (pos? (:ms executing)) (double (/ (:scan-ms executing) (:ms executing))))}))

(defn -main [& args]
  (let [dir (first args)
        n-posts (parse-long (or (second args) "3000"))
        data (#'ldbc/load-subset dir n-posts)
        {:keys [db quads]} (#'ldbc/build-kotobase data)
        p (first (#'ldbc/pick-persons data 3))]
    (analyse db :warmup (:ic02 queries) p)
    (prn
     {:schema 1
      :receipt/type :scan-instrumentation
      :implementation "kotobase-peer.scan-instrument"
      :adr "ADR-2608021000 -- close the 30x between the reimplemented strategies and the real engine"
      :arrangement-pin (System/getProperty "kotobase.arrangement-pin" "see deps.edn")
      :dataset {:subset-posts n-posts :quads quads :messages (count (:messages data))}
      :start-person p
      :analyses (mapv (fn [[k q]] (analyse db k q p)) queries)
      :reading
      (str "For each query, :planning and :executing each report how many times "
           "the engine called scan*, how many rows those calls returned, and how "
           "much of the wall time was inside them. :by-pattern-shape keys are "
           "[s-bound? p-bound? o-bound?] -- [false true false] is a broad scan of "
           "an attribute, [true true false] is a keyed seek. If execute time is "
           "not mostly scan time, the cost is in the join's own bookkeeping, not "
           "in the store.")
      :caveats
      ["scan* is wrapped via with-redefs, so this is the real engine with a counter around one function -- nothing is reimplemented."
       "Counts and rows are load-invariant; the ms figures are not. Compare counts across runs, and treat times as within-run ratios."
       "Single run, one start person. A diagnosis, not a benchmark."]})))
