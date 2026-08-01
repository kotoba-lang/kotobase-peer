(ns kotobase-peer.ic09-diag
  "Why is IC09 21 seconds when Neo4j answers the same two-hop query on the same
  data in the same process in 246 ms? (bench/results/2026-08-01-ldbc-snb-
  interactive.edn, ADR-2608021000 §6-1.)

  The instruction in that ADR is to PRINT THE PLAN before touching anything.
  This does that, and separates the two things a `kotobase-peer.core/query`
  call actually does:

    1. PLAN. `datalog-query-plan` estimates each clause's cardinality. Unless
       materialized statistics were supplied, it estimates by RUNNING the
       clause -- `(count (kqe/query db pattern visible?))` -- and
       `arrangement.query/query` builds a SET of every matching quad. For a
       clause like `[?f1 \"knows\" ?f2]`, where neither end is bound, that
       materialises every knows quad in the graph on every single query.

    2. EXECUTE. `arrangement.datalog/q` runs the reordered join.

  Reports each clause's probe cost and row count separately from the join, so
  the 21 seconds lands on whichever of the two actually owns it rather than on
  a guess. Also prints the chosen order, because the first hypothesis (that
  the planner ignores `:in` bindings) was WRONG -- datalog-query-plan does
  substitute them into the probe pattern -- and the order it picks should be
  checked rather than assumed a second time.

  Usage: clojure -M:ic09-diag <ldbc-dynamic-dir> [posts]"
  (:require [clojure.string :as str]
            [kotobase-peer.core :as eng]
            [kotobase-peer.ldbc-snb-bench :as ldbc]
            [arrangement.query :as kqe]
            [arrangement.datalog :as datalog]))

(def ^:private everything (constantly true))

(defn- ms [f]
  (let [t (System/nanoTime) r (f)]
    {:ms (/ (- (System/nanoTime) t) 1e6) :result r}))

(def ^:private ic09-two-hop
  '{:find [?f2 ?msg ?date]
    :in [?person]
    :where [[?person "knows" ?f1]
            [?f1 "knows" ?f2]
            [?msg "hasCreator" ?f2]
            [?msg "message/creationDate" ?date]]})

(def ^:private ic09-one-hop
  '{:find [?f ?msg ?date]
    :in [?person]
    :where [[?person "knows" ?f]
            [?msg "hasCreator" ?f]
            [?msg "message/creationDate" ?date]]})

(defn- probe-clauses
  "What the planner pays before it plans anything: one `kqe/query` per clause,
  with `:in` bindings substituted exactly as datalog-query-plan substitutes
  them. Returns per-clause cost and cardinality."
  [db query person]
  (let [input-vars (vec (remove #{'$} (:in query)))
        bindings (zipmap input-vars [person])
        var? (fn [x] (and (symbol? x) (str/starts-with? (name x) "?")))]
    (mapv (fn [clause]
            (let [pattern (mapv (fn [t] (cond (= t '_) nil
                                              (contains? bindings t) (get bindings t)
                                              (var? t) nil
                                              :else t))
                                clause)
                  {:keys [ms result]} (ms #(count (kqe/query db pattern everything)))]
              {:clause clause :probe-pattern pattern :rows result :probe-ms ms}))
          (:where query))))

(defn- analyse [db label query person]
  (let [probes (probe-clauses db query person)
        planned (ms #(eng/datalog-query-plan db query everything [person]))
        plan (:result planned)
        exec (ms #(count (datalog/q db (:query plan) everything [person])))
        total (ms #(count (eng/query db query everything [person])))]
    {:query label
     :clause-probes probes
     :probe-total-ms (reduce + (map :probe-ms probes))
     :plan-ms (:ms planned)
     :chosen-order (mapv (fn [s] {:step (:step s) :clause (:clause s)
                                  :estimated-rows (:estimated-rows s)
                                  :estimate-source (:estimate-source s)})
                         (:plan plan))
     :execute-ms (:ms exec)
     :execute-rows (:result exec)
     :end-to-end-ms (:ms total)
     :end-to-end-rows (:result total)}))

(defn -main [& args]
  (let [dir (first args)
        n-posts (parse-long (or (second args) "3000"))
        data (#'ldbc/load-subset dir n-posts)
        {:keys [db quads]} (#'ldbc/build-kotobase data)
        starts (#'ldbc/pick-persons data 3)
        p (first starts)]
    ;; warm the JIT once on the cheaper query
    (eng/query db ic09-one-hop everything [p])
    (prn
     {:schema 1
      :receipt/type :ic09-diagnosis
      :implementation "kotobase-peer.ic09-diag"
      :adr "ADR-2608021000 §6-1 -- print the plan before touching anything"
      :dataset {:subset-posts n-posts :quads quads
                :knows-quads (* 2 (count (:knows data)))
                :messages (count (:messages data))}
      :start-person p
      :analyses (mapv (fn [[label q]] (analyse db label q p))
                      [[:ic09-one-hop ic09-one-hop]
                       [:ic09-two-hop ic09-two-hop]])
      :reading
      (str "Compare probe-total-ms against execute-ms per query. The probes are "
           "what datalog-query-plan pays to ESTIMATE cardinality, by running each "
           "clause; execute-ms is the join itself. If the probes dominate, the "
           "cost is planning, not joining, and the fix is to stop estimating by "
           "full materialisation -- not to reorder anything.")
      :caveats
      ["Single start person, single run per measurement -- this is a diagnosis, not a benchmark. The receipt with percentiles is 2026-08-01-ldbc-snb-interactive.edn."
       "end-to-end-ms goes through kotobase-peer.core/query, which plans AND executes, so it should be roughly probe-total + plan overhead + execute."
       "Host load is recorded by whoever runs this; probe and execute are measured back to back, so their RATIO is meaningful even when the machine is busy."]})))
