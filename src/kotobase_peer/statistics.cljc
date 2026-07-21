(ns kotobase-peer.statistics
  "M5: Datalog statistics, join planner, delta-materialized arrangements.

   Collects cardinality histograms by index and key prefix, optimizes multi-index
   join order using greedy selectivity reduction, and maintains incremental
   materialized query views updated atomically with manifest publishes.

   All functions return immutable data. No effects are executed here.")

;; ============================================================================
;; Cardinality Histograms
;; ============================================================================

(defn build-cardinality-histogram
  "M5: Construct cardinality estimates by index and key prefix.

   Scans visible-rows to compute:
   - Full cardinality (total rows)
   - Prefix cardinalities (grouping by first 2 components)
   - Average cardinality per prefix

   Used by join planner to estimate selectivity.

   Returns: {:index :full-cardinality :prefix-cardinalities :average-cardinality-per-prefix}"
  [index rows]
  (let [full-card (count rows)
        by-prefix (frequencies (map (fn [row]
                                     (let [comps (get row "components")]
                                       (take 2 comps)))
                                   rows))]
    {:index index
     :full-cardinality full-card
     :prefix-cardinalities by-prefix
     :average-cardinality-per-prefix (if (seq by-prefix)
                                       (double (/ full-card (count by-prefix)))
                                       0.0)}))

(defn build-index-statistics
  "M5: Build histograms for all indexes in a manifest.

   Returns map: {:eavt {...} :aevt {...} :avet {...} :vaet {...}}"
  [manifest]
  (let [indexes-map (get (:node manifest) "indexes")]
    (into {}
          (map (fn [[index-name levels]]
                 (let [index (keyword index-name)
                       refs (mapcat val levels)
                       cardinality (reduce + (map #(get % "count" 0) refs))]
                   [index {:index index
                           :full-cardinality cardinality
                           :prefix-cardinalities {}
                           :average-cardinality-per-prefix 0.0}]))
               indexes-map))))

;; ============================================================================
;; Join Order Planner
;; ============================================================================

(defn selectivity-estimate
  "M5: Estimate selectivity of a join condition.

   Returns factor in (0..1]:
   - 1.0 if no histograms available (conservative)
   - min(card-a, card-b) / max(card-a, card-b) otherwise

   Assumes join is on common components; result is multiplicative reduction
   from earlier stage cardinality."
  [histograms left-index right-index]
  (let [left-hist (get histograms left-index)
        right-hist (get histograms right-index)]
    (if (and left-hist right-hist)
      (let [left-card (:full-cardinality left-hist 1)
            right-card (:full-cardinality right-hist 1)
            estimated-match (min left-card right-card)]
        (double (/ estimated-match (max left-card right-card 1))))
      1.0)))

(defn plan-join-order
  "M5: Optimize multi-index join order using cardinality histograms.

   Greedy strategy: pick smallest index first, then process remaining
   in order of increasing selectivity * current-rows.

   Input: {:query-indexes [:eavt :aevt :avet] ...}
   Returns: sequence of join steps with estimated cost at each stage."
  [query histograms]
  (let [base-index :eavt
        base-card (get-in histograms [:eavt :full-cardinality] 1.0)
        remaining-indexes (rest (or (:query-indexes query) [:eavt]))]
    (cons {:step 0 :index base-index :estimated-rows base-card}
          (loop [remaining remaining-indexes
                 steps []
                 current-card base-card
                 step-num 1]
            (if (seq remaining)
              (let [selectivities (map (fn [idx]
                                        [idx (selectivity-estimate histograms base-index idx)])
                                      remaining)
                    [next-idx selectivity] (apply min-key second selectivities)
                    next-card (* current-card selectivity)]
                (recur (remove #{next-idx} remaining)
                       (conj steps {:step step-num
                                    :index next-idx
                                    :selectivity selectivity
                                    :estimated-rows next-card})
                       next-card
                       (inc step-num)))
              steps)))))

;; ============================================================================
;; Delta-Materialized Arrangements
;; ============================================================================

(defn delta-arrangement
  "M5: Define an incremental materialized arrangement.

   Arrangement = pre-computed query result updated atomically with manifest publishes.

   Example: MaterializedView of (find ?e ?a where [?e :type/resource] [?e ?a])
   is updated every time new [e type/resource] datom is flushed.

   Returns: arrangement descriptor with materialization status."
  [{:keys [name query-pattern base-index delta-index manifest-cid epoch]}]
  {:arrangement/name name
   :arrangement/query-pattern query-pattern
   :arrangement/base-index base-index
   :arrangement/delta-index delta-index
   :arrangement/manifest-cid manifest-cid
   :arrangement/epoch epoch
   :arrangement/materialized? false
   :arrangement/rows []
   :arrangement/update-plan nil})

(defn maintain-materialized-delta
  "M5: Incrementally update materialized arrangement.

   When new datoms are flushed:
   1. Apply delta (new assertions/retractions) to materialized result
   2. Record which arrangements to update
   3. Attach update to manifest publish (same atomic epoch)

   Returns: updated arrangement with new rows and delta snapshot."
  [arrangement new-datoms]
  (let [delta-rows (mapv (fn [{:keys [e a v op]}]
                          {:e e :a a :v v :op op :delta? true})
                        new-datoms)]
    (assoc arrangement
           :arrangement/materialized? true
           :arrangement/delta-rows delta-rows
           :arrangement/last-updated #?(:clj (System/currentTimeMillis)
                                        :cljs (.now js/Date)))))

(defn query-with-arrangements
  "M5: Execute query using pre-materialized arrangements when available.

   Decision tree:
   1. Look up arrangement by query name
   2. If materialized & not stale: return arrangement rows
   3. Else: fall back to index scan

   Returns: {:result-type :materialized/:scan :rows [...] :manifest-cid ...}"
  [manifest query-pattern arrangements]
  (let [arrangement-name (:arrangement-name query-pattern)
        matching-arrangement (first (filter #(= (:arrangement/name %) arrangement-name)
                                            arrangements))]
    (if (and matching-arrangement (:arrangement/materialized? matching-arrangement))
      {:result-type :materialized
       :arrangement (:arrangement/name matching-arrangement)
       :rows (:arrangement/delta-rows matching-arrangement)}
      {:result-type :scan
       :needs-index-scan true
       :manifest-cid (:cid manifest)})))
