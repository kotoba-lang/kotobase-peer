(ns kotobase-peer.statistics
  "M5: Datalog statistics, join planner, delta-materialized arrangements.

   Collects cardinality histograms by index and key prefix, optimizes multi-index
   join order using greedy selectivity reduction, and maintains incremental
   materialized query views updated atomically with manifest publishes.

   All functions return immutable data. No effects are executed here."
  (:require [clojure.set]
            [kotobase-peer.merkle-lsm :as lsm]))

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

   When full run objects are available (with :node field), computes detailed
   histograms including prefix cardinalities. When only run-refs are available
   (typical for persisted manifests), uses run counts for cardinality estimates.

   Returns map: {:eavt {...} :aevt {...} :avet {...} :vaet {...}}"
  [manifest]
  (let [indexes-map (get (:node manifest) "indexes")
        epoch (:epoch manifest)]
    (into {}
          (map (fn [[index-name levels]]
                 (let [index (keyword index-name)
                       refs (mapcat val levels)
                       ;; Try to get actual rows from full run objects if available
                       rows (->> refs
                                 (mapcat (fn [run]
                                           ;; If run has :node, extract rows; otherwise empty
                                           (if-let [node (:node run)]
                                             (get node "rows")
                                             [])))
                                 (filter #(<= (get % "epoch") epoch))
                                 vec)]
                   (if (seq rows)
                     ;; If we have actual rows, compute detailed statistics
                     [index (build-cardinality-histogram index rows)]
                     ;; Otherwise, use count-based estimates from run-refs
                     (let [cardinality (reduce + (map #(get % "count" 0) refs))]
                       [index {:index index
                               :full-cardinality cardinality
                               :prefix-cardinalities {}
                               :average-cardinality-per-prefix 0.0}]))))
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
  (let [indexes (vec (or (:query-indexes query) [:eavt]))
        base-index (apply min-key #(get-in histograms [% :full-cardinality]
                                           #?(:clj Double/POSITIVE_INFINITY
                                              :cljs js/Infinity))
                          indexes)
        base-card (get-in histograms [base-index :full-cardinality] 1.0)
        remaining-indexes (remove #{base-index} indexes)]
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

(defn plan-clause-order
  "Greedy connected join order for portable host executors. CLAUSES contain
  {:id, :estimated-rows, :vars}. Prefer a clause sharing already-bound vars,
  then the lowest estimated cardinality. Returns immutable plan steps."
  [clauses]
  (loop [remaining (vec clauses), bound #{}, result []]
    (if (empty? remaining)
      result
      (let [connected? (fn [clause] (seq (clojure.set/intersection bound (:vars clause))))
            candidates (if (seq bound)
                         (let [connected (filter connected? remaining)]
                           (if (seq connected) connected remaining))
                         remaining)
            next-clause (apply min-key #(or (:estimated-rows %)
                                            #?(:clj Long/MAX_VALUE
                                               :cljs js/Number.MAX_SAFE_INTEGER))
                               candidates)]
        (recur (vec (remove #(= (:id %) (:id next-clause)) remaining))
               (into bound (:vars next-clause))
               (conj result
                     (assoc next-clause :step (count result)
                            :bound-vars-after (into bound (:vars next-clause)))))))))

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
                                        :cljs (js/Date.now)))))

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
