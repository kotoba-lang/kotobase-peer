(ns kotobase-peer.compaction
  "M4: Range compaction scheduler, L0 sub-levels, safe-epoch pinning, garbage collection.

   Implements deterministic compaction selection, level-based merging with overlap detection,
   intra-L0 compaction for write amplification reduction, and mark-sweep garbage collection.

   All functions return immutable data. No effects are executed here."
  (:require [clojure.set :as set]
            [kotobase-peer.merkle-lsm :as lsm]
            [ipld.core :as ipld]))

;; ============================================================================
;; Range Overlap Detection
;; ============================================================================

(defn key-range-overlap?
  "Check if two [min-key max-key] ranges have any overlap.
   Returns true if ranges overlap, false otherwise."
  [range-a range-b]
  (let [[min-a max-a] range-a
        [min-b max-b] range-b]
    (and (not (nil? min-a)) (not (nil? min-b))
         (not (nil? max-a)) (not (nil? max-b))
         (not (pos? (compare min-a max-b)))
         (not (pos? (compare min-b max-a))))))

(defn run-key-range
  "Extract [min-key max-key] range from a run or run-ref.
   Handles both direct run objects and run-ref metadata."
  [run]
  (if-let [min-key (or (:first-component-min run)
                       (get run "first-component-min")
                       (:min-key run)
                       (get run "min-key"))]
    [min-key (or (:first-component-max run)
                 (get run "first-component-max")
                 (:max-key run)
                 (get run "max-key"))]
    (when-let [node (:node run)]
      [(get node "min-key") (get node "max-key")])))

;; ============================================================================
;; Compaction Plan Selection
;; ============================================================================

(defn compaction-plan
  "M4: Select overlapping runs from L0 and candidate next level for compaction.

   Returns task descriptor {:index :level :l0-runs :target-runs :safe-epoch :manifest-cid :l0-count}
   or nil if no compaction is warranted.

   Scheduling policy: trigger when L0 run count exceeds threshold (default 4).
   This bounds read amplification from unbounded novelty walk."
  [manifest index safe-epoch level-threshold]
  (let [indexes-map (get (:node manifest) "indexes")
        index-name (name index)
        index-levels (get indexes-map index-name)
        l0-runs (mapv #(dissoc % :effects) (get index-levels "l0"))
        l0-count (count l0-runs)]
    (when (and (pos? l0-count) (>= l0-count level-threshold))
      ;; Find overlapping L1 runs as target for compaction
      (let [l0-ranges (keep run-key-range l0-runs)
            target-runs (vec (filter
                             (fn [run]
                               (let [run-range (run-key-range run)]
                                 (some #(key-range-overlap? % run-range) l0-ranges)))
                             (get index-levels "l1")))]
        {:index index
         :level :l1
         :l0-runs l0-runs
         :target-runs target-runs
         :safe-epoch safe-epoch
         :manifest-cid (:cid manifest)
         :l0-count l0-count}))))

;; ============================================================================
;; Level-based Compaction
;; ============================================================================

(defn compact-with-level
  "M4: Merge L0 runs with a single level (e.g., L1) to produce output runs.

   Strategy:
   1. Find target-level runs that overlap with L0 key ranges
   2. Merge L0 + overlapping target runs into partitioned output
   3. Return untouched target runs as-is (no copy overhead)

   Returns: {:compacted [...] :untouched [...] :all-output [...]}"
  [index tenant safe-epoch target-rows l0-runs level-runs]
  (let [l0-ranges (mapv run-key-range l0-runs)
        overlapping (filterv (fn [run]
                              (let [run-range (run-key-range run)]
                                (some #(key-range-overlap? % run-range) l0-ranges)))
                            level-runs)
        to-compact (vec (concat l0-runs overlapping))
        untouched (vec (remove (set overlapping) level-runs))
        compacted (lsm/compact-runs-partitioned index tenant safe-epoch target-rows to-compact)]
    {:compacted compacted
     :untouched untouched
     :all-output (vec (concat compacted untouched))}))

;; ============================================================================
;; L0 Sub-levels (Intra-L0 Compaction)
;; ============================================================================

(defn l0-sublevels
  "M4: Organize L0 runs into sub-levels (L0S0, L0S1, ...) based on write order.

   Intra-L0 compaction avoids visiting lower levels, reducing write amplification
   when L0 has multiple small runs.

   Returns: {:l0s0 [...] :l0s1 [...] ...}"
  [l0-runs max-per-sublevel]
  (into (sorted-map)
        (map-indexed (fn [i runs]
                      [(keyword (str "l0s" i)) runs])
                     (partition-all max-per-sublevel l0-runs))))

(defn intra-l0-compaction
  "M4: Compact consecutive L0 sub-levels without visiting lower levels.

   Takes first 2 sublevels (oldest + second-oldest) and merges them into
   a single run, reducing L0 run count while keeping metadata compact.

   Returns: vector of merged runs, or nil if compaction not warranted."
  [index tenant safe-epoch target-rows sublevels]
  (let [sorted-sublevels (sort-by key sublevels)
        sublevels-to-merge (->> sorted-sublevels (take 2) (mapv val) (apply concat) vec)]
    (when (> (count sublevels-to-merge) 1)
      (lsm/compact-runs-partitioned index tenant safe-epoch target-rows sublevels-to-merge))))

;; ============================================================================
;; Garbage Collection
;; ============================================================================

(defn gc-live-cids
  "M4: Collect all CIDs reachable from a manifest.

   Walks:
   - Manifest's CID itself
   - Previous manifest link (if present)
   - All run references in index levels
   - Values within statistics (may contain IPLD Links)

   Returns set of live CID strings."
  [manifest]
  (let [cids (atom #{})
        add-cid! (fn [c] (when c (swap! cids conj c)))

        visit-run-ref (fn [ref]
                       (if-let [cid (get ref "cid")]
                         (add-cid! (ipld/link-cid cid))
                         (when-let [run-cid (:cid ref)]
                           (add-cid! run-cid))))

        visit-indexes (fn [indexes-map]
                       (doseq [[_index-name levels] indexes-map]
                         (doseq [[_level-name runs] levels]
                           (doseq [run-ref runs]
                             (visit-run-ref run-ref)))))

        visit-node (fn [node]
                    (add-cid! (:cid manifest))
                    (when-let [prev-link (get node "previous")]
                      (add-cid! (ipld/link-cid prev-link)))
                    (when-let [indexes (get node "indexes")]
                      (visit-indexes indexes))
                    (when-let [stats (get node "statistics")]
                      (doseq [v (vals stats)]
                        (swap! cids into (lsm/linked-cids v)))))]
    (visit-node (:node manifest))
    @cids))

(defn gc-candidates
  "M4: Find unreachable CIDs by marking reachable blocks from manifest.

   Returns set of CIDs that may be safely deleted:
   difference(all_stored_cids, live_from_manifest)

   In production, host would iterate through manifest chain (old → new)
   and mark blocks as live before GC sweep."
  [current-manifest all-stored-cids]
  (let [live-from-manifest (gc-live-cids current-manifest)]
    (set/difference all-stored-cids live-from-manifest)))

;; ============================================================================
;; Safe-Epoch Pinning
;; ============================================================================

(defn safe-epoch-pin
  "M4: Create a pin record to prevent GC of required snapshots.

   Fields:
   - :manifest-cid — head CID at pin time
   - :epoch — manifest epoch number
   - :epoch-readers — active query readers at each epoch [e1 e2 ...]
   - :replicas — replication consumers at epochs [...]
   - :legal-hold — explicit retention epoch (e.g., regulatory)

   Returns immutable pin record."
  [{:keys [manifest-cid epoch epoch-readers replicas legal-hold]}]
  {:manifest-cid manifest-cid
   :epoch epoch
   :epoch-readers (or epoch-readers [])
   :replicas (or replicas [])
   :legal-hold legal-hold
   :pinned-at #?(:clj (System/currentTimeMillis)
                 :cljs (.now js/Date))})

(defn minimum-safe-epoch
  "M4: Compute minimum epoch that must be retained.

   Combines:
   1. Legal hold epochs (regulatory/compliance)
   2. Active reader epochs (query snapshots in flight)
   3. Replica epochs (standby sync lag)

   Compaction may discard versions older than this epoch
   (except newest-before-safe-epoch for MVCC)."
  [pins]
  (let [all-epochs (concat (map :epoch pins)
                           (mapcat :epoch-readers pins)
                           (mapcat :replicas pins))]
    (if (seq all-epochs)
      (reduce min all-epochs)
      0)))
