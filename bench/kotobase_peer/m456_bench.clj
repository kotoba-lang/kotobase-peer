(ns kotobase-peer.m456-bench
  "M4–M6 Merkle-LSM performance benchmarks.

   Measures:
   - M4 compaction scheduling and GC latency
   - M5 statistics collection and join planning

   Pass sizes explicitly for release sweeps; defaults remain local-test scale."
  (:require [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.compaction :as compaction]
            [kotobase-peer.statistics :as statistics]))

;; ============================================================================
;; Utilities
;; ============================================================================

(defn- elapsed-ms [f]
  (let [started (System/nanoTime) result (f)]
    {:result result :ms (/ (- (System/nanoTime) started) 1e6)}))

(defn- percentile [values p]
  (nth (vec (sort values))
       (min (dec (count values)) (long (Math/ceil (* p (count values)))))))

(defn- datom [i]
  {:components [(str "entity-" i) "bench/value" (str "value-" i)]
   :epoch (inc i) :op :assert :value (str "value-" i)})

;; ============================================================================
;; M4 Compaction Benchmarks
;; ============================================================================

(defn bench-m4-compaction-plan
  "Benchmark compaction plan selection overhead."
  [run-count]
  (let [batch-size 1000
        batch (vec (take batch-size (map datom (range))))
        runs (vec (repeatedly run-count #(lsm/build-run :eavt "bench" batch)))
        manifest (lsm/build-manifest
                 {:db-id "test" :epoch 1
                  :indexes {:eavt {:l0 (take (inc (quot run-count 2)) runs)
                                   :l1 (drop (inc (quot run-count 2)) runs)}}})
        timings (atom [])]
    (dotimes [_ 10]
      (let [{:keys [ms]} (elapsed-ms #(compaction/compaction-plan manifest :eavt 0 4))]
        (swap! timings conj ms)))
    {:operation :m4-compaction-plan
     :run-count run-count
     :p50-ms (percentile @timings 0.50)
     :p95-ms (percentile @timings 0.95)
     :p99-ms (percentile @timings 0.99)
     :avg-ms (/ (reduce + @timings) (count @timings))}))

(defn bench-m4-gc-candidates
  "Benchmark GC candidate collection overhead."
  [block-count]
  (let [batch (vec (take 100 (map datom (range))))
        run (lsm/build-run :eavt "bench" batch)
        manifest (lsm/build-manifest
                 {:db-id "test" :epoch 1
                  :indexes {:eavt {:l0 [run]}}})
        all-cids (into #{} (range block-count))
        timings (atom [])]
    (dotimes [_ 10]
      (let [{:keys [ms]} (elapsed-ms #(compaction/gc-candidates manifest all-cids))]
        (swap! timings conj ms)))
    {:operation :m4-gc-candidates
     :block-count block-count
     :p50-ms (percentile @timings 0.50)
     :p95-ms (percentile @timings 0.95)
     :p99-ms (percentile @timings 0.99)
     :avg-ms (/ (reduce + @timings) (count @timings))}))

(defn bench-m4-safe-epoch-pin
  "Benchmark safe-epoch pin creation and minimum epoch calculation."
  [pin-count]
  (let [pins (vec (repeatedly
                   pin-count
                   (fn []
                     (compaction/safe-epoch-pin
                      {:manifest-cid (str "m" (rand-int 1000))
                       :epoch (rand-int 100)
                       :epoch-readers (vec (repeatedly (rand-int 5)
                                                      (fn [] (rand-int 100))))}))))
        timings (atom [])]
    (dotimes [_ 10]
      (let [{:keys [ms]} (elapsed-ms #(compaction/minimum-safe-epoch pins))]
        (swap! timings conj ms)))
    {:operation :m4-safe-epoch-pin
     :pin-count pin-count
     :p50-ms (percentile @timings 0.50)
     :p95-ms (percentile @timings 0.95)
     :p99-ms (percentile @timings 0.99)
     :avg-ms (/ (reduce + @timings) (count @timings))}))

;; ============================================================================
;; M5 Statistics Benchmarks
;; ============================================================================

(defn bench-m5-histogram-collection
  "Benchmark cardinality histogram building."
  [row-count]
  (let [rows (mapv (fn [i]
                     {"key" (lsm/canonical-key :eavt "t" ["e" "a" (str i)] i)
                      "components" ["e" "a" (str i)]
                      "epoch" i
                      "op" "assert"
                      "value" i})
                   (range row-count))
        timings (atom [])]
    (dotimes [_ 10]
      (let [{:keys [ms]} (elapsed-ms
                          #(statistics/build-cardinality-histogram :eavt rows))]
        (swap! timings conj ms)))
    {:operation :m5-histogram-collection
     :row-count row-count
     :p50-ms (percentile @timings 0.50)
     :p95-ms (percentile @timings 0.95)
     :p99-ms (percentile @timings 0.99)
     :avg-ms (/ (reduce + @timings) (count @timings))}))

(defn bench-m5-join-planner
  "Benchmark join order planning."
  [index-count]
  (let [histograms (into {}
                         (map (fn [i]
                               [((vec [:eavt :aevt :avet :vaet]) (mod i 4))
                                {:full-cardinality (/ 1000.0 (inc i))}])
                             (range index-count)))
        query {:query-indexes (take index-count [:eavt :aevt :avet :vaet])}
        timings (atom [])]
    (dotimes [_ 10]
      (let [{:keys [ms]} (elapsed-ms
                          #(statistics/plan-join-order query histograms))]
        (swap! timings conj ms)))
    {:operation :m5-join-planner
     :index-count index-count
     :p50-ms (percentile @timings 0.50)
     :p95-ms (percentile @timings 0.95)
     :p99-ms (percentile @timings 0.99)
     :avg-ms (/ (reduce + @timings) (count @timings))}))

(defn bench-m5-delta-maintenance
  "Benchmark delta arrangement maintenance."
  [datom-count]
  (let [arr (statistics/delta-arrangement {:name :bench :base-index :eavt})
        datoms (vec (repeatedly datom-count
                               #(hash-map :e "e1" :a :test :v (rand-int 1000) :op :assert)))
        timings (atom [])]
    (dotimes [_ 10]
      (let [{:keys [ms]} (elapsed-ms
                          #(statistics/maintain-materialized-delta arr datoms))]
        (swap! timings conj ms)))
    {:operation :m5-delta-maintenance
     :datom-count datom-count
     :p50-ms (percentile @timings 0.50)
     :p95-ms (percentile @timings 0.95)
     :p99-ms (percentile @timings 0.99)
     :avg-ms (/ (reduce + @timings) (count @timings))}))

;; ============================================================================
;; Main Benchmark Driver
;; ============================================================================

(defn -main [& _args]
  (let [m4-run-count (or (some-> (System/getenv "M4_RUN_COUNT") parse-long) 100)
        m4-block-count (or (some-> (System/getenv "M4_BLOCK_COUNT") parse-long) 10000)
        m4-pin-count (or (some-> (System/getenv "M4_PIN_COUNT") parse-long) 100)
        m5-row-count (or (some-> (System/getenv "M5_ROW_COUNT") parse-long) 100000)
        m5-index-count (or (some-> (System/getenv "M5_INDEX_COUNT") parse-long) 4)
        m5-datom-count (or (some-> (System/getenv "M5_DATOM_COUNT") parse-long) 1000)

        results (vec (concat
                     [(bench-m4-compaction-plan m4-run-count)
                      (bench-m4-gc-candidates m4-block-count)
                      (bench-m4-safe-epoch-pin m4-pin-count)]
                     [(bench-m5-histogram-collection m5-row-count)
                      (bench-m5-join-planner m5-index-count)
                      (bench-m5-delta-maintenance m5-datom-count)]))]
    (doseq [result results]
      (prn result))
    (shutdown-agents)))
