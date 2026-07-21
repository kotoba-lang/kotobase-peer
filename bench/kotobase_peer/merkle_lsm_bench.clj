(ns kotobase-peer.merkle-lsm-bench
  "Repeatable Merkle-LSM scale gate. Pass 1000 100000 10000000 explicitly for
  the ADR release sweep; defaults stay small enough for a local pre-push gate."
  (:require [kotobase-peer.merkle-lsm :as lsm]))

(defn- elapsed-ms [f]
  (let [started (System/nanoTime) result (f)]
    {:result result :ms (/ (- (System/nanoTime) started) 1e6)}))

(defn- percentile [values p]
  (nth (vec (sort values))
       (min (dec (count values)) (long (Math/ceil (* p (count values)))))))

(defn- datom [i]
  {:components [(str "entity-" i) "bench/value" (str "value-" i)]
   :epoch (inc i) :op :assert :value (str "value-" i)})

(defn measure [n writers target-rows]
  (let [batch-size 1000
        batches (vec (partition-all batch-size (map datom (range n))))
        timings (atom [])
        started (System/nanoTime)
        runs (->> batches
                  (partition-all (max 1 writers))
                  (mapcat (fn [wave]
                            (->> wave
                                 (mapv (fn [batch]
                                         (future
                                           (let [{:keys [result ms]}
                                                 (elapsed-ms
                                                  #(lsm/build-run :eavt "bench" batch))]
                                             (swap! timings conj ms)
                                             result))))
                                 (mapv deref))))
                  vec)
        compact (elapsed-ms #(lsm/compact-runs-partitioned
                              :eavt "bench" 0 target-rows runs))
        compacted-runs (:result compact)
        total-ms (/ (- (System/nanoTime) started) 1e6)]
    {:datoms n :writers writers :batches (count batches)
     :total-ms total-ms :throughput-per-sec (/ (* n 1000.0) total-ms)
     :flush-p50-ms (percentile @timings 0.50)
     :flush-p95-ms (percentile @timings 0.95)
     :flush-p99-ms (percentile @timings 0.99)
     :compact-ms (:ms compact)
     :compacted-runs (count compacted-runs)
     :max-run-count (reduce max 0 (map :count compacted-runs))
     :compacted-count (reduce + (map :count compacted-runs))}))

(defn -main [& args]
  (let [sizes (if (seq args) (mapv parse-long args) [1000 10000])
        writers (parse-long (or (System/getenv "MERKLE_BENCH_WRITERS") "32"))
        target-rows (parse-long (or (System/getenv "MERKLE_BENCH_TARGET_ROWS") "4096"))
        results (mapv #(measure % writers target-rows) sizes)]
    (doseq [result results] (prn result))
    (when-not (every? #(= (:datoms %) (:compacted-count %)) results)
      (throw (ex-info "Merkle benchmark lost rows" {:results results})))
    (shutdown-agents)))
