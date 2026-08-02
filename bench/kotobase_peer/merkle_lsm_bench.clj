(ns kotobase-peer.merkle-lsm-bench
  "Repeatable Merkle-LSM scale gate. Pass 1000 100000 10000000 explicitly for
  the ADR release sweep; defaults stay small enough for a local pre-push gate."
  (:require [merkle-lsm.core :as lsm]
            [kotobase-peer.performance-gate :as gate])
  (:import [java.lang.management ManagementFactory]
           [com.sun.management OperatingSystemMXBean]))

(defn- elapsed-ms [f]
  (let [started (System/nanoTime) result (f)]
    {:result result :ms (/ (- (System/nanoTime) started) 1e6)}))

(defn- percentile [values p]
  (nth (vec (sort values))
       (min (dec (count values))
            (dec (long (Math/ceil (* p (count values))))))))

(defn- datom [i]
  {:components [(str "entity-" i) "bench/value" (str "value-" i)]
   :epoch (inc i) :op :assert :value (str "value-" i)})

(defn- used-heap-bytes []
  (let [runtime (Runtime/getRuntime)]
    (- (.totalMemory runtime) (.freeMemory runtime))))

(defn- process-cpu-ms []
  (let [bean (ManagementFactory/getOperatingSystemMXBean)]
    (when (instance? OperatingSystemMXBean bean)
      (/ (.getProcessCpuTime ^OperatingSystemMXBean bean) 1e6))))

(defn- object-metrics [runs]
  (let [effects (->> runs (mapcat :effects)
                     (filter #(= :block/put (:effect/type %)))
                     vec)]
    {:objects (count effects)
     :bytes (reduce + 0 (map #(alength ^bytes (:bytes %)) effects))}))

(defn- with-peak-heap [f]
  (let [running? (atom true)
        peak (atom (used-heap-bytes))
        sampler (future
                  (while @running?
                    (swap! peak max (used-heap-bytes))
                    (Thread/sleep 5)))]
    (try
      (let [result (f)]
        (swap! peak max (used-heap-bytes))
        {:result result :peak-heap-bytes @peak})
      (finally
        (reset! running? false)
        @sampler))))

(defn measure [n writers target-rows]
  (System/gc)
  (Thread/sleep 50)
  (let [heap-before (used-heap-bytes)
        cpu-before (process-cpu-ms)
        measured
        (with-peak-heap
          (fn []
            (let [batch-size 1000
                  batches (partition-all batch-size (map datom (range n)))
                  timings (atom [])
                  started (System/nanoTime)
                  runs (->> batches
                            (partition-all (max 1 writers))
                            (mapcat
                             (fn [wave]
                               (->> wave
                                    (mapv
                                     (fn [batch]
                                       (future
                                         (let [{:keys [result ms]}
                                               (elapsed-ms
                                                #(lsm/build-run
                                                  :eavt "bench" batch))]
                                           (swap! timings conj ms)
                                           result))))
                                    (mapv deref))))
                            vec)
                  flush-storage (object-metrics runs)
                  compact (elapsed-ms
                           #(lsm/compact-runs-partitioned
                             :eavt "bench" 0 target-rows runs))
                  compacted-runs (:result compact)
                  compact-storage (object-metrics compacted-runs)
                  total-ms (/ (- (System/nanoTime) started) 1e6)]
              {:datoms n :writers writers
               :batches (long (Math/ceil (/ n (double batch-size))))
               :total-ms total-ms
               :throughput-per-sec (/ (* n 1000.0) total-ms)
               :flush-p50-ms (percentile @timings 0.50)
               :flush-p95-ms (percentile @timings 0.95)
               :flush-p99-ms (percentile @timings 0.99)
               :compact-ms (:ms compact)
               :input-runs (count runs)
               :compacted-runs (count compacted-runs)
               :max-run-count (reduce max 0 (map :count compacted-runs))
               :compacted-count (reduce + (map :count compacted-runs))
               :flush-objects (:objects flush-storage)
               :flush-object-bytes (:bytes flush-storage)
               :compact-objects (:objects compact-storage)
               :compact-object-bytes (:bytes compact-storage)
               :write-amplification
               (/ (+ (:bytes flush-storage) (:bytes compact-storage))
                  (double (max 1 (:bytes flush-storage))))
               :read-amplification
               (/ (count runs) (double (max 1 (count compacted-runs))))})))
        result (:result measured)
        cpu-after (process-cpu-ms)]
    (assoc result
           :heap-before-bytes heap-before
           :peak-heap-bytes (:peak-heap-bytes measured)
           :peak-heap-delta-bytes
           (max 0 (- (:peak-heap-bytes measured) heap-before))
           :cpu-ms (if (and cpu-before cpu-after)
                     (max 0 (- cpu-after cpu-before))
                     0))))

(defn -main [& args]
  (let [sizes (if (seq args) (mapv parse-long args) [1000 10000])
        writers (parse-long (or (System/getenv "MERKLE_BENCH_WRITERS") "32"))
        target-rows (parse-long (or (System/getenv "MERKLE_BENCH_TARGET_ROWS") "4096"))
        enforce? (= "1" (System/getenv "MERKLE_BENCH_ENFORCE_GATE"))
        results (mapv (fn [size]
                        (let [metrics (measure size writers target-rows)]
                          (assoc metrics :gate (gate/evaluate metrics))))
                      sizes)]
    (doseq [result results] (prn result))
    (when-not (every? #(= (:datoms %) (:compacted-count %)) results)
      (throw (ex-info "Merkle benchmark lost rows" {:results results})))
    (when (and enforce? (not-every? #(get-in % [:gate :passed?]) results))
      (throw (ex-info "Merkle scale resource gate failed"
                      {:results results})))
    (shutdown-agents)))
