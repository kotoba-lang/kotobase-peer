(ns kotobase-peer.chunk-bench
  "ADR-2607244000 Stage B gate: is compaction peak LIVE SET independent of dataset
  size?

  Block reads being bounded (chunked-compaction-test) is necessary but not
  sufficient — the gate the ADR actually sets is memory. So blocks are spilled
  to a temp DIRECTORY and read back on demand, standing in for R2/B2, and the
  input runs are released before the measurement starts. Keeping the block
  store in a map would put the dataset in the heap and measure nothing.

  Reported: peak live set (heap after a forced collection) during compaction at
  each dataset size. Flat is the pass
  condition; a slope means something still co-resides.

  Run:
    clojure -M:chunk-bench
    clojure -M:chunk-bench 50000 200000"
  (:require [clojure.java.io :as io]
            [clojure.java.shell]
            [clojure.string]
            [ipld.core :as ipld]
            [kotobase-peer.merkle-lsm :as lsm])
  (:import [java.io File]))

(defn- live-bytes
  "Live heap after a forced collection.

  RSS was the wrong instrument: it is the JVM's high-water allocation, so with
  a large -Xmx it climbs with churn even when the live set is flat, and with a
  small one the JVM thrashes instead of failing. Neither tells you whether the
  algorithm holds O(dataset) or O(fan-in). Live-set-after-GC does."
  []
  (let [rt (Runtime/getRuntime)]
    (System/gc)
    (Thread/sleep 60)
    (- (.totalMemory rt) (.freeMemory rt))))

(defn- gb [b] (format "%.2f GB" (/ (double b) 1024 1024 1024)))
(defn- mb [b] (format "%.1f MB" (/ (double b) 1024 1024)))

(defn- spill-run!
  "Write every block of RUN to DIR as <cid>.edn and return the run node.
  The run value itself is dropped by the caller afterwards."
  [^File dir run]
  (doseq [b (or (:blocks run) [])]
    (spit (io/file dir (str (ipld/link-cid (get-in b [:descriptor "cid"])) ".edn"))
          (pr-str (:node b))))
  (:node run))

(defn- disk-fetch [^File dir]
  (fn [cid]
    (read-string (slurp (io/file dir (str cid ".edn"))))))

(defn- build-inputs!
  "R runs of N rows each, spilled to DIR. Interleaved key spaces so the merge
  is a real k-way merge with overlapping ranges."
  [dir runs-count rows-per-run]
  (mapv (fn [r]
          (let [entries (vec (for [i (range rows-per-run)]
                               {:components ["e" (format "a%09d" (+ r (* runs-count i)))]
                                :epoch 1 :op :assert :value (str r "-" i)}))
                run (lsm/build-run :eavt "t" entries
                                   {:block-rows 128 :max-block-bytes 1048576})
                node (spill-run! dir run)]
            node))
        (range runs-count)))

(defn- measure [runs-count rows-per-run target-rows]
  (let [dir (doto (File. (str (System/getProperty "java.io.tmpdir")
                              "/kotobase-chunk-bench-" (System/nanoTime)))
              (.mkdirs))
        nodes (build-inputs! dir runs-count rows-per-run)
        total (* runs-count rows-per-run)]
    (System/gc)
    (Thread/sleep 300)
    (let [baseline (live-bytes)
          peak (atom baseline)
          emitted (atom 0)
          rows (atom 0)
          t0 (System/nanoTime)
          summary (lsm/compact-chunks
                   {:index :eavt :tenant "t" :safe-epoch 0
                    :target-rows target-rows
                    :run-nodes nodes
                    :fetch-block (disk-fetch dir)}
                   (fn [run]
                     ;; emit! must persist and DROP; retaining here would
                     ;; recreate exactly the O(output) hold Stage A removed
                     (swap! emitted inc)
                     (swap! rows + (get (:node run) "count"))
                     ;; sampled, not every chunk: a forced collection per emit
                     ;; costs more than the compaction it is measuring
                     (when (zero? (mod @emitted 8))
                       (swap! peak max (live-bytes)))))
          elapsed-ms (/ (- (System/nanoTime) t0) 1e6)]
      (doseq [f (.listFiles dir)] (.delete f))
      (.delete dir)
      {:datoms total
       :runs-in runs-count
       :baseline-live baseline
       :peak-live @peak
       :delta (- @peak baseline)
       :out-runs @emitted
       :rows-out @rows
       :rows-conserved? (= total @rows)
       :chunks (:chunks summary)
       :blocks-read (:blocks-read summary)
       :elapsed-ms (long elapsed-ms)})))

(defn -main [& args]
  (let [sizes (if (seq args) (mapv #(Long/parseLong %) args) [50000 200000])
        runs-count 8
        target 4096]
    (println (format "chunk-bench: fan-in=%d target-rows=%d, blocks spilled to disk"
                     runs-count target))
    (println)
    (let [results (mapv (fn [total]
                          (let [r (measure runs-count (quot total runs-count) target)]
                            (println (format "datoms=%-8d peak live-set %s (delta %s)  chunks=%-4d blocks=%-6d rows-conserved=%s  %dms"
                                             (:datoms r) (mb (:peak-live r)) (mb (:delta r))
                                             (:chunks r) (:blocks-read r)
                                             (:rows-conserved? r) (:elapsed-ms r)))
                            r))
                        sizes)]
      (println)
      (let [[a b] [(first results) (last results)]
            d-datoms (- (:datoms b) (:datoms a))
            ;; from the ABSOLUTE peak, not from :delta. Delta is peak minus a
            ;; baseline taken after the same forced collection, so it is ~0 by
            ;; construction and a slope computed from it always prints 0.0 --
            ;; a number that reads like a pass while measuring nothing.
            ;;
            ;; Sampling is at chunk boundaries, so the in-chunk peak (k readers
            ;; x one block, plus the chunk's entries) is not observed here; that
            ;; bound comes from the design and the block-read counts in
            ;; chunked-compaction-test. What this establishes is that nothing
            ;; accumulates ACROSS chunks, which is where the old slope came from.
            d-rss (- (:peak-live b) (:peak-live a))]
        (when (pos? d-datoms)
          (println (format "live-set slope: %.1f bytes/datom  (the 1M RSS receipt measured ~2300)"
                           (/ (double d-rss) d-datoms))))
        (println "all rows conserved:" (every? :rows-conserved? results))))))
