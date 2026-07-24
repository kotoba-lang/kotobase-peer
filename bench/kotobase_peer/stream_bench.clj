(ns kotobase-peer.stream-bench
  "ADR-2607244000 gate harness: streaming/spill compaction under a BOUNDED
  heap. Unlike merkle-lsm-bench (pure in-memory: input run bytes + retained
  entries + output runs all co-resident -> measured ~4.5GB floor +
  ~2.3KB/datom), this harness SPILLS every run's bytes to disk immediately,
  holds only {path,count} refs, and compacts hierarchically through
  `compact-run-readers-streaming`:

  - level N merges at most FAN-IN readers per group, emitting each output
    run straight to disk;
  - one level-N GROUP becomes ONE level-N+1 reader -- a lazy concatenation
    of the group's ordered output runs, decoded run-by-run as the merge
    consumes them (a group's output is internally sorted, so sequential
    concatenation preserves the per-reader sorted-rows contract);
  - the level whose readers fit a single group merges EVERYTHING -- its
    merged row sequence equals the global sorted sequence (retention is
    per-level idempotent, see retained-entries-from-row-seqs), so the final
    partitioned runs are identical to a single global pass.

  Peak co-resident rows are O(fan-in x max(batch, target-rows)) per level,
  independent of total dataset size -- the property the gates measure.

  gate-1M: `clojure -J-Xmx4g -M:stream-bench 1000000` must complete with
  rows conserved. gate-10M: `-J-Xmx8g ... 10000000`."
  (:require [clojure.java.io :as io]
            [ipld.core :as ipld]
            [kotobase-peer.merkle-lsm :as lsm]))

(defn- elapsed-ms [f]
  (let [started (System/nanoTime) result (f)]
    {:result result :ms (/ (- (System/nanoTime) started) 1e6)}))

(defn- datom [i]
  {:components [(str "entity-" i) "bench/value" (str "value-" i)]
   :epoch (inc i) :op :assert :value (str "value-" i)})

(defn- spill-run!
  "Persist a run as a content-addressed block store on disk: EVERY effect
  block (root node + row blocks -- rows live in separate row blocks for runs
  over default-run-block-rows, so spilling only the root would lose the
  data, the bug this replaced). Returns a light ref {:root cid :count n}."
  [dir run]
  (doseq [{:keys [cid bytes]} (:effects run)]
    (let [f (io/file dir (str cid ".bin"))]
      (when-not (.exists f)
        (with-open [out (io/output-stream f)]
          (.write out ^bytes bytes)))))
  {:root (:cid run) :count (:count run)})

(defn- load-block [dir cid]
  (with-open [in (io/input-stream (io/file dir (str cid ".bin")))]
    (ipld/decode (.readAllBytes in))))

(defn- ref-reader
  "Rows of one spilled run, decoding the root then streaming row blocks
  one at a time (<=128 rows co-resident per open run)."
  [dir {:keys [root]}]
  (fn []
    (let [node (load-block dir root)]
      (if-some [rows (get node "rows")]
        rows
        (mapcat (fn [desc] (get (load-block dir (ipld/link-cid (get desc "cid"))) "rows"))
                (get node "blocks"))))))

(defn- group-reader
  "One reader over a merge group's ORDERED output refs: lazily concatenates
  their rows, so a consumed run's decoded blocks become collectible."
  [dir refs]
  (fn [] (mapcat (fn [ref] ((ref-reader dir ref))) refs)))

(defn- compact-group!
  "Merge one group of readers -> spilled ordered refs for the next level."
  [dir target-rows readers]
  (let [out (atom [])]
    (lsm/compact-run-readers-streaming
     :eavt "bench" 0 target-rows readers
     (fn [run] (swap! out conj (spill-run! dir run))))
    @out))

(defn measure [n target-rows fan-in]
  (let [dir (io/file "target" (str "stream-bench-" n "-" (System/currentTimeMillis)))
        _ (.mkdirs dir)
        batch-size 1000
        build (elapsed-ms
               (fn []
                 (->> (partition-all batch-size (map datom (range n)))
                      (mapv (fn [batch] (spill-run! dir (lsm/build-run :eavt "bench" batch)))))))
        compact
        (elapsed-ms
         (fn []
           (loop [readers (mapv #(ref-reader dir %) (:result build))]
             (let [groups (vec (partition-all fan-in readers))
                   group-refs (mapv (fn [rs] (compact-group! dir target-rows (vec rs))) groups)]
               (if (= 1 (count groups))
                 (first group-refs)
                 (recur (mapv #(group-reader dir %) group-refs)))))))
        final-refs (:result compact)
        total-rows (reduce + (map :count final-refs))
        result {:datoms n :target-rows target-rows :fan-in fan-in
                :build-ms (:ms build)
                :compact-ms (:ms compact)
                :total-ms (+ (:ms build) (:ms compact))
                :throughput-per-sec (/ (* n 1000.0) (+ (:ms build) (:ms compact)))
                :final-runs (count final-refs) :final-rows total-rows}]
    (doseq [f (reverse (file-seq dir))] (.delete ^java.io.File f))
    result))

(defn -main [& args]
  (let [sizes (if (seq args) (mapv parse-long args) [10000])
        target-rows (parse-long (or (System/getenv "STREAM_BENCH_TARGET_ROWS") "4096"))
        fan-in (parse-long (or (System/getenv "STREAM_BENCH_FAN_IN") "64"))
        results (mapv #(measure % target-rows fan-in) sizes)]
    (doseq [r results] (prn r))
    (when-not (every? #(= (:datoms %) (:final-rows %)) results)
      (throw (ex-info "stream bench lost rows" {:results results})))
    (shutdown-agents)))
