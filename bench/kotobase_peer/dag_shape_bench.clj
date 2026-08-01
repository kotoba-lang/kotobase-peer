(ns kotobase-peer.dag-shape-bench
  "ADR-2607310900 訂正4's explicit precondition: \"次に試す前に、DAG の深さと
  幅を実測すること\" -- measure the block DAG's depth and width BEFORE
  attempting another prefetch strategy.

  Why this exists: the production hydrate (kotobase.net, p50 2,282 ms = 92%
  of query time) walks blocks through `kotobase.server.trampoline/with-blocks`,
  which discovers exactly ONE new block per pass and re-runs the whole
  computation from scratch on each miss. net-kotobase#266 tried to make those
  misses free with a breadth-first parallel block prefetch and made hydration
  ~24% WORSE (reverted, #267). The ADR's stated reason: breadth-first only
  helps when the DAG is WIDE, and the chain's novelty \"は chain なので\"
  depth may track novelty length rather than prolly-tree height. That was a
  hypothesis. This harness measures it.

  What is measured (structure, not wall-clock):
    * prolly-tree: height, nodes per level (the width profile), leaves,
      entries, fanout.
    * sync trampoline: SEQUENTIAL round trips and total node decodes for the
      full-prefix scan `hydrate!` actually issues (`hot-datoms` with no
      :components -> `scan-prefix` with prefix \"\"). Simulated by running the
      REAL `pt/scan-prefix` against a real throw-on-miss get-fn inside a real
      retry loop -- the same algorithm as `with-blocks`, only the fetch is
      local, so the COUNTS are exact even though the latency is not.
    * async (`scan-prefix-async`) equivalent: sequential WAVES and decodes,
      by walking the same tree level-batched.
    * novelty: the not-yet-folded cons-chain / segment-index walk, which is a
      pointer chase and therefore a separate depth term no prefetch can
      flatten.

  Round-trip COUNTS are the product here. Wall-clock is then projected by
  multiplying against two independently-measured R2 latencies (bench/results/
  2026-07-21-materialized-view-100k.edn's p50 21 ms in NRT, and the ~90 ms
  the ADR attributes to the production path) -- projection, labelled as such,
  never presented as an end-to-end measurement.

  Usage: clojure -M:dag-shape-bench [docs] [novelty-tx]"
  (:require [kotobase-peer.core :as eng]
            [clojure.string :as str]
            [ipld.core :as ipld]
            [prolly-tree.core :as pt])
  (:import [javax.crypto Cipher Mac]
           [javax.crypto.spec SecretKeySpec GCMParameterSpec]
           [java.util Base64]))

;; ── same crypto harness as the other head-to-head benches ──────────────────
(def ^:private test-dek (SecretKeySpec. (byte-array (range 1 33)) "AES"))
(def ^:private test-blind-key (SecretKeySpec. (byte-array (range 33 65)) "HmacSHA256"))
(def ^:private test-nonce-key (SecretKeySpec. (byte-array (range 65 97)) "HmacSHA256"))

(defn- test-encrypt-fn [^bytes plaintext]
  (let [mac (doto (Mac/getInstance "HmacSHA256") (.init test-nonce-key))
        nonce (byte-array (take 12 (.doFinal mac plaintext)))
        cipher (Cipher/getInstance "AES/GCM/NoPadding")]
    (.init cipher Cipher/ENCRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
    (byte-array (concat nonce (.doFinal cipher plaintext)))))

(defn- test-decrypt-fn [^bytes blob]
  (let [nonce (byte-array (take 12 blob))
        ct (byte-array (drop 12 blob))
        cipher (Cipher/getInstance "AES/GCM/NoPadding")]
    (.init cipher Cipher/DECRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
    (.doFinal cipher ct)))

(defn- test-blind-fn [component]
  (let [mac (doto (Mac/getInstance "HmacSHA256") (.init test-blind-key))]
    (.encodeToString (Base64/getEncoder) (.doFinal mac (.getBytes (pr-str component) "UTF-8")))))

;; ── production-shaped workload ─────────────────────────────────────────────
;; kotobase-protocols-worker.kotobase-store/hydrate! rebuilds a LocalStore
;; from "doc/coll"+"doc/key"+"doc/val" rows via ONE :aevt hot-datoms scan.
;; Three datoms per doc, exactly as diff->tx-data! writes them.

(defn- doc-quads [i]
  (let [eid (str "doc:" i)]
    [{:s eid :p "doc/coll" :o (pr-str "nodes")}
     {:s eid :p "doc/key" :o (pr-str (str "k" i))}
     {:s eid :p "doc/val" :o (pr-str {:i i :payload (str "payload-" i)})}]))

;; ── the two block-discovery strategies, counted ────────────────────────────

(defn- trampolined-scan
  "Run the REAL `pt/scan-prefix` under the REAL with-blocks algorithm: a
  throw-on-miss sync get-fn over an in-memory cache, retried from scratch on
  each miss. Returns the scan result plus the exact cost counters.

  `:round-trips` is the number of SEQUENTIAL fetches -- with-blocks awaits
  each one before the retry that discovers the next, so this is the count
  that multiplies by network RTT. `:node-decodes` is how many times a block
  was decoded, which is the CPU term (the trampoline caches bytes, never
  decoded nodes, so every retry re-decodes everything it already walked)."
  [store root-cid prefix]
  (let [cache (atom {})
        round-trips (atom 0)
        decodes (atom 0)
        get-fn (fn [cid]
                 (if-let [b (get @cache cid)]
                   (do (swap! decodes inc) b)
                   (throw (ex-info "block-miss" {:block-miss true :cid cid}))))]
    (loop [passes 0]
      (let [outcome (try
                      {:ok (pt/scan-prefix get-fn root-cid prefix)}
                      (catch clojure.lang.ExceptionInfo e
                        (if (:block-miss (ex-data e))
                          {:miss (:cid (ex-data e))}
                          (throw e))))]
        (if-let [cid (:miss outcome)]
          (do (swap! cache assoc cid (get store cid))
              (swap! round-trips inc)
              (recur (inc passes)))
          {:entries (count (:ok outcome))
           :round-trips @round-trips
           :passes (inc passes)
           :node-decodes @decodes})))))

(defn- level-batched-scan
  "The `scan-prefix-async` strategy, counted: fetch each internal node's
  surviving children CONCURRENTLY (one wave per tree level) and decode each
  node exactly once. `:waves` is the sequential term -- what multiplies by
  RTT -- and is bounded by tree HEIGHT, not by block count."
  [store root-cid prefix]
  (let [decodes (atom 0)
        node (fn [cid] (swap! decodes inc) (ipld/decode (get store cid)))
        past? (fn [k] (and (pos? (compare k prefix))
                           (not (str/starts-with? k prefix))))
        children-to-walk
        (fn [n]
          (loop [cs (get n "children") prev nil acc []]
            (if (empty? cs)
              acc
              (let [[mk link] (first cs)]
                (cond
                  (and (some? prev) (past? prev)) acc
                  (neg? (compare mk prefix)) (recur (rest cs) mk acc)
                  :else (recur (rest cs) mk (conj acc (ipld/link-cid link))))))))]
    (loop [frontier [root-cid] waves 0 leaves 0 entries 0]
      (if (empty? frontier)
        {:waves waves :node-decodes @decodes :leaves leaves :entries entries}
        (let [nodes (mapv node frontier)          ; one concurrent wave
              leaf? (fn [n] (= "leaf" (get n "kind")))
              ls (filter leaf? nodes)
              next-frontier (vec (mapcat children-to-walk (remove leaf? nodes)))]
          (recur next-frontier (inc waves)
                 (+ leaves (count ls))
                 (+ entries (reduce + 0 (map #(count (filter (fn [[k _]] (str/starts-with? k prefix))
                                                             (get % "entries"))) ls)))))))))

(defn- tree-shape
  "Depth and per-level width of the whole tree (no prefix pruning) --
  the structural fact ADR-2607310900 訂正4 asked for."
  [store root-cid]
  (loop [frontier [root-cid] level 0 widths [] leaves 0 entries 0 nodes 0]
    (if (empty? frontier)
      {:height level :widths widths :nodes nodes :leaves leaves :entries entries
       :max-width (if (seq widths) (apply max widths) 0)}
      (let [ns* (mapv #(ipld/decode (get store %)) frontier)
            leaf? (fn [n] (= "leaf" (get n "kind")))
            ls (filter leaf? ns*)
            kids (vec (mapcat (fn [n] (map (fn [[_ link]] (ipld/link-cid link)) (get n "children")))
                              (remove leaf? ns*)))]
        (recur kids (inc level) (conj widths (count frontier))
               (+ leaves (count ls))
               (+ entries (reduce + 0 (map #(count (get % "entries")) ls)))
               (+ nodes (count frontier)))))))

(defn- novelty-shape
  "The unfolded-novelty half of a hydrate. Novelty is a cons chain of
  {\"e\" tx-link \"rest\" rest-link} nodes (or, once the subject directory is
  live, 16-entry segments chained by \"rest\"). Either way the successor CID
  is only known AFTER its predecessor is decoded -- a pointer chase. Its hop
  count is a depth term that NO prefetch strategy can flatten, because the
  addresses do not exist until you have read the block that names them."
  [store state]
  (let [walk (fn [cid]
               (loop [c cid hops 0 txs 0]
                 (if (nil? c)
                   {:hops hops :txs txs}
                   (let [n (ipld/decode (get store c))]
                     (recur (some-> (get n "rest") ipld/link-cid)
                            (inc hops)
                            (+ txs (if (contains? n "entries")
                                     (count (get n "entries"))
                                     1)))))))
        front (walk (some-> (get state "novelty-front") ipld/link-cid))
        back (walk (some-> (get state "novelty-back") ipld/link-cid))
        idx (walk (some-> (get state "novelty-subject-index") ipld/link-cid))]
    {:novelty-count (get state "novelty-count" 0)
     :front front :back back :subject-index idx
     :sequential-hops (+ (:hops front) (:hops back))}))

;; ── projection ─────────────────────────────────────────────────────────────

(def ^:private rtt-samples
  "Independently measured R2 latencies, NOT measured by this harness.
  21 ms: bench/results/2026-07-21-materialized-view-100k.edn (real R2, NRT,
  42 KB range GET, p50 over 50 samples). 90 ms: the per-round-trip figure
  ADR-2607310900 訂正4 attributes to the production path (~41 blocks / ~21
  passes reconciling to the observed 2.2 s)."
  {:r2-nrt-p50-ms 21 :adr-production-ms 90})

(defn- project [round-trips]
  (into {} (map (fn [[k ms]] [k (double (* round-trips ms))]) rtt-samples)))

;; ── main ───────────────────────────────────────────────────────────────────

(defn- build!
  "Commit `docs` docs, fold them into an indexed snapshot, then commit
  `novelty-tx` further transactions and leave them UNFOLDED -- the state a
  live graph is actually in between folds."
  [docs novelty-tx]
  (let [store (atom {})
        put! (fn [cid bytes] (swap! store assoc cid bytes))
        get-fn (fn [cid] (get @store cid))
        batch 500
        chain (reduce (fn [c lo]
                        (eng/commit! put! get-fn
                                     (vec (mapcat doc-quads (range lo (min docs (+ lo batch)))))
                                     c test-encrypt-fn))
                      nil (range 0 docs batch))
        folded (eng/fold! put! get-fn chain test-blind-fn test-encrypt-fn test-decrypt-fn)
        with-novelty (reduce (fn [c i]
                               (eng/commit! put! get-fn
                                            (doc-quads (+ docs i))
                                            c test-encrypt-fn))
                             folded (range novelty-tx))]
    {:store @store :get-fn get-fn :chain with-novelty :folded folded}))

(defn- measure [docs novelty-tx]
  (binding [*out* *err*] (println "measuring docs=" docs "novelty=" novelty-tx) (flush))
  (let [{:keys [store get-fn chain]} (build! docs novelty-tx)
        state (#'eng/state-at get-fn chain)
        snap-cid (eng/latest-snapshot-cid get-fn chain)
        snap (ipld/decode (get store snap-cid))
        ;; hydrate! reads :aevt -> the "pso" index root (kotobase-peer.core/index-spec)
        root (some-> (get-in snap ["index-roots" "pso"]) ipld/link-cid)
        shape (tree-shape store root)
        sync-cost (trampolined-scan store root "")
        async-cost (level-batched-scan store root "")
        nov (novelty-shape store state)
        ;; A hydrate pays BOTH halves. The snapshot half's round trips are
        ;; strategy-dependent; the novelty half's hops are a pointer chase and
        ;; are sequential under either strategy.
        sync-total (+ (:round-trips sync-cost) (:sequential-hops nov))
        async-total (+ (:waves async-cost) (:sequential-hops nov))]
    {:workload {:docs docs :datoms (* 3 docs) :unfolded-tx novelty-tx
                :index :aevt :index-root "pso"}
     :prolly-tree (assoc shape :fanout-mean
                         (when (> (:nodes shape) (:leaves shape))
                           (double (/ (- (:nodes shape) 1)
                                      (max 1 (- (:nodes shape) (:leaves shape)))))))
     :sync-trampoline (assoc sync-cost :projected-wall-ms (project (:round-trips sync-cost)))
     :async-level-batched (assoc async-cost :projected-wall-ms (project (:waves async-cost)))
     :novelty nov
     :hydrate-total {:sync-round-trips sync-total
                     :async-round-trips async-total
                     :novelty-share-of-async
                     (when (pos? async-total) (double (/ (:sequential-hops nov) async-total)))
                     :sync-projected-wall-ms (project sync-total)
                     :async-projected-wall-ms (project async-total)}
     :ratios {:snapshot-round-trip-reduction
              (when (pos? (:waves async-cost))
                (double (/ (:round-trips sync-cost) (:waves async-cost))))
              :snapshot-decode-reduction
              (when (pos? (:node-decodes async-cost))
                (double (/ (:node-decodes sync-cost) (:node-decodes async-cost))))
              :whole-hydrate-round-trip-reduction
              (when (pos? async-total) (double (/ sync-total async-total)))}}))

(def ^:private matrix
  "docs x unfolded-tx. 64 is `default-fold-threshold` -- the most novelty a
  well-behaved graph carries between folds, i.e. the worst case the fold
  scheduler is supposed to permit."
  [[1000 0] [1000 20] [1000 64]
   [5000 20] [5000 64]
   [20000 20] [20000 64]])

(defn -main [& args]
  (let [cells (if (seq args)
                [[(parse-long (first args)) (parse-long (or (second args) "0"))]]
                matrix)
        ;; print each cell as it completes -- this workstation runs many
        ;; concurrent agent sessions (load average >130 during this run), so a
        ;; harness that only prints at the end can lose an hour of work.
        runs (mapv (fn [[d n]]
                     (let [r (measure d n)]
                       (binding [*out* *err*] (prn [:cell r]) (flush))
                       r))
                   cells)]
    (prn
     {:schema 1
      :receipt/type :dag-shape
      :implementation "kotobase-peer.dag-shape-bench"
      :adr "ADR-2607310900 訂正4 precondition (measure DAG depth and width before retrying prefetch)"
      :shape-note "doc/coll + doc/key + doc/val -- the rows kotobase-protocols-worker.kotobase-store/hydrate! reads via one :aevt hot-datoms scan"
      :rtt-samples rtt-samples
      :runs runs
      :caveats
      ["Counts are exact (real pt/scan-prefix, real tree, real with-blocks retry algorithm); wall-clock is PROJECTED from separately-measured R2 RTTs and is not an end-to-end measurement."
       "In-memory block store on JVM -- this measures block-discovery STRUCTURE, not R2 or Worker CPU."
       "The novelty walk is a pointer chase: each successor CID is only known after decoding its predecessor, so its hops are sequential under BOTH strategies. hot-datoms' async arity routes the novelty TX-BLOCK reads through read-tx-block-async but still enumerates the chain with the SYNC get-fn (`(novelty-cids get-fn state)`), so these hops stay on the trampoline even after the fix -- which is what makes them the NEXT bottleneck, not a solved one."
       "The head/state walk (chain.core/head -> state block) is a small fixed number of additional sequential fetches, not counted here."
       ":node-decodes is the SNAPSHOT scan only. with-blocks wraps the whole hydrate, so a real restart also re-walks the state and novelty steps; the true decode total is higher than the number reported here. Round-trip counts are unaffected (one distinct block = one fetch either way)."]})))
