(ns kotobase-peer.load-test
  "Load test / benchmark harness for kotobase-peer's write (`commit!`), read
  (`hot-datoms`), and compaction (`fold!`, both unbounded and the new
  `max-novelty`-bounded form) paths against a growing novelty backlog.

  This is NOT a correctness test (see `test/kotobase_peer/core_test.cljc`
  for that) -- it exists to answer, with real numbers instead of static
  reasoning, the question that came up investigating gftdcojp/app-aozora#78
  (`getRecord`/`listRecords` timing out / getting platform-canceled against
  a real production novelty backlog): how does `hot-datoms`/`fold!`'s own
  latency actually scale as novelty grows, and does the `max-novelty` budget
  (kotobase-peer commit db957cf) really keep per-call fold latency roughly
  CONSTANT regardless of total backlog size (the property that makes it
  useful for a fold cron that must make forward progress within one bounded
  Worker invocation), or does it only help up to some point.

  In-memory `put!`/`get-fn` (no network/R2/D1) so this measures the ENGINE's
  own algorithmic cost in isolation from storage latency -- the real number
  a deployed Worker sees is this plus real R2/KV round-trip time per
  `read-tx-block`/`put!` call (worse, not better, than what's measured
  here). Real AES-256-GCM + HMAC-SHA256 (not a no-op mock) via `javax.
  crypto`, matching `core_test.cljc`'s own JVM crypto helpers exactly (kept
  duplicated here rather than shared, matching this codebase's existing
  test-helper duplication convention across repos) -- so encrypt/decrypt
  cost is included in every number below, not hidden.

  Run: clojure -M:bench (see bb.edn's `loadtest` task / deps.edn's `:bench`
  alias) or `bb loadtest [novelty-sizes...]` (defaults to a preset sweep)."
  (:require [ipld.core :as ipld]
            [kotobase-peer.core :as eng])
  (:import [javax.crypto Cipher Mac]
           [javax.crypto.spec SecretKeySpec GCMParameterSpec]
           [java.util Base64]))

;; ── real JVM crypto (identical construction to core_test.cljc's) ───────────

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

;; ── harness ─────────────────────────────────────────────────────────────────

(defn- mem-store []
  (let [store (atom {})]
    {:put! (fn [cid bytes] (swap! store assoc cid bytes))
     :get-fn (fn [cid] (get @store cid))}))

(defn- commit-n!
  "Commits n small single-quad tx blocks in sequence starting from nil,
  returns the final chain-cid. This is itself timed separately (write-path
  cost) from the read/fold measurements below, which all operate on the
  chain-cid this returns (i.e. AFTER the writes, matching how a real fold
  cron encounters an already-accumulated backlog, not while it's growing)."
  [put! get-fn n]
  (reduce (fn [chain-cid i]
            (eng/commit! put! get-fn [{:s (str "actor-" i) :p "role" :o "member"}]
                         chain-cid test-encrypt-fn))
          nil
          (range n)))

(defn- elapsed-ms [f]
  (let [start (System/nanoTime)
        result (f)]
    {:ms (/ (- (System/nanoTime) start) 1e6) :result result}))

(defn measure-at-novelty-size
  "Builds a fresh chain with `n` unfolded writes, then times:
    :commit-total-ms   -- total wall time to make all n commit! calls
    :hot-datoms-ms     -- one hot-datoms read against the full n-entry backlog
    :fold-unbounded-ms -- one fold! call that compacts ALL n entries at once
    :fold-bounded-ms   -- one fold! call with max-novelty=bound (only
                          meaningful when n > bound; on a FRESH chain of the
                          same size, so this is an apples-to-apples single-
                          call comparison against fold-unbounded-ms, not a
                          multi-call amortized figure)
  Every measurement starts from its own fresh in-memory store / fresh n-entry
  backlog so earlier measurements (which mutate/fold the chain) never skew
  a later one."
  [n bound]
  (let [everything (constantly true)
        {:keys [put! get-fn]} (mem-store)
        {:keys [ms result]} (elapsed-ms #(commit-n! put! get-fn n))
        chain-cid result
        commit-total-ms ms

        hot (let [{:keys [ms]} (elapsed-ms #(eng/hot-datoms get-fn chain-cid everything test-blind-fn test-decrypt-fn))]
              ms)

        fold-unbounded
        (let [{:keys [put! get-fn]} (mem-store)
              chain-cid (commit-n! put! get-fn n)
              {:keys [ms]} (elapsed-ms #(eng/fold! put! get-fn chain-cid test-blind-fn test-encrypt-fn test-decrypt-fn))]
          ms)

        fold-bounded
        (when (> n bound)
          (let [{:keys [put! get-fn]} (mem-store)
                chain-cid (commit-n! put! get-fn n)
                {:keys [ms]} (elapsed-ms #(eng/fold! put! get-fn chain-cid ipld/link? bound
                                                     test-blind-fn test-encrypt-fn test-decrypt-fn))]
            ms))]
    {:novelty-size n
     :commit-total-ms commit-total-ms
     :commit-avg-ms (/ commit-total-ms n)
     :hot-datoms-ms hot
     :fold-unbounded-ms fold-unbounded
     :fold-bounded-ms fold-bounded
     :fold-bound bound}))

(defn- fmt-ms [ms] (if ms (format "%8.2f" (double ms)) "       -"))

(defn run-sweep!
  "Runs measure-at-novelty-size across `sizes` (default a preset sweep) with
  a fixed fold budget `bound` (default 24, matching pmap-async-batch-size --
  not load-tested against Workers' exact per-plan ceiling, see that var's
  own docstring; this harness exists partly to inform re-tuning it with
  real numbers instead of guessing). Prints a table; returns the raw results
  vector for programmatic use."
  ([] (run-sweep! [10 50 100 500 1000] 24))
  ([sizes bound]
   (println (format "\n%-14s %14s %14s %16s %16s %16s"
                     "novelty-size" "commit-total" "commit-avg" "hot-datoms" "fold-unbounded" "fold-bounded"))
   (println (format "%-14s %14s %14s %16s %16s %16s"
                     "" "(ms)" "(ms/write)" "(ms)" "(ms)" (str "(ms, bound=" bound ")")))
   (println (apply str (repeat 96 "-")))
   (let [results (mapv (fn [n]
                          (let [r (measure-at-novelty-size n bound)]
                            (println (format "%-14d %14s %14s %16s %16s %16s"
                                              n (fmt-ms (:commit-total-ms r)) (fmt-ms (:commit-avg-ms r))
                                              (fmt-ms (:hot-datoms-ms r)) (fmt-ms (:fold-unbounded-ms r))
                                              (fmt-ms (:fold-bounded-ms r))))
                            (flush) ; each row is expensive (real crypto x n); show progress as it happens
                            r))
                        sizes)]
     (println (apply str (repeat 96 "-")))
     (let [bounded (keep :fold-bounded-ms results)]
       (when (seq bounded)
         (println (format "\nfold-bounded-ms range across the sweep: %.2f - %.2f ms (bound=%d)."
                           (apply min bounded) (apply max bounded) bound))
         (println "If this range stays roughly flat as novelty-size grows while fold-unbounded-ms")
         (println "keeps climbing, max-novelty is doing its job: a fold cron using it pays a roughly")
         (println "constant per-call cost and makes guaranteed forward progress every cycle, instead")
         (println "of an all-or-nothing fold whose cost (and cancellation risk) grows with the backlog.")))
     results)))

;; ── index-query benchmark (cold-datoms / :avet, separate from novelty) ─────
;; The commit!/hot-datoms/fold! sweep above is about the NOVELTY-side cost
;; (unfolded writes). This section instead asks: once a graph IS folded
;; (indexed, zero novelty), does a keyed `:avet` point lookup actually stay
;; cheap as the graph grows, matching cold-datoms's own docstring claim
;; ("Touches only the chosen index tree's blocks along the components
;; prefix path ... so a keyed read stays small regardless of graph size")?
;; commit!'s docstring made an equivalent claim (O(tx)) that this same
;; harness DISPROVED empirically (see run-sweep! above / kotoba-lang/
;; kotobase-peer#16) -- so this claim is worth checking with real numbers
;; too, not assumed just because it's documented.

(defn- commit-and-fold-n!
  "Builds a FOLDED (indexed, zero-novelty) graph of n entities, each with a
  distinct :email value (actor-0@example.com .. actor-{n-1}@example.com) so
  a keyed :avet query for exactly one of them is a genuine needle-in-
  haystack lookup, not a query that happens to match everything. Returns
  the folded chain-cid."
  [put! get-fn n]
  (let [chain-cid (reduce (fn [chain-cid i]
                             (eng/commit! put! get-fn
                                          [{:s (str "actor-" i) :p "email"
                                            :o (str "actor-" i "@example.com")}]
                                          chain-cid test-encrypt-fn))
                           nil
                           (range n))]
    (eng/fold! put! get-fn chain-cid test-blind-fn test-encrypt-fn test-decrypt-fn)))

(defn measure-index-query-at-graph-size
  "Builds a fresh n-entity FOLDED graph, then times:
    :full-scan-ms -- cold-datoms with no :components filter (an unkeyed
                     :eavt walk of the whole snapshot -- expected to scale
                     with n, this is the baseline a keyed query should beat)
    :keyed-avet-ms -- cold-datoms {:index :avet :components [\"email\"
                      \"actor-0@example.com\"]}, a query matching exactly
                      ONE of the n entities. cold-datoms's own docstring
                      claims this 'stays small regardless of graph size' --
                      that's the claim this measures, not assumes."
  [n]
  (let [everything (constantly true)
        {:keys [put! get-fn]} (mem-store)
        folded (commit-and-fold-n! put! get-fn n)
        snap-cid (eng/latest-snapshot-cid get-fn folded)
        {full-ms :ms} (elapsed-ms #(eng/cold-datoms get-fn snap-cid nil everything test-blind-fn test-decrypt-fn))
        {keyed-ms :ms result :result}
        (elapsed-ms #(eng/cold-datoms get-fn snap-cid {:index :avet :components ["email" "actor-0@example.com"]}
                                      everything test-blind-fn test-decrypt-fn))]
    (when (not= 1 (count result))
      (throw (ex-info "keyed :avet query didn't return exactly 1 row -- benchmark's own correctness assumption broke, numbers below would be meaningless"
                       {:n n :matched (count result)})))
    {:graph-size n :full-scan-ms full-ms :keyed-avet-ms keyed-ms}))

(defn run-index-sweep!
  "Runs measure-index-query-at-graph-size across `sizes` (default a preset
  sweep). Prints a table; returns the raw results vector."
  ([] (run-index-sweep! [10 50 100 500 1000]))
  ([sizes]
   (println (format "\n%-14s %16s %16s" "graph-size" "full-scan" "keyed-avet"))
   (println (format "%-14s %16s %16s" "" "(ms)" "(ms, 1-of-n row)"))
   (println (apply str (repeat 48 "-")))
   (let [results (mapv (fn [n]
                          (let [r (measure-index-query-at-graph-size n)]
                            (println (format "%-14d %16s %16s"
                                              n (fmt-ms (:full-scan-ms r)) (fmt-ms (:keyed-avet-ms r))))
                            (flush)
                            r))
                        sizes)]
     (println (apply str (repeat 48 "-")))
     (let [keyed (mapv :keyed-avet-ms results)]
       (println (format "\nkeyed-avet-ms range across the sweep: %.2f - %.2f ms."
                         (apply min keyed) (apply max keyed)))
       (println "If this stays roughly flat while full-scan-ms keeps climbing with graph-size,")
       (println "cold-datoms's 'range-pruned, independent of graph size' claim holds for a keyed")
       (println "point lookup on an already-folded (indexed) graph -- i.e. the read path is fine")
       (println "once folded; commit!'s O(n^2) write-path bug (kotobase-peer#16) is unrelated to")
       (println "this index structure and doesn't implicate cold-datoms's query performance."))
     results)))

(defn -main [& args]
  (let [sizes (if (seq args) (mapv parse-long args) [10 50 100 500 1000])]
    (run-sweep! sizes 24)
    (run-index-sweep! sizes)
    (shutdown-agents)))
