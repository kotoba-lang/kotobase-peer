(ns kotobase-peer.novelty-chain-depth-test
  "How deep the novelty cons chain is on ClojureScript -- the runtime that ships.

  ## This is the mirror `novelty_segments_test.cljc` says it OWES

  That file measures the same shape and guards every behavioural test behind
  `#?(:clj ...)`, with a note stating why: its `hot-datoms`/`fold!` calls are
  synchronous, which is correct on the JVM and wrong on cljs where the crypto
  fns return Promises. It ends `OWED: the Promise-based cljs mirror ... Guarding
  here states the JVM-only coverage honestly instead of asserting portability no
  machine checks; it does not close the hole.`

  So on cljs -- the Cloudflare Worker runtime this code actually runs in -- the
  only thing pinned was the CONSTANT (`segment-size-is-the-one-the-adr-measured-
  against`, platform-neutral). Measured 2026-09-09 by setting the constant to 1:
  the constant test goes red and no cljs test observes that the chain became 64
  nodes deep. This closes that, by walking the chain rather than calling the
  synchronous readers -- the walk needs no crypto, which is why it can be a
  cljs test at all.

  ## Why the depth is worth a test on both platforms

  Two accepted ADRs disagree about it by a factor of 16:

    root ADR-2608021000  novelty is a cons chain whose DEPTH EQUALS THE NUMBER
                         OF UNFOLDED TRANSACTIONS, width 1, prefetch impossible
    root ADR-2608160100  measured `hops = ceil(unfolded-tx / 16)` -- 64 unfolded
                         transactions produce FOUR hops

  The second is right, and the first describes a shape this repository has
  already left: `novelty-segment-size` is 16 and its own docstring says \"1 was
  the original shape: one block per unfolded transaction\". The batching landed
  as the fix FOR the measurement ADR-2608021000 recorded, so quoting that ADR's
  depth today quotes the defect rather than the code. kotoba-lang/ayatori's
  co-scientist iteration 05 did exactly that and built a packing argument on a
  chain 16x deeper than the one that exists.

  The depth is what every packing-policy argument multiplies by: at the fold
  threshold of 64 it is 4, so a grouping change on this chain is worth
  single-digit round trips. If it silently returns to one block per transaction
  it becomes 64, every such argument changes by 16x, and NOTHING ELSE FAILS.

  ## Two facts neither existing test has

  * The 16/17 BOUNDARY. `depth < n` is true for any batching factor; a segment
    size of 8 would satisfy it and change every downstream number.
  * WHICH COMMIT wrote each surviving segment. A commit-scoped pack can only
    co-locate blocks written by the same commit, so this is the fact that
    decides whether `1 commit = 1 pack` can put a chain in one pack."
  (:require #?(:clj  [clojure.test :refer [deftest]]
               :cljs [cljs.test :refer [deftest is async]])
            #?(:cljs [ipld.core :as ipld])
            [kotobase-peer.core :as peer]))

#?(:cljs
   (do
     (defn- new-store []
       ;; Every block stays reachable synchronously: this measures the SHAPE of
       ;; what was written, so a block-miss trampoline would only add noise.
       ;; `owner` records which commit first wrote each cid -- the question
       ;; "do the surviving segments cross commits" cannot be answered without it.
       {:blocks (atom {}) :owner (atom {}) :at (atom 0)})

     (defn- put-fn [store]
       (fn [cid bytes]
         (let [k (str cid)]
           (when-not (contains? @(:blocks store) k)
             (swap! (:owner store) assoc k @(:at store)))
           (swap! (:blocks store) assoc k bytes))
         cid))

     (defn- get-fn [store] (fn [cid] (get @(:blocks store) (str cid))))
     (def ^:private encrypt #(js/Promise.resolve %))

     (defn- node-at [store cid]
       (let [by (get @(:blocks store) (str cid))]
         (ipld/decode (if (string? by) (js/Uint8Array.from (js/Array.from by)) by))))

     (defn- walk-back
       "Walks `novelty-back` the way a hydrate does -- one node at a time, the
        next CID unavailable until this node decodes. Returns the depth, the
        total entries, each node's entry count, and the commit that wrote it."
       [store state]
       (loop [link (get state "novelty-back") depth 0 entries 0 sizes [] owners []]
         (if (or (nil? link) (> depth 1000))
           {:depth depth :entries entries :sizes sizes :owners owners}
           (let [cid (ipld/link-cid link)
                 node (node-at store cid)
                 es (get node "es")
                 k (if es (count es) 1)]
             (recur (get node "rest") (inc depth) (+ entries k) (conj sizes k)
                    (conj owners (get @(:owner store) (str cid))))))))

     (defn- commit-n
       "`n` single-quad commits threaded through prev-chain-cid, one per
        transaction, which is what an unfolded window is made of."
       [store n]
       (reduce (fn [p i]
                 (.then p (fn [prev]
                            (reset! (:at store) i)
                            (peer/commit! (put-fn store) (get-fn store)
                                          [[(str "s" i) "kind" "row"]] prev encrypt))))
               (js/Promise.resolve nil)
               (range n)))

     (deftest novelty-chain-depth-is-one-node-per-sixteen-transactions
       (async done
         (let [store (new-store)
               n peer/default-fold-threshold]   ; 64 -- the depth every argument uses
           (-> (commit-n store n)
               (.then
                (fn [head]
                  (let [state (:state (peer/head (get-fn store) head))
                        {:keys [depth entries sizes owners]} (walk-back store state)]
                    (is (= n (get state "novelty-count"))
                        "all 64 transactions are unfolded, so this is the fold-threshold window")
                    (is (= n entries)
                        (str "the chain still carries every entry; got " entries))
                    (is (= 4 depth)
                        (str "ceil(64/16) = 4 nodes, not 64. got " depth
                             " -- if this is 64 the segmented shape has reverted and every"
                             " packing-policy number changes by 16x"))
                    (is (= [16 16 16 16] sizes)
                        (str "each node carries a full 16 entries; got " (pr-str sizes)))
                    ;; The pack-plane question. A commit-scoped pack can only
                    ;; co-locate blocks written by the SAME commit, so if each
                    ;; surviving segment came from a different commit then one
                    ;; pack per commit puts exactly one chain link in each pack
                    ;; (N/P = 1.00) and the walk costs 4 per link where the
                    ;; per-object plane costs 2.
                    (is (= depth (count (distinct owners)))
                        (str "every surviving segment was written by a DIFFERENT commit; "
                             (pr-str owners)))
                    (done))))
               (.catch (fn [e]
                         (is false (str "measurement failed: " (or (some-> e .-message) e)))
                         (done)))))))

     (deftest novelty-chain-depth-crosses-the-segment-boundary-exactly-once
       ;; The boundary. 16 is one node and 17 is two -- without a case on the
       ;; line, a segment size of 8 or 32 would satisfy every assertion above
       ;; that only checks "fewer nodes than transactions".
       (async done
         (let [a (new-store) b (new-store)]
           (-> (commit-n a 16)
               (.then (fn [head]
                        (let [w (walk-back a (:state (peer/head (get-fn a) head)))]
                          (is (= 1 (:depth w)) (str "16 entries fit one node; got " (:depth w)))
                          (is (= [16] (:sizes w)) (pr-str (:sizes w))))
                        (commit-n b 17)))
               (.then (fn [head]
                        (let [w (walk-back b (:state (peer/head (get-fn b) head)))]
                          (is (= 2 (:depth w)) (str "the 17th entry opens a second node; got " (:depth w)))
                          (is (= [1 16] (:sizes w))
                              (str "newest-first: the open node holds the 1 new entry, the closed"
                                   " one holds 16. got " (pr-str (:sizes w))))
                          (is (= 17 (:entries w)) "no entry lost at the boundary"))
                        (done)))
               (.catch (fn [e]
                         (is false (str "measurement failed: " (or (some-> e .-message) e)))
                         (done))))))))

   :clj
   ;; The walk decodes blocks through ipld on the cljs side only; the shape
   ;; being measured is platform-independent but the harness is not.
   (deftest cljs-only-namespace))
