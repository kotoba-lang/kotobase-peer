(ns kotobase-peer.hydrate-async-test
  "Reading back a commit through `hydrate-chain-cached` over an ASYNC block store.

  cljs only, deliberately. Every other test here runs on the JVM, where block
  reads are synchronous and always succeed. The cljs path is the one that runs
  in a Cloudflare Worker over R2/D1/B2, and there a read goes through a
  block-miss trampoline: a synchronous `get-fn` that throws on a cache miss,
  caught and retried by the shell. That works only while the miss is thrown
  synchronously. `pmap-async` calls its mapping function inside a promise
  continuation, so the novelty half threw into async context, the retry never
  fired, and a commit was writable and then unreadable against every real async
  store. No JVM test can observe that, and there was no cljs test over an async
  store at all, which is how it survived.

  The store below is hostile in the one way that matters: bytes come back only
  through a Promise, and the synchronous reader starts empty, so anything read
  has to arrive either through the trampoline or through `async-get-fn`."
  (:require #?(:clj  [clojure.test :refer [deftest]]
               :cljs [cljs.test :refer [deftest is async]])
            [kotobase-peer.core :as peer]))

#?(:cljs
   (do
     (defn- new-store [] {:blocks (atom {}) :cache (atom {})})

     (defn- put-fn [store]
       (fn [cid bytes] (swap! (:blocks store) assoc (str cid) bytes) cid))

     (defn- fetch1-fn [store]
       (fn [cid] (js/Promise.resolve (get @(:blocks store) (str cid)))))

     (defn- sync-get-fn [store]
       (fn [cid]
         (let [k (str cid)]
           (if (contains? @(:cache store) k)
             (get @(:cache store) k)
             (throw (ex-info "block-miss" {:block-miss true :cid k}))))))

     (defn- with-blocks
       "Minimal block-miss trampoline, the same shape a Worker shell uses."
       [store f]
       (let [fetch1 (fetch1-fn store)
             sync-get (sync-get-fn store)]
         (letfn [(miss-cid [e]
                   ;; Read the CAUSE CHAIN, not the outermost `ex-data`. When a
                   ;; throw crosses an async continuation, nbb/SCI wraps it in
                   ;; its own error (`{:type :sci/error …}`) and puts the
                   ;; original under `:cause`, so a one-link read loses the
                   ;; `:cid` that selects the retry -- and the miss surfaces as
                   ;; a generic rejection instead. Root ADR-2608190100.
                   (loop [e e n 0]
                     (cond (nil? e) nil
                           (> n 8) nil
                           (:cid (ex-data e)) (:cid (ex-data e))
                           :else (recur (ex-cause e) (inc n)))))
                 (retry [e]
                   (if-let [cid (miss-cid e)]
                     (-> (fetch1 cid)
                         (.then (fn [bytes]
                                  (swap! (:cache store) assoc cid bytes)
                                  (step))))
                     (js/Promise.reject e)))
                 (step []
                   ;; `(.catch retry)` -- the sibling passed BY NAME -- is never
                   ;; invoked under nbb/SCI, silently. Measured 2026-08-19 in
                   ;; kotobase-server and reproduced in four lines; the wrapper
                   ;; is correct on every runtime. Root ADR-2608190100.
                   (try (-> (js/Promise.resolve (f sync-get)) (.catch (fn [e] (retry e))))
                        (catch :default e (retry e))))]
           (step))))

     (def ^:private blind #(js/Promise.resolve (pr-str %)))
     (def ^:private decrypt #(js/Promise.resolve %))
     (def ^:private encrypt #(js/Promise.resolve %))

     (deftest the-trampoline-retries-a-miss-thrown-inside-a-continuation
       ;; The namespace docstring says the defect it exists for is a miss that
       ;; arrives in ASYNC context. Measured 2026-08-19 under nbb, the test
       ;; below never takes that path: instrumenting both arms showed the sync
       ;; catch firing twice and the promise `.catch` firing zero times, so the
       ;; two-quad fixture proves the sync half only. This drives the async arm
       ;; directly -- the miss is thrown inside a `.then` continuation, which is
       ;; the only arm `(.catch …)` can serve.
       (async done
         (let [store (new-store)
               _ ((put-fn store) "cid-async" "bytes")
               fired (atom 0)]
           (-> (with-blocks store
                 (fn [get-fn]
                   (-> (js/Promise.resolve nil)
                       (.then (fn [_] (swap! fired inc) (get-fn "cid-async"))))))
               (.then (fn [v]
                        (is (= "bytes" v) "the async miss was fetched and the read retried")
                        (is (= 2 @fired) "f ran twice: once to miss, once after the fetch")
                        (done)))
               (.catch (fn [e]
                         (is false (str "async miss was not retried: "
                                        (or (some-> e .-message) e)))
                         (done)))))))

     (deftest hydrate-chain-cached-reads-back-over-an-async-store
       (async done
         (let [store (new-store)
               fetch1 (fetch1-fn store)]
           (-> (with-blocks store
                 (fn [get-fn]
                   (peer/commit! (put-fn store) get-fn
                                 [["alice" "role" "admin"]
                                  ["alice" "team" "platform"]]
                                 nil encrypt)))
               (.then
                (fn [head]
                  (is (string? head) "commit! returns the new chain CID")
                  ;; Cold: nothing pre-warmed. Before the fix this rejected with
                  ;; block-miss for a novelty block the store demonstrably holds.
                  (reset! (:cache store) {})
                  (with-blocks store
                    (fn [get-fn]
                      (peer/hydrate-chain-cached
                       get-fn head blind decrypt nil nil fetch1)))))
               (.then
                (fn [db]
                  (is (= 2 (count (peer/q db ["alice" nil nil] (constantly true))))
                      "both committed quads read back")
                  (done)))
               (.catch
                (fn [error]
                  (is false (str "read rejected: "
                                 (or (some-> error .-message) error)))
                  (done)))))))

     ;; ── the silent-partial read, reproduced without a network ────────────────
     ;;
     ;; root ADR-2608170300 measured a 25-commit workload on testnet and got
     ;; ROWS 17 where the production control got 25 -- with the pack gate OFF,
     ;; so the loss is not about packing. Its verdict: no pack read number can
     ;; be trusted until this is fixed.
     ;;
     ;; That measurement needed a deployment and an authenticated write. This
     ;; does not: 25 commits chained through `prev-chain-cid` and read back
     ;; through the same trampoline the Worker shell uses is the same shape, in
     ;; memory. If the loss is in the read path it belongs here.
     ;;
     ;; Asserted as a COUNT and then as the missing subjects, because "17 rows"
     ;; and "the wrong 17 rows" are different defects and a set-equality alone
     ;; would not say which. A read that answers 17 of 25 without erroring is
     ;; data loss wearing a success, which is the class this repository treats
     ;; as the most dangerous.
     (deftest twenty-five-chained-commits-all-read-back
       (async done
         (let [store (new-store)
               fetch1 (fetch1-fn store)
               n 25
               subjects (mapv #(str "s" %) (range n))]
           (-> (reduce
                (fn [p i]
                  (.then p (fn [prev]
                             (with-blocks store
                               (fn [get-fn]
                                 (peer/commit! (put-fn store) get-fn
                                               [[(nth subjects i) "kind" "row"]]
                                               prev encrypt))))))
                (js/Promise.resolve nil)
                (range n))
               (.then
                (fn [head]
                  (is (string? head) "the 25th commit returns a chain CID")
                  ;; Cold read: nothing pre-warmed, exactly as a fresh isolate.
                  (reset! (:cache store) {})
                  (with-blocks store
                    (fn [get-fn]
                      (peer/hydrate-chain-cached
                       get-fn head blind decrypt nil nil fetch1)))))
               (.then
                (fn [db]
                  (let [rows (peer/q db [nil "kind" "row"] (constantly true))
                        ;; `:s`. `peer/q`'s docstring says it returns a set of
                        ;; `{:s :p :o}` quads -- not `{:e :a :v}`, and not
                        ;; vectors. Two earlier versions of this line guessed
                        ;; `first` and then `:e`, and BOTH reported all 25
                        ;; subjects missing while the COUNT assertion passed.
                        ;; A harness that extracts the wrong key produces
                        ;; exactly the failure this test exists to catch, so the
                        ;; shape was read out of the source rather than guessed
                        ;; a third time.
                        seen (set (map :s rows))
                        missing (remove seen subjects)]
                    (is (= n (count rows))
                        (str "all " n " committed rows read back, got " (count rows)))
                    (is (empty? missing)
                        (str "no subject silently dropped; missing " (pr-str (vec missing))))
                    (done))))
               (.catch
                (fn [error]
                  ;; A rejection is a DIFFERENT defect from a short answer, and
                  ;; saying so is the point: this test exists to tell them apart.
                  (is false (str "read rejected rather than answering short: "
                                 (or (some-> error .-message) error)))
                  (done)))))))))

#?(:clj
   ;; Placeholder so the JVM runner sees a well-formed namespace. The behaviour
   ;; under test does not exist on this platform.
   (deftest cljs-only-namespace))
