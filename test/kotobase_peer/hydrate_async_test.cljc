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
         (letfn [(retry [e]
                   (if-let [cid (:cid (ex-data e))]
                     (-> (fetch1 cid)
                         (.then (fn [bytes]
                                  (swap! (:cache store) assoc cid bytes)
                                  (step))))
                     (js/Promise.reject e)))
                 (step []
                   (try (-> (js/Promise.resolve (f sync-get)) (.catch retry))
                        (catch :default e (retry e))))]
           (step))))

     (def ^:private blind #(js/Promise.resolve (pr-str %)))
     (def ^:private decrypt #(js/Promise.resolve %))
     (def ^:private encrypt #(js/Promise.resolve %))

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
                  (done)))))))))

#?(:clj
   ;; Placeholder so the JVM runner sees a well-formed namespace. The behaviour
   ;; under test does not exist on this platform.
   (deftest cljs-only-namespace))
