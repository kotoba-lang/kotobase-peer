(ns kotobase-peer.cache-test
  "The host side of the block cache, against the REAL read path.

  A cache measured against a fake reader proves nothing about the reader, so
  every number here comes from `eng/cold-datoms` over a persisted snapshot."
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase-peer.cache :as kc]
            [kotobase-peer.core :as eng]
            [block.cache :as bc]
            [merkle-lsm.core :as lsm]
            [kotobase-peer.core-test :refer [test-blind-fn test-encrypt-fn test-decrypt-fn]]))

(defn- store []
  (let [m (atom {}) reads (atom 0)]
    {:put! (fn [cid b] (swap! m assoc cid b))
     :get-fn (fn [cid] (swap! reads inc) (get @m cid))
     :reads reads}))

(def ^:private everything (constantly true))

(defn- snapshot! [put! get-fn n]
  (let [db (eng/transact (eng/empty-db)
                         (for [i (range n)]
                           [(str "e" i) (if (zero? (mod i 100)) ":rare" ":common") (str "v" i)]))
        chain (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)]
    (eng/latest-snapshot-cid get-fn chain)))

#?(:clj
   (deftest a-cached-get-fn-answers-identically-and-reads-fewer-blocks
     (let [{:keys [put! get-fn reads]} (store)
           snap (snapshot! put! get-fn 500)
           q #(eng/cold-datoms % snap {:index :eavt :components ["e7"]}
                               everything test-blind-fn test-decrypt-fn)]
       (reset! reads 0)
       (let [plain (doall (repeatedly 5 #(q get-fn)))
             plain-reads @reads
             cache (kc/default-cache)
             cached (kc/cached-get-fn cache get-fn)]
         (reset! reads 0)
         (let [via-cache (doall (repeatedly 5 #(q cached)))
               cached-reads @reads]
           (testing "same rows -- a cache that changes an answer is a bug"
             (is (= (set plain) (set via-cache)))
             (is (seq (first via-cache))))
           (println (format "  [cold-datoms x5] uncached %d blocks, cached %d (%.1fx)"
                            plain-reads cached-reads
                            (double (/ plain-reads (max 1 cached-reads)))))
           (testing "repeated reads stop reaching the store"
             (is (< cached-reads plain-reads))
             (is (pos? (:hits (:small (kc/stats cache)))) "the metadata segment is being used")))))))

#?(:clj
   (deftest verification-runs-once-per-block-not-once-per-read
     ;; core/verified-node re-hashes on EVERY read. The cache makes that once.
     (let [{:keys [put! get-fn reads]} (store)
           snap (snapshot! put! get-fn 200)
           verified (atom 0)
           cache (kc/default-cache)
           counting-verify (bc/wrap-get-fn cache get-fn
                                           {:verify! (fn [cid bytes]
                                                       (swap! verified inc)
                                                       (kc/verify-cid! cid bytes))})]
       (dotimes [_ 6]
         (eng/cold-datoms counting-verify snap {:index :eavt :components ["e3"]}
                          everything test-blind-fn test-decrypt-fn))
       (let [{:keys [small large]} (kc/stats cache)
             stores (+ (:stores small) (:stores large))]
         (testing "six reads, but each distinct block hashed exactly once --
                  summed across BOTH segments, since a snapshot read touches a
                  small node and larger data blocks"
           (is (= @verified stores) "every verify corresponded to a first-time store")
           (is (< @verified 6) "and far fewer than one per read"))))))

#?(:clj
   (deftest a-corrupt-block-still-throws-through-the-cache
     (testing "verify-on-miss is not verify-never"
       (let [m (atom {}) 
             put! (fn [cid b] (swap! m assoc cid b))
             get-fn (fn [cid] (get @m cid))
             snap (snapshot! put! get-fn 20)
             lying (fn [_cid] (byte-array [1 2 3]))
             cached (kc/cached-get-fn (kc/default-cache) lying)]
         (is (thrown? clojure.lang.ExceptionInfo (cached snap)))))))

#?(:clj
   (deftest the-declared-merkle-lsm-cache-effects-now-have-an-interpreter
     ;; :cache/get and :cache/put appeared exactly twice in this repo before
     ;; kotobase-peer.cache: at their own definitions.
     (let [cache (kc/default-cache)
           bytes (byte-array 32)]
       (is (nil? (kc/handle-effect cache (lsm/cache-get "cid-1"))))
       (is (nil? (kc/handle-effect cache (lsm/cache-put "cid-1" bytes))))
       (is (some? (kc/handle-effect cache (lsm/cache-get "cid-1"))))
       (testing "an effect that is not ours passes through instead of being swallowed"
         (is (= ::kc/pass (kc/handle-effect cache (lsm/block-get "cid-1"))))))))
