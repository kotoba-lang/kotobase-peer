(ns kotobase-peer.availability-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [kotobase-peer.availability :as avail]))

(defn- mem-store [initial]
  (let [store (atom initial)]
    {:put! (fn [cid bytes] (swap! store assoc cid bytes))
     :get-fn (fn [cid] (get @store cid))}))

(defn- bytes-of [ints]
  #?(:clj (byte-array (map byte ints))
     :cljs (js/Uint8Array. (clj->js ints))))

(def block-a (bytes-of [1 2 3 4 5]))
(def block-b (bytes-of [9 9 9]))
(def nonce-1 (bytes-of [42 42]))
(def nonce-2 (bytes-of [7 7]))

(deftest prove-and-verify-roundtrip-ok
  (testing "node holds the block, verifier holds the SAME block -> :ok"
    (let [node (mem-store {"cid-a" block-a})
          verifier (mem-store {"cid-a" block-a})
          chal (avail/challenge "cid-a" nonce-1 1)
          resp (avail/prove (:get-fn node) chal)]
      (is (some? resp))
      (is (= :ok (avail/verify (:get-fn verifier) chal resp))))))

(deftest prove-returns-nil-when-node-lacks-block
  (testing "fails closed: no fabricated proof for a block the node doesn't have"
    (let [node (mem-store {})
          chal (avail/challenge "cid-missing" nonce-1 1)]
      (is (nil? (avail/prove (:get-fn node) chal))))))

(deftest verify-missed-when-node-response-nil
  (testing "no response at all (node offline / didn't answer) -> :missed"
    (let [verifier (mem-store {"cid-a" block-a})
          chal (avail/challenge "cid-a" nonce-1 1)]
      (is (= :missed (avail/verify (:get-fn verifier) chal nil))))))

(deftest verify-fails-when-bytes-differ
  (testing "node's bytes under this cid differ from the verifier's own replica -> :failed"
    (let [node (mem-store {"cid-a" block-b})
          verifier (mem-store {"cid-a" block-a})
          chal (avail/challenge "cid-a" nonce-1 1)
          resp (avail/prove (:get-fn node) chal)]
      (is (= :failed (avail/verify (:get-fn verifier) chal resp))))))

(deftest verify-rejects-replayed-proof-across-epochs
  (testing "a proof computed under one nonce does not verify against a challenge with a different nonce (freshness)"
    (let [node (mem-store {"cid-a" block-a})
          verifier (mem-store {"cid-a" block-a})
          chal1 (avail/challenge "cid-a" nonce-1 1)
          resp1 (avail/prove (:get-fn node) chal1)
          chal2 (avail/challenge "cid-a" nonce-2 2)]
      (is (= :failed (avail/verify (:get-fn verifier) chal2 resp1))))))

(deftest verify-lacks-replica-when-verifier-has-no-copy
  (testing "a verifier with no local replica cannot audit at all -> :verifier-lacks-replica, not a false :ok/:failed"
    (let [verifier (mem-store {})
          chal (avail/challenge "cid-a" nonce-1 1)
          fake-response {:kotobase.availability/cid "cid-a" :kotobase.availability/proof "deadbeef"}]
      (is (= :verifier-lacks-replica (avail/verify (:get-fn verifier) chal fake-response))))))

(deftest verify-malformed-when-cid-mismatches
  (testing "a response claiming a different cid than what was challenged -> :malformed"
    (let [verifier (mem-store {"cid-a" block-a})
          chal (avail/challenge "cid-a" nonce-1 1)
          bad-response {:kotobase.availability/cid "cid-other" :kotobase.availability/proof "whatever"}]
      (is (= :malformed (avail/verify (:get-fn verifier) chal bad-response))))))

(deftest audit-outcome-is-a-plain-record
  (is (= {:audit/node "n1" :audit/cid "cid-a" :audit/epoch 3 :audit/verdict :ok}
         (avail/audit-outcome "n1" "cid-a" 3 :ok))))
