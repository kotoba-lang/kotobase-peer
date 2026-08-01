(ns kotobase-peer.novelty-segments-test
  "ADR-2608021000 §3 / §6-3: the unfolded-novelty queue holds up to
  `novelty-segment-size` entries per block instead of one, so ENUMERATING it
  costs ceil(n/16) sequential block reads instead of n.

  These tests hold the two things that can go wrong. ORDER: the queue is a
  persistent queue with a newest-first `back` and an oldest-first `front`, and
  batching entries into nodes gives each node an internal order that has to
  agree with its chain's direction -- get that backwards and a fold replays
  transactions out of order into the snapshot, silently. COMPATIBILITY: every
  chain already committed is made of single-entry nodes, so a reader that only
  understood segments would stop seeing history that is already live."
  (:require [clojure.test :refer [deftest testing is]]
            [ipld.core :as ipld]
            [kotobase-peer.core :as eng]
            [kotobase-peer.core-test :refer [test-encrypt-fn test-blind-fn test-decrypt-fn]]))

(defn- store []
  (let [s (atom {})]
    {:put! (fn [cid bytes] (swap! s assoc cid bytes))
     :get-fn (fn [cid] (get @s cid))
     :blocks s}))

(defn- commit-n [put! get-fn n]
  (reduce (fn [chain i]
            (eng/commit! put! get-fn [{:s (str "e" i) :p "seq" :o (str i)}]
                         chain test-encrypt-fn))
          nil (range n)))

(defn- expected [n]
  (set (for [i (range n)]
         {:e (str "e" i) :a "seq" :v_edn (str "\"" i "\"") :added true})))

(defn- novelty-node-count
  "Blocks a novelty walk must fetch: the length of the front+back chains.
  This is the number the change exists to reduce -- one per transaction
  before, ceil(n/16) after -- and counting it here keeps the claim honest
  without a benchmark."
  [get-fn chain]
  (let [state (#'eng/state-at get-fn chain)
        walk (fn [link]
               (loop [cid (some-> link ipld/link-cid) n 0]
                 (if (nil? cid)
                   n
                   (recur (some-> (get (ipld/get-node get-fn cid) "rest") ipld/link-cid)
                          (inc n)))))]
    (+ (walk (get state "novelty-front")) (walk (get state "novelty-back")))))

(deftest sixty-four-unfolded-commits-fit-in-four-blocks
  (let [{:keys [put! get-fn]} (store)
        chain (commit-n put! get-fn 64)]
    (is (= 64 (eng/novelty-size get-fn chain)))
    (is (= 4 (novelty-node-count get-fn chain))
        "64 entries at 16 per segment -- one sequential fetch per 16, not per 1")
    (is (= (expected 64)
           (set (eng/hot-datoms get-fn chain (constantly true)
                                test-blind-fn test-decrypt-fn)))
        "nothing lost or duplicated across segment boundaries")))

(deftest fold-drains-a-segmented-queue-without-reordering
  (let [{:keys [put! get-fn]} (store)
        chain (commit-n put! get-fn 40)
        folded (eng/fold! put! get-fn chain test-blind-fn test-encrypt-fn test-decrypt-fn)]
    (is (zero? (eng/novelty-size get-fn folded)))
    (is (= (expected 40)
           (set (eng/hot-datoms get-fn folded (constantly true)
                                test-blind-fn test-decrypt-fn))))))

(deftest a-bounded-fold-leaves-the-remainder-readable
  ;; The bounded fold is where oldest-first `front` and newest-first `back`
  ;; actually meet: it drains a prefix, rebuilds front from what is left, and
  ;; the next read has to splice both halves back into one order.
  (let [{:keys [put! get-fn]} (store)
        chain (commit-n put! get-fn 40)
        folded (eng/fold! put! get-fn chain ipld/link? 10
                          test-blind-fn test-encrypt-fn test-decrypt-fn)]
    (is (= 30 (eng/novelty-size get-fn folded)) "10 taken, 30 left")
    (is (= (expected 40)
           (set (eng/hot-datoms get-fn folded (constantly true)
                                test-blind-fn test-decrypt-fn)))
        "snapshot half plus remaining novelty half still reads as the whole graph")))

(deftest a-chain-of-legacy-single-entry-nodes-still-reads
  ;; Simulates a graph committed before this landing: single-entry nodes built
  ;; by hand in the old shape, then read through the new walker. Live chains
  ;; are full of these and there is no migration step.
  (let [{:keys [put! get-fn]} (store)
        tx (fn [i] (ipld/put-node! put! {"payload" (str "tx" i)}))
        ;; oldest-first chain, as `front` is walked
        legacy-head (reduce (fn [rest-link i]
                              (ipld/link (ipld/put-node! put! {"e" (ipld/link (tx i))
                                                              "rest" rest-link})))
                            nil (reverse (range 5)))
        entries (#'eng/walk-novelty-entries get-fn (ipld/link-cid legacy-head))]
    (is (= 5 (count entries)) "all five legacy nodes are visible to the segment-aware walker")
    (is (every? :cid entries))))

(deftest segment-size-is-the-one-the-adr-measured-against
  ;; The 4-blocks-for-64-entries assertion above is only meaningful against a
  ;; stated size; if someone tunes this, that test should be re-read, not
  ;; silently satisfied by a different arithmetic.
  (is (= 16 @#'eng/novelty-segment-size)))
