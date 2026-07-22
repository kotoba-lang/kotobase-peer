(ns kotobase-peer.compaction-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.compaction :as compaction]))

;; ============================================================================
;; M4 Compaction Tests
;; ============================================================================

(deftest m4-compaction-plan-selects-overlapping-runs
  (testing "compaction-plan should select overlapping L1 runs based on L0 key ranges"
    (let [l0-run-1 (lsm/build-run :eavt "t"
                                 [{:components ["a" "x"] :epoch 1 :op :assert :value 1}])
          l0-run-2 (lsm/build-run :eavt "t"
                                 [{:components ["b" "y"] :epoch 2 :op :assert :value 2}])
          l1-overlap (lsm/build-run :eavt "t"
                                   [{:components ["a" "z"] :epoch 0 :op :assert :value 0}])
          l1-disjoint (lsm/build-run :eavt "t"
                                    [{:components ["z" "z"] :epoch 0 :op :assert :value 0}])
          manifest (lsm/build-manifest
                   {:db-id "test" :epoch 2
                    :indexes {:eavt {:l0 [l0-run-1 l0-run-2]
                                     :l1 [l1-overlap l1-disjoint]}}})
          plan (compaction/compaction-plan manifest :eavt 0 2)]
      (is (some? plan))
      (is (= 2 (count (:l0-runs plan))))
      (is (= 1 (count (:target-runs plan)))))))

(deftest m4-range-overlap-detection
  (testing "key-range-overlap? should correctly identify overlapping ranges"
    (is (true? (compaction/key-range-overlap? ["a" "d"] ["c" "f"])))
    (is (true? (compaction/key-range-overlap? ["a" "d"] ["a" "d"])))
    (is (false? (compaction/key-range-overlap? ["a" "c"] ["d" "f"])))))

(deftest m4-gc-candidates-finds-unreachable-blocks
  (testing "gc-candidates should identify orphaned CIDs"
    (let [run-1 (lsm/build-run :eavt "t" [{:components ["a"] :epoch 1 :op :assert :value 1}])
          run-2 (lsm/build-run :eavt "t" [{:components ["b"] :epoch 2 :op :assert :value 2}])
          manifest-1 (lsm/build-manifest
                     {:db-id "test" :epoch 1
                      :indexes {:eavt {:l0 [run-1]}}})
          manifest-2 (lsm/build-manifest
                     {:db-id "test" :epoch 2
                      :indexes {:eavt {:l0 [run-2]}}
                      :previous (:cid manifest-1)})

          all-cids #{(:cid run-1) (:cid run-2) (:cid manifest-1) (:cid manifest-2) "orphan-cid"}
          candidates (compaction/gc-candidates manifest-2 all-cids manifest-1)]
      (is (= #{"orphan-cid"} candidates)))))

(deftest m4-safe-epoch-pin-prevents-gc
  (testing "minimum-safe-epoch should consider all reader and replica epochs"
    (let [pins [(compaction/safe-epoch-pin {:manifest-cid "m1" :epoch 10 :epoch-readers [7 8]})
                (compaction/safe-epoch-pin {:manifest-cid "m2" :epoch 5 :legal-hold 5})]
          safe-epoch (compaction/minimum-safe-epoch pins)]
      (is (= 5 safe-epoch)))))

(deftest m4-l0-sublevels-organization
  (testing "l0-sublevels should partition runs into buckets"
    (let [runs [(lsm/build-run :eavt "t" [{:components ["a"] :epoch 1 :op :assert :value 1}])
                (lsm/build-run :eavt "t" [{:components ["b"] :epoch 2 :op :assert :value 2}])
                (lsm/build-run :eavt "t" [{:components ["c"] :epoch 3 :op :assert :value 3}])]
          sublevels (compaction/l0-sublevels runs 2)]
      (is (= 2 (count sublevels)))
      (is (= 2 (count (:l0s0 sublevels))))
      (is (= 1 (count (:l0s1 sublevels)))))))

(deftest m4-gc-live-cids-collects-reachable
  (testing "gc-live-cids should collect all reachable CIDs from manifest"
    (let [run (lsm/build-run :eavt "t" [{:components ["a"] :epoch 1 :op :assert :value 1}])
          manifest (lsm/build-manifest
                   {:db-id "test" :epoch 1
                    :indexes {:eavt {:l0 [run]}}})
          live-cids (compaction/gc-live-cids manifest)]
      (is (contains? live-cids (:cid manifest))))))
