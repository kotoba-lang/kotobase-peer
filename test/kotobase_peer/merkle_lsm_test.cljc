(ns kotobase-peer.merkle-lsm-test
  (:require [clojure.test :refer [deftest is testing]]
            [ipld.core :as ipld]
            [kotobase-peer.merkle-lsm :as lsm]))

(def entries
  [{:components ["alice" "name" "Alice"] :epoch 7 :op :assert :value "Alice"}
   {:components ["bob" "name" "Bob"] :epoch 8 :op :assert :value "Bob"}
   {:components ["alice" "name" "Alice"] :epoch 9 :op :retract :value "Alice"}])

(deftest canonical-key-is-framed-and-newest-first
  (is (not= (lsm/canonical-key :eavt "t" ["a|b" "c"] 1)
            (lsm/canonical-key :eavt "t" ["a" "b|c"] 1)))
  (is (neg? (compare (lsm/canonical-key :eavt "t" ["alice"] 9)
                     (lsm/canonical-key :eavt "t" ["alice"] 7)))))

(deftest run-is-canonical-and-range-described
  (let [a (lsm/build-run :eavt "tenant-a" entries)
        b (lsm/build-run :eavt "tenant-a" (reverse entries))]
    (is (= (:cid a) (:cid b)))
    (is (= (vec (:bytes a)) (vec (:bytes b))))
    (is (= 3 (:count a)))
    (is (= (:min-key a) (get (:node a) "min-key")))
    (is (= (:max-key a) (get (:node a) "max-key")))
    (is (= {:effect/type :block/put :cid (:cid a) :bytes (:bytes a)}
           (first (:effects a))))))

(deftest manifest-links-runs-and-is-deterministic
  (let [run (lsm/build-run :eavt "tenant-a" entries)
        opts {:db-id "tenant-a" :epoch 9 :safe-epoch 7
              :indexes {:eavt {:l0 [run]} :avet {:l1 []}}}
        a (lsm/build-manifest opts)
        b (lsm/build-manifest opts)
        run-link (get-in (:node a) ["indexes" "eavt" "l0" 0 "cid"])]
    (is (= (:cid a) (:cid b)))
    (is (ipld/link? run-link))
    (is (= (:cid run) (ipld/link-cid run-link)))))

(deftest publication-puts-before-cas
  (let [run (lsm/build-run :eavt "tenant-a" entries)
        manifest (lsm/build-manifest
                  {:db-id "tenant-a" :epoch 9 :indexes {:eavt {:l0 [run]}}})
        plan (lsm/publication-plan "tenant-a" "old-cid" [run] manifest)
        effects (:effects plan)]
    (is (= [:block/put :block/put :head/cas]
           (mapv :effect/type effects)))
    (is (= {:effect/type :head/cas :db-id "tenant-a"
            :expected "old-cid" :next (:cid manifest)}
           (peek effects)))))

(deftest flush-plan-builds-covering-runs-and-sparse-vaet
  (let [target (ipld/link (:cid (lsm/build-run :eavt "t" [])))
        plan (lsm/flush-plan
              {:db-id "db" :tenant "t" :epoch 12 :safe-epoch 10
               :expected "old"
               :datoms [{:e "a" :a "name" :v "Alice"}
                        {:e "a" :a "friend" :v target}]})
        effects (:effects plan)]
    (is (= #{:eavt :aevt :avet :vaet} (set (keys (:runs plan)))))
    (is (= 2 (get-in plan [:runs :eavt :count])))
    (is (= 1 (get-in plan [:runs :vaet :count])))
    (is (= :head/cas (:effect/type (peek effects))))
    (is (every? #(= :block/put (:effect/type %)) (pop effects)))
    (is (= (:cid (:manifest plan)) (get-in plan [:result :manifest])))))

(deftest invalid-manifest-safe-epoch-is-rejected
  (is (thrown? #?(:clj Exception :cljs js/Error)
               (lsm/build-manifest {:db-id "x" :epoch 2 :safe-epoch 3}))))

(deftest linked-cids-walks-decoded-ipld-values
  (let [a (:cid (lsm/build-run :eavt "t" []))
        b (:cid (lsm/build-run :avet "t" []))]
    (is (= #{a b}
           (lsm/linked-cids {"a" (ipld/link a)
                             "nested" [{"b" (ipld/link b)} "plain"]})))))

(deftest mvcc-merge-and-safe-epoch-compaction
  (let [r1 (lsm/build-run :eavt "t"
                          [{:components ["a" "name" "Alice"]
                            :epoch 1 :op :assert :value "Alice"}
                           {:components ["b" "name" "Bob"]
                            :epoch 2 :op :assert :value "Bob"}])
        r2 (lsm/build-run :eavt "t"
                          [{:components ["a" "name" "Alice"]
                            :epoch 3 :op :retract :value "Alice"}
                           {:components ["a" "name" "Alicia"]
                            :epoch 4 :op :assert :value "Alicia"}])
        compacted (lsm/compact-runs :eavt "t" 3 [r1 r2])]
    (testing "snapshot visibility honors assertions and tombstones"
      (is (= 2 (count (lsm/visible-rows [r1 r2] 2))))
      (is (= ["b"] (mapv #(first (get % "components"))
                          (lsm/visible-rows [r1 r2] 3))))
      (is (= #{"a" "b"}
             (set (map #(first (get % "components"))
                       (lsm/visible-rows [r1 r2] 4))))))
    (testing "compaction preserves snapshots at and above safe epoch"
      (is (= (lsm/visible-rows [r1 r2] 3)
             (lsm/visible-rows [compacted] 3)))
      (is (= (lsm/visible-rows [r1 r2] 4)
             (lsm/visible-rows [compacted] 4)))
      (is (= 3 (:count compacted))))))
