(ns kotobase-peer.materialized-view-test
  (:require [clojure.test :refer [deftest is testing]]
            [ipld.core :as ipld]
            [kotobase-peer.materialized-view :as view]))

(defn- entries [n]
  (mapv (fn [i]
          {:key (str "tenant-a/" (apply str (repeat (- 8 (count (str i))) "0")) i)
           :value {"id" i "title" (str "post-" i)}})
        (range n)))

(deftest packed-view-is-deterministic-and-independently-addressed
  (let [a (view/build-view {:view-id :posts/by-time :epoch 7
                            :block-rows 4 :entries (entries 10)})
        b (view/build-view {:view-id :posts/by-time :epoch 7
                            :block-rows 4 :entries (reverse (entries 10))})
        bundle (get-in a [:bundle :node])]
    (is (= (:pack-cid a) (:pack-cid b)))
    (is (= (get-in a [:bundle :cid]) (get-in b [:bundle :cid])))
    (is (= 3 (count (:blocks a))))
    (is (= 10 (get bundle "count")))
    (is (= (:pack-cid a) (ipld/link-cid (get bundle "pack-cid"))))
    (is (= [:object/put :block/put] (mapv :effect/type (:effects a))))))

(deftest bounded-query-fetches-only-overlapping-blocks
  (let [built (view/build-view {:view-id :posts/by-time :epoch 7
                                :block-rows 100 :entries (entries 1000)})
        bundle (get-in built [:bundle :node])
        result (view/query-packed bundle (:pack-bytes built)
                                  {:lower "tenant-a/00000450"
                                   :upper "tenant-a/00000459"
                                   :limit 10})]
    (is (= (range 450 460) (map #(get % "id") (:values result))))
    (is (= 1 (get-in result [:plan :estimated-requests])))
    (is (< (get-in result [:plan :estimated-bytes])
           (get bundle "pack-bytes")))))

(deftest query-blocks-are-cid-verified
  (let [built (view/build-view {:view-id :posts :epoch 1
                                :block-rows 10 :entries (entries 10)})
        descriptor (first (get-in built [:bundle :node "blocks"]))
        bytes (:bytes (first (:blocks built)))
        corrupt #?(:clj (aclone ^bytes bytes)
                   :cljs (.slice bytes))]
    #?(:clj (aset-byte ^bytes corrupt 0 (byte (bit-xor 1 (aget ^bytes corrupt 0))))
       :cljs (aset corrupt 0 (bit-xor 1 (aget corrupt 0))))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (view/decode-range descriptor corrupt)))))

(deftest query-plan-is-browser-host-effect-data
  (let [built (view/build-view {:view-id :entities :epoch 9
                                :block-rows 2 :entries (entries 5)})
        bundle (get-in built [:bundle :node])
        plan (view/range-query-plan
              {:bundle bundle
               :lower "tenant-a/00000002"
               :upper "tenant-a/00000003"})]
    (testing "the pure kernel asks only for bounded object ranges"
      (is (= 1 (:estimated-requests plan)))
      (is (every? #(= :object/range-get (:effect/type %)) (:need plan)))
      (is (every? pos? (map :length (:need plan)))))))

(deftest datom-projection-bridges-existing-materialized-view-rows
  (let [source (get-in (view/build-view {:view-id :source :epoch 1 :entries []})
                       [:bundle :cid])
        built (view/build-datom-projection
               {:view-id :person/cards :epoch 12 :source-manifest source
                :rows [{:e "alice" :a "name" :v_edn "\"Alice\"" :added true}
                       {:e "bob" :a "name" :v_edn "\"Bob\"" :added false}]})
        bundle (get-in built [:bundle :node])
        result (view/query-packed bundle (:pack-bytes built)
                                  {:lower (view/view-key ["alice" "name"])
                                   :upper (view/view-key ["alice" "name"])
                                   :limit 1})]
    (is (= 1 (:count built)))
    (is (= source (ipld/link-cid (get bundle "source-manifest"))))
    (is (= "alice" (get-in result [:values 0 "e"])))))
