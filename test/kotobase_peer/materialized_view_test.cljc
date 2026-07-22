(ns kotobase-peer.materialized-view-test
  (:require [clojure.test :refer [deftest is testing]]
            [ipld.core :as ipld]
            [kotobase-peer.materialized-view :as view]))

(defn- entries [n]
  (mapv (fn [i]
          {:key (str "tenant-a/" (apply str (repeat (- 8 (count (str i))) "0")) i)
           :value {"id" i "title" (str "post-" i)}})
        (range n)))

(defn- xor-bytes [bytes key-byte]
  #?(:clj
     (byte-array (map #(unchecked-byte (bit-xor key-byte (bit-and 255 %))) bytes))
     :cljs
     (js/Uint8Array. (clj->js (mapv #(bit-xor key-byte %) bytes)))))

(defn- slice-bytes [bytes offset length]
  #?(:clj (java.util.Arrays/copyOfRange ^bytes bytes offset (+ offset length))
     :cljs (.slice bytes offset (+ offset length))))

(defn- test-encryptor [key-byte]
  (fn [{:keys [plaintext]}]
    {:bytes (xor-bytes plaintext key-byte)
     :algorithm "test/xor-v1"
     :nonce #?(:clj (byte-array [1 2 3]) :cljs (js/Uint8Array. #js [1 2 3]))}))

(defn- test-decryptor [keys]
  (fn [{:keys [key-id ciphertext]}]
    (xor-bytes ciphertext (get keys key-id))))

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

(deftest query-statistics-are-deterministic-scoped-bundle-metadata
  (let [statistics [{:pattern [nil "name" nil] :rows 101}
                    {:pattern [nil "role" "admin"] :rows 1}]
        built (view/build-view {:view-id :entities :epoch 1 :entries (entries 2)
                                :statistics-scope "tenant-a/public-v1"
                                :query-statistics (reverse statistics)})
        rebuilt (view/build-view {:view-id :entities :epoch 1 :entries (entries 2)
                                  :statistics-scope "tenant-a/public-v1"
                                  :query-statistics statistics})
        bundled (get-in built [:bundle :node "query-statistics"])]
    (is (= (get-in built [:bundle :cid]) (get-in rebuilt [:bundle :cid])))
    (is (= "tenant-a/public-v1" (get bundled "visibility-scope")))
    (is (= 1 (get bundled "epoch")))
    (is (= [101 1] (mapv #(get % "rows") (get bundled "clauses"))))))

(deftest query-statistics-require-an-explicit-visibility-scope
  (is (thrown? #?(:clj Exception :cljs js/Error)
               (view/build-view {:view-id :entities :epoch 1 :entries (entries 1)
                                 :query-statistics [{:pattern [nil nil nil]
                                                     :rows 1}]}))))

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

(deftest adjacent-blocks-coalesce-but-remain-individually-verified
  (let [built (view/build-view {:view-id :feed :epoch 1
                                :block-rows 2 :entries (entries 10)})
        bundle (get-in built [:bundle :node])
        result (view/query-packed bundle (:pack-bytes built)
                                  {:lower "tenant-a/00000000"
                                   :upper "tenant-a/00000009"})
        bounded-plan (view/range-query-plan
                      {:bundle bundle
                       :lower "tenant-a/00000000"
                       :upper "tenant-a/00000009"
                       :max-range-bytes 1})]
    (is (= 5 (count (get bundle "blocks"))))
    (is (= 1 (get-in result [:plan :estimated-requests])))
    (is (= 5 (count (get-in result [:plan :fetches 0 :descriptors]))))
    (is (= (range 10) (map #(get % "id") (:values result))))
    (is (= 5 (:estimated-requests bounded-plan)))))

(deftest non-contiguous-point-batch-deduplicates-blocks-and-filters-overfetch
  (let [built (view/build-view {:view-id :authors :epoch 1
                                :block-rows 10 :entries (entries 100)})
        bundle (get-in built [:bundle :node])
        requested-keys ["tenant-a/00000001" "tenant-a/00000007"
                        "tenant-a/00000042" "tenant-a/00000099"]
        plan (view/batch-point-query-plan {:bundle bundle :keys requested-keys})
        ranges (mapv #(slice-bytes (:pack-bytes built) (:offset %) (:length %))
                     (:fetches plan))
        plaintext-blocks
        (mapcat (fn [fetch bytes]
                  (map (fn [descriptor]
                         (slice-bytes bytes
                                      (- (get descriptor "offset") (:offset fetch))
                                      (get descriptor "length")))
                       (:descriptors fetch)))
                (:fetches plan) ranges)
        result (view/finish-logical-blocks-batch-query plan plaintext-blocks)]
    (is (= 4 (count result)))
    (is (= (set requested-keys) (set (keys result))))
    (is (= 3 (count (:descriptors plan))) "two keys share the first block")
    (is (= 3 (:estimated-requests plan)) "non-adjacent blocks stay bounded")
    (is (< (:estimated-bytes plan) (get bundle "pack-bytes")))))

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

(deftest encrypted-blocks-preserve-range-addressing-and-plaintext-cids
  (let [built (view/build-view {:view-id :private/feed :epoch 3
                                :block-rows 4 :entries (entries 10)
                                :key-id "tenant-a/dek-v1"
                                :encrypt-block-fn (test-encryptor 91)})
        bundle (get-in built [:bundle :node])
        descriptor (first (get bundle "blocks"))
        result (view/query-packed bundle (:pack-bytes built)
                                  {:lower "tenant-a/00000003"
                                   :upper "tenant-a/00000006"}
                                  (test-decryptor {"tenant-a/dek-v1" 91}))]
    (is (= "tenant-a/dek-v1" (get-in descriptor ["encryption" "key-id"])))
    (is (= "test/xor-v1" (get-in descriptor ["encryption" "algorithm"])))
    (is (ipld/link? (get descriptor "stored-cid")))
    (is (= (range 3 7) (map #(get % "id") (:values result))))
    (is (= 1 (get-in result [:plan :estimated-requests])))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (view/query-packed bundle (:pack-bytes built)
                                    {:lower "tenant-a/00000003"
                                     :upper "tenant-a/00000003"})))
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (view/query-packed bundle (:pack-bytes built)
                                    {:lower "tenant-a/00000003"
                                     :upper "tenant-a/00000003"}
                                    (test-decryptor {"tenant-a/dek-v1" 17}))))))

(deftest key-rotation-changes-envelope-not-logical-block-cid
  (let [v1 (view/build-view {:view-id :private/feed :epoch 3
                             :block-rows 5 :entries (entries 10)
                             :key-id "tenant-a/dek-v1"
                             :encrypt-block-fn (test-encryptor 41)})
        v2 (view/build-view {:view-id :private/feed :epoch 3
                             :block-rows 5 :entries (entries 10)
                             :key-id "tenant-a/dek-v2"
                             :encrypt-block-fn (test-encryptor 73)})]
    (is (= (map :cid (:blocks v1)) (map :cid (:blocks v2))))
    (is (not= (:pack-cid v1) (:pack-cid v2)))
    (is (not= (get-in v1 [:bundle :cid]) (get-in v2 [:bundle :cid])))
    (is (= "tenant-a/dek-v2"
           (get-in v2 [:bundle :node "blocks" 0 "encryption" "key-id"])))))

(deftest block-bloom-has-no-false-negatives-and-prunes-exact-misses
  (let [built (view/build-view {:view-id :lookup :epoch 1
                                :block-rows 100 :entries (entries 100)})
        bundle (get-in built [:bundle :node])
        filter-data (get-in bundle ["blocks" 0 "filter"])
        present (mapv :key (entries 100))
        absent (mapv #(str "tenant-a/000000" % "-missing") (range 10 100))
        pruned (count (remove #(view/bloom-might-contain? filter-data %) absent))
        missing-key "tenant-a/00000050-missing"
        result (view/query-packed bundle (:pack-bytes built)
                                  {:lower missing-key :upper missing-key :limit 1})]
    (is (every? #(view/bloom-might-contain? filter-data %) present))
    (is (>= pruned 80))
    (is (empty? (:values result)))
    (is (zero? (get-in result [:plan :estimated-requests])))
    (is (zero? (get-in result [:plan :estimated-bytes])))))

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

(deftest delta-view-chain-applies-assertions-and-retractions
  (let [base (view/build-view
              {:view-id :feed :epoch 10 :block-rows 2
               :entries [{:key "a" :value {"version" 1}}
                         {:key "b" :value {"version" 1}}]})
        delta (view/build-view-delta
               {:view-id :feed :epoch 11 :block-rows 2
                :previous-bundle (get-in base [:bundle :cid])
                :changes [{:key "a" :value nil :op :retract}
                          {:key "b" :value {"version" 2} :op :assert}
                          {:key "c" :value {"version" 1} :op :assert}]})
        generations [{:bundle (get-in delta [:bundle :node])
                      :pack-bytes (:pack-bytes delta)}
                     {:bundle (get-in base [:bundle :node])
                      :pack-bytes (:pack-bytes base)}]
        result (view/query-packed-chain generations {:lower "a" :upper "z"})
        compacted (view/compact-packed-chain
                   {:view-id :feed :epoch 11 :block-rows 2
                    :generations generations})
        compacted-result (view/query-packed
                          (get-in compacted [:bundle :node]) (:pack-bytes compacted)
                          {:lower "a" :upper "z"})]
    (is (= "delta" (get-in delta [:bundle :node "mode"])))
    (is (= (get-in base [:bundle :cid])
           (ipld/link-cid (get-in delta [:bundle :node "previous-bundle"]))))
    (is (= [{"version" 2} {"version" 1}] (:values result)))
    (is (= 2 (get-in result [:plan :estimated-requests])))
    (is (= (:values result) (:values compacted-result)))
    (is (= "base" (get-in compacted [:bundle :node "mode"])))
    (is (nil? (get-in compacted [:bundle :node "previous-bundle"])))))

(deftest packed-chain-compaction-is-depth-and-byte-bounded
  (let [bundles [{"pack-bytes" 40} {"pack-bytes" 30}]
        below (view/packed-chain-compaction-plan
               {:bundles (take 1 bundles)
                :max-generations 2 :max-pack-bytes 64})
        at-depth (view/packed-chain-compaction-plan
                  {:bundles bundles
                   :max-generations 2 :max-pack-bytes 80})
        over-bytes (view/packed-chain-compaction-plan
                    {:bundles bundles
                     :max-generations 2 :max-pack-bytes 64})
        built (view/build-view
               {:view-id :feed :epoch 1
                :entries [{:key "a" :value 1}]})]
    (is (false? (:compact? below)))
    (is (true? (:within-budget? below)))
    (is (true? (:compact? at-depth)))
    (is (true? (:within-budget? at-depth)))
    (is (true? (:compact? over-bytes)))
    (is (false? (:within-budget? over-bytes)))
    (is (thrown-with-msg?
         #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
         #"byte budget exceeded"
         (view/compact-packed-chain
          {:view-id :feed :epoch 1
           :generations [{:bundle (get-in built [:bundle :node])
                          :pack-bytes (:pack-bytes built)}]
           :max-input-bytes 0})))))
