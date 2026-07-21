(ns kotobase-peer.object-store-worker-test
  (:require [cljs.test :refer [deftest is]]
            [kotobase-peer.object-store.worker :as worker]))

(deftest immutable-object-and-block-namespaces-are-distinct
  (let [env #js {"MERKLE_S3_PREFIX" "test-prefix"}]
    (is (= "test-prefix/blocks/bafy-block" (worker/block-key env "bafy-block")))
    (is (= "test-prefix/objects/bafy-pack" (worker/object-key env "bafy-pack")))))
