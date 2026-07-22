(ns kotobase-peer.object-store-worker-test
  (:require [cljs.test :refer [deftest is async]]
            [kotobase-peer.object-store.worker :as worker]))

(deftest immutable-object-and-block-namespaces-are-distinct
  (let [env #js {"MERKLE_S3_PREFIX" "test-prefix"}]
    (is (= "test-prefix/blocks/bafy-block" (worker/block-key env "bafy-block")))
    (is (= "test-prefix/objects/bafy-pack" (worker/object-key env "bafy-pack")))))

(defn- cas-bucket []
  (let [state (atom {:value nil :version 0})
        bucket
        #js {:get (fn [_]
                    (let [{:keys [value version]} @state]
                      (js/Promise.resolve
                       (when value
                         #js {:etag (str "v" version)
                              :text (fn [] (js/Promise.resolve value))}))))
             :put (fn [_ next opts]
                    (let [{:keys [value version]} @state
                          only-if (.-onlyIf opts)
                          matches (when only-if (.-etagMatches only-if))
                          absent? (when only-if (.-etagDoesNotMatch only-if))
                          won? (if matches
                                 (= matches (str "v" version))
                                 (and absent? (nil? value)))]
                      (when won? (reset! state {:value next :version (inc version)}))
                      (js/Promise.resolve (when won? #js {}))))}]
    {:bucket bucket :state state}))

(deftest compare-and-exchange-head-adapts-async-etag-cas
  (async done
    (let [{:keys [bucket state]} (cas-bucket)
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}]
      (-> (worker/compare-and-exchange-head! env "db" nil "cid-1")
          (.then (fn [actual]
                   (is (= "cid-1" actual))
                   (worker/compare-and-exchange-head! env "db" nil "cid-stale")))
          (.then (fn [actual]
                   (is (= "cid-1" actual) "stale expected returns actual head without overwrite")
                   (is (= "cid-1" (:value @state)))
                   (worker/compare-and-exchange-head! env "db" "cid-1" "cid-2")))
          (.then (fn [actual]
                   (is (= "cid-2" actual))
                   (is (= "cid-2" (:value @state)))
                   (done)))))))
