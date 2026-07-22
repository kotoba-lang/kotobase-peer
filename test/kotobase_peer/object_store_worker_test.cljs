(ns kotobase-peer.object-store-worker-test
  (:require [cljs.test :refer [deftest is async]]
            [ipld.core :as ipld]
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

(deftest gc-marks-every-head-before-sweeping-shared-block-prefix
  (async done
    (let [child-a-bytes (ipld/encode {"value" "a"})
          child-a (ipld/cid child-a-bytes)
          root-a-bytes (ipld/encode {"child" (ipld/link child-a)})
          root-a (ipld/cid root-a-bytes)
          child-b-bytes (ipld/encode {"value" "b"})
          child-b (ipld/cid child-b-bytes)
          root-b-bytes (ipld/encode {"child" (ipld/link child-b)})
          root-b (ipld/cid root-b-bytes)
          orphan-bytes (ipld/encode {"orphan" true})
          orphan (ipld/cid orphan-bytes)
          prefix "test/"
          block-key #(str prefix "blocks/" %)
          objects (atom {(str prefix "heads/db-a") root-a
                         (str prefix "heads/db-b") root-b
                         (block-key root-a) root-a-bytes
                         (block-key child-a) child-a-bytes
                         (block-key root-b) root-b-bytes
                         (block-key child-b) child-b-bytes
                         (block-key orphan) orphan-bytes})
          old-date (js/Date. 0)
          bytes-object (fn [value]
                         #js {:arrayBuffer
                              (fn []
                                (js/Promise.resolve
                                 (.slice (.-buffer value)
                                         (.-byteOffset value)
                                         (+ (.-byteOffset value) (.-byteLength value)))))})
          bucket
          #js {:get (fn [key]
                      (let [value (get @objects key)]
                        (js/Promise.resolve
                         (when value
                           (if (string? value)
                             #js {:text (fn [] (js/Promise.resolve value))}
                             (bytes-object value))))))
               :list (fn [opts]
                       (let [wanted (.-prefix opts)
                             listed (->> (keys @objects)
                                         (filter #(.startsWith % wanted))
                                         (mapv (fn [key] #js {:key key :uploaded old-date})))]
                         (js/Promise.resolve #js {:objects (clj->js listed)
                                                 :truncated false})))
               :delete (fn [keys]
                         (doseq [key (js->clj keys)] (swap! objects dissoc key))
                         (js/Promise.resolve nil))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}]
      (-> (worker/gc-unreachable! env "db-a" 0 false)
          (.then (fn [audit]
                   (is (= 2 (:heads audit)))
                   (is (= 4 (:reachable audit)))
                   (is (= 1 (:candidates audit)) "only the orphan is collectible")
                   (is (= 0 (:deleted audit)))
                   (worker/gc-unreachable! env "db-a" 0 true)))
          (.then (fn [sweep]
                   (is (= 1 (:deleted sweep)))
                   (is (nil? (get @objects (block-key orphan))))
                   (is (every? #(contains? @objects (block-key %))
                               [root-a child-a root-b child-b])
                       "blocks reachable only from the other database head survive")
                   (swap! objects assoc (block-key orphan) orphan-bytes)
                   (let [head-list-calls (atom 0)
                         fenced-bucket
                         #js {:get (fn [key] (.get bucket key))
                              :list (fn [opts]
                                      (when (= (.-prefix opts) (str prefix "heads/"))
                                        (when (= 2 (swap! head-list-calls inc))
                                          (swap! objects assoc (str prefix "heads/db-c") root-a)))
                                      (.list bucket opts))
                              :delete (fn [keys] (.delete bucket keys))}
                         fenced-env #js {"MERKLE_BUCKET" fenced-bucket
                                         "MERKLE_S3_PREFIX" "test"}]
                     (worker/gc-unreachable! fenced-env "db-a" 0 true))))
          (.then (fn [fenced]
                   (is (= :heads-changed (:aborted fenced)))
                   (is (= 0 (:deleted fenced)))
                   (is (contains? @objects (block-key orphan))
                       "head mutation fences deletion of a previously marked candidate")
                   (done)))))))
