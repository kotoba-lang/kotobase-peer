(ns kotobase-peer.object-store-worker-test
  (:require [cljs.test :refer [deftest is async]]
            [clojure.string :as str]
            [ipld.core :as ipld]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.object-store.worker :as worker]
            [kotobase-peer.retention :as retention]))

(deftest immutable-object-and-block-namespaces-are-distinct
  (let [env #js {"MERKLE_S3_PREFIX" "test-prefix"}]
    (is (= "test-prefix/blocks/bafy-block" (worker/block-key env "bafy-block")))
    (is (= "test-prefix/objects/bafy-pack" (worker/object-key env "bafy-pack")))))

(deftest entity-readers-apply-mvcc-and-tombstones-across-manifests
  (async done
    (let [first-plan (lsm/flush-plan
                      {:db-id "db-a" :epoch 1
                       :datoms [{:e "alice" :a "role" :v "admin"}
                                {:e "bob" :a "role" :v "admin"}]})
          second-plan (lsm/flush-plan
                       {:db-id "db-a" :epoch 2
                        :previous (get-in first-plan [:manifest :cid])
                        :datoms [{:e "alice" :a "role" :v "admin" :op :retract}
                                 {:e "alice" :a "name" :v "Alice"}
                                 {:e "bob" :a "role" :v "admin" :op :retract}]})
          blocks (into {}
                       (map (fn [{:keys [cid bytes]}]
                              [(str "test/blocks/" cid) bytes]))
                       (filter #(= :block/put (:effect/type %))
                               (concat (:effects first-plan)
                                       (:effects second-plan))))
          head-cid (get-in second-plan [:manifest :cid])
          bucket
          #js {:get
               (fn [key]
                 (let [value (if (= key "test/heads/db-a")
                               head-cid (get blocks key))]
                   (js/Promise.resolve
                    (when value
                      #js {:etag "stable"
                           :text (fn [] (js/Promise.resolve value))
                           :arrayBuffer
                           (fn []
                             (js/Promise.resolve
                              (if (string? value)
                                (.-buffer (.encode (js/TextEncoder.) value))
                                (.slice (.-buffer value) (.-byteOffset value)
                                        (+ (.-byteOffset value)
                                           (.-byteLength value))))))}))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}]
      (-> (worker/find-entities! env "db-a" "")
          (.then
           (fn [entities]
             (is (= #{"alice"} (set (keys entities))))
             (is (= [["alice" "name" "Alice"]]
                    (mapv #(get % "components") (get entities "alice"))))
             (worker/find-latest-entity! env "db-a" "alice")))
          (.then
           (fn [rows]
             (is (= [["alice" "name" "Alice"]]
                    (mapv #(get % "components") rows)))
             (done)))
          (.catch
           (fn [error]
             (is false (str "MVCC entity read rejected: " error))
             (done)))))))

(deftest atomic-publication-persists-everything-before-head-cas
  (async done
    (let [entries (atom {})
          head-touched? (atom false)
          fail-object? (atom false)
          bucket
          #js {:get (fn [key]
                      (let [{:keys [value etag]} (get @entries key)]
                        (js/Promise.resolve
                         (when value
                           #js {:etag etag
                                :text (fn [] (js/Promise.resolve value))}))))
               :put (fn [key value opts]
                      (cond
                        (and @fail-object? (str/includes? key "/objects/"))
                        (js/Promise.reject (js/Error. "injected object failure"))

                        (str/includes? key "/heads/")
                        (do
                          (reset! head-touched? true)
                          (is (contains? @entries "test/blocks/root"))
                          (is (contains? @entries "test/objects/pack"))
                          (swap! entries assoc key {:value value :etag "head-v1"})
                          (js/Promise.resolve #js {:etag "head-v1"}))

                        :else
                        (do (swap! entries assoc key {:value value :etag "immutable"})
                            (js/Promise.resolve #js {:etag "immutable"}))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}
          plan {:effects [{:effect/type :block/put :cid "base" :bytes #js [1]}
                          {:effect/type :object/put :cid "pack" :bytes #js [2]}
                          {:effect/type :block/put :cid "root" :bytes #js [3]}
                          {:effect/type :head/cas :db-id "db-a"
                           :expected nil :next "root"}]}]
      (-> (worker/apply-atomic-publication! env plan)
          (.then (fn [result]
                   (is (:published? result))
                   (is @head-touched?)
                   (reset! entries {})
                   (reset! head-touched? false)
                   (reset! fail-object? true)
                   (worker/apply-atomic-publication! env plan)))
          (.then (fn [_]
                   (is false "injected immutable failure should reject")
                   (done)))
          (.catch (fn [_]
                    (is (not @head-touched?)
                        "head remains untouched after immutable write failure")
                    (done)))))))

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

(deftest retention-root-renewal-and-release-use-etag-cas
  (async done
    (let [{:keys [bucket]} (cas-bucket)
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}
          root (retention/root-node {:db-id "db-a" :kind :reader :id "query/a"
                                     :manifest-cid "cid-1" :epoch 4
                                     :expires-at 2000})]
      (is (= "test/roots/db-a/reader/query%2Fa"
             (worker/retention-root-key env "db-a" :reader "query/a")))
      (-> (worker/cas-retention-root! env root nil)
          (.then (fn [created]
                   (is (:won? created))
                   (worker/get-retention-root! env "db-a" :reader "query/a")))
          (.then (fn [{stored :root :keys [etag]}]
                   (is (= root stored))
                   (-> (worker/cas-retention-root!
                        env (assoc root "expires-at" 3000) "stale-etag")
                       (.then (fn [stale]
                                (is (not (:won? stale)))
                                (worker/release-retention-root!
                                 env stored etag 1500))))))
          (.then (fn [released]
                   (is (:won? released))
                   (is (= 1500 (get-in released [:root "released-at"])))
                   (done)))
          (.catch (fn [error]
                    (is false (str "retention registry promise rejected: " error))
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
                   (is (= :roots-changed (:aborted fenced)))
                   (is (= 0 (:deleted fenced)))
                   (is (contains? @objects (block-key orphan))
                       "head mutation fences deletion of a previously marked candidate")
                   (let [root (retention/root-node
                               {:db-id "db-a" :kind :legal-hold :id "case-1"
                                :manifest-cid orphan :epoch 2})
                         other-db-root (retention/root-node
                                        {:db-id "db-b" :kind :release :id "v1"
                                         :manifest-cid root-a :epoch 1})]
                     (swap! objects assoc (str prefix "roots/db-a/legal-hold/case-1")
                            (js/JSON.stringify (clj->js root))
                            (str prefix "roots/db-b/release/v1")
                            (js/JSON.stringify (clj->js other-db-root)))
                     (worker/gc-unreachable! env "db-a" 0 false 1000))))
          (.then (fn [pinned]
                   (is (= 2 (:active-retention-roots pinned)))
                   (is (= 1 (:safe-epoch pinned))
                       "global GC reports the oldest root across shared blocks")
                   (is (= 0 (:candidates pinned))
                       "a legal-hold root keeps an otherwise orphaned manifest live")
                   (worker/retention-safe-epoch! env "db-a" 1000)))
          (.then (fn [safe-epoch]
                   (is (= 2 safe-epoch)
                       "compaction and GC consume the same active root boundary")
                   (let [released (-> (retention/root-node
                                      {:db-id "db-a" :kind :legal-hold :id "case-1"
                                       :manifest-cid orphan :epoch 2})
                                     (retention/release-node 1100))]
                     (swap! objects assoc (str prefix "roots/db-a/legal-hold/case-1")
                            (js/JSON.stringify (clj->js released)))
                     (worker/gc-unreachable! env "db-a" 0 false 1200))))
          (.then (fn [released]
                   (is (= 1 (:active-retention-roots released)))
                   (is (= 1 (:safe-epoch released)))
                   (is (= 1 (:candidates released)))
                   (done)))
          (.catch (fn [error]
                    (is false (str "GC promise rejected: " error))
                    (done)))))))

(deftest compaction-scheduler-leases-reclaims-and-checkpoints
  (async done
    (let [prefix "test/"
          manifest-1 (lsm/build-manifest {:db-id "db-a" :epoch 1})
          manifest-2 (lsm/build-manifest {:db-id "db-a" :epoch 2
                                          :previous (:cid manifest-1)})
          view-bundle-node {"format" "kotobase/query-bundle"
                            "version" 1 "view-id" "kept" "epoch" 2
                            "source-manifest" (ipld/link (:cid manifest-2))
                            "pack-cid" (ipld/link (:cid manifest-1))
                            "count" 0 "blocks" []}
          view-bundle-bytes (ipld/encode view-bundle-node)
          view-bundle-cid (ipld/cid view-bundle-bytes)
          publication-node {"format" "kotobase/epoch-publication"
                            "version" 1 "db-id" "db-a" "epoch" 2
                            "base-manifest" (ipld/link (:cid manifest-2))
                            "statistics" (ipld/link (:cid manifest-1))
                            "views" {"kept" {"bundle" (ipld/link view-bundle-cid)
                                               "pack" (ipld/link (:cid manifest-1))
                                               "count" 0}}}
          publication-bytes (ipld/encode publication-node)
          publication-cid (ipld/cid publication-bytes)
          entries (atom {(str prefix "heads/db-a")
                         {:value publication-cid :etag "v-head"}
                         (str prefix "blocks/" publication-cid)
                         {:value publication-bytes :etag "v-publication"}
                         (str prefix "blocks/" view-bundle-cid)
                         {:value view-bundle-bytes :etag "v-view-bundle"}
                         (str prefix "blocks/" (:cid manifest-1))
                         {:value (:bytes manifest-1) :etag "v-m1"}
                         (str prefix "blocks/" (:cid manifest-2))
                         {:value (:bytes manifest-2) :etag "v-m2"}})
          version (atom 10)
          bucket
          #js {:list
               (fn [_]
                 (js/Promise.resolve #js {:objects #js [] :truncated false}))
               :get
               (fn [key]
                 (let [{:keys [value etag]} (get @entries key)]
                   (js/Promise.resolve
                    (when value
                      #js {:etag etag
                           :text (fn [] (js/Promise.resolve value))
                           :arrayBuffer
                           (fn []
                             (js/Promise.resolve
                              (if (string? value)
                                (.-buffer (.encode (js/TextEncoder.) value))
                                (.slice (.-buffer value)
                                        (.-byteOffset value)
                                        (+ (.-byteOffset value)
                                           (.-byteLength value))))))}))))
               :put
               (fn [key value opts]
                 (let [current (get @entries key)
                       condition (when opts (.-onlyIf opts))
                       matches (when condition (.-etagMatches condition))
                       absent (when condition (.-etagDoesNotMatch condition))
                       won? (cond
                              matches (= matches (:etag current))
                              (= "*" absent) (nil? current)
                              :else true)]
                   (if won?
                     (let [etag (str "v" (swap! version inc))]
                       (swap! entries assoc key {:value value :etag etag})
                       (js/Promise.resolve #js {:etag etag}))
                     (js/Promise.resolve nil))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}
          opts {:db-id "db-a" :owner "murakumo-a" :window-size 2
                :target-run-rows 16 :min-manifests 2 :l0-threshold 4
                :lease-ms 100 :now-ms 1000 :token "token-a"}]
      (-> (worker/resolve-database-head! env "db-a")
          (.then
           (fn [resolved]
             (is (= publication-cid (:head-cid resolved)))
             (is (= (:cid manifest-2) (:base-cid resolved)))
             (is (= publication-node (:publication resolved)))
             (worker/claim-compaction-lease! env opts)))
          (.then
           (fn [first-claim]
             (is (:claimed? first-claim))
             (is (= 1 (get-in first-claim [:lease "attempt"])))
             (-> (worker/claim-compaction-lease!
                  env (assoc opts :owner "murakumo-b" :now-ms 1001 :token "token-b"))
                 (.then (fn [contender]
                          (is (= :leased (:reason contender)))
                          (worker/renew-compaction-lease!
                           env (:lease first-claim) (:lease-etag first-claim) 100 1050)))
                 (.then (fn [renewed]
                          (is (:won? renewed))
                          (-> (worker/renew-compaction-lease!
                               env (:lease first-claim) (:lease-etag first-claim) 200 1050)
                              (.then (fn [stale-renewal]
                                       (is (not (:won? stale-renewal)))
                                       renewed))))))))
          (.then
           (fn [_]
             (worker/claim-compaction-lease!
              env (assoc opts :owner "murakumo-b" :now-ms 1150 :token "token-b"))))
          (.then
           (fn [reclaimed]
             (is (:claimed? reclaimed))
             (is (= 2 (get-in reclaimed [:lease "attempt"])))
             (worker/run-compaction-once!
              env (assoc opts :owner "murakumo-c" :now-ms 1250 :token "token-c"))))
          (.then
           (fn [result]
             (is (= :published (:outcome result)))
             (is (false? (:lease-fenced? result)))
             (let [next-head (:value (get @entries (str prefix "heads/db-a")))
                   next-root (ipld/decode
                              (:value (get @entries (str prefix "blocks/" next-head))))]
               (is (not= publication-cid next-head))
               (is (= "kotobase/epoch-publication" (get next-root "format")))
               (is (= (get publication-node "statistics")
                      (get next-root "statistics")))
               (is (= (get-in publication-node ["views" "kept" "pack"])
                      (get-in next-root ["views" "kept" "pack"])))
               (is (not= (:cid manifest-2)
                         (ipld/link-cid (get next-root "base-manifest"))))
               (let [next-bundle-cid
                     (ipld/link-cid
                      (get-in next-root ["views" "kept" "bundle"]))
                     next-bundle
                     (ipld/decode
                      (:value (get @entries
                                   (str prefix "blocks/" next-bundle-cid))))]
                 (is (= (ipld/link-cid (get next-root "base-manifest"))
                        (ipld/link-cid (get next-bundle
                                            "source-manifest"))))))
             (is (some #(str/includes? % "/checkpoints/") (keys @entries)))
             (worker/claim-compaction-lease!
              env (assoc opts :owner "murakumo-d" :now-ms 1400 :token "token-d"))))
          (.then
           (fn [idle]
             (is (= :no-pressure (:reason idle))
                 "a compacted head is not scheduled forever")
             (done)))
          (.catch (fn [error]
                    (is false (str "scheduler promise rejected: " error))
                    (done)))))))
