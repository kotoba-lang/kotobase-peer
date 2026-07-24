(ns kotobase-peer.object-store-worker-test
  (:require [cljs.test :refer [deftest is async]]
            [clojure.string :as str]
            [goog.object :as gobj]
            [ipld.core :as ipld]
            [kotobase-peer.database-restore :as database-restore]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.object-store.worker :as worker]
            [kotobase-peer.resumable-execution :as resumable]
            [kotobase-peer.retention :as retention]))

(deftest immutable-object-and-block-namespaces-are-distinct
  (let [env #js {"MERKLE_S3_PREFIX" "test-prefix"}]
    (is (= "test-prefix/blocks/bafy-block" (worker/block-key env "bafy-block")))
    (is (= "test-prefix/objects/bafy-pack" (worker/object-key env "bafy-pack")))))

(deftest reachability-treats-materialized-objects-as-opaque-cid-leaves
  (async done
    (let [object-bytes (ipld/encode {"opaque-pack" true})
          object-cid (str (ipld/cid object-bytes))
          root-bytes
          (ipld/encode {"pack" (ipld/link object-cid)})
          root-cid (str (ipld/cid root-bytes))
          values {(str "opaque/blocks/" root-cid) root-bytes
                  (str "opaque/objects/" object-cid) object-bytes}
          bucket
          #js {:get
               (fn [key]
                 (js/Promise.resolve
                  (when-let [bytes (get values key)]
                    #js {:arrayBuffer
                         (fn []
                           (js/Promise.resolve
                            (.slice (.-buffer bytes)
                                    (.-byteOffset bytes)
                                    (+ (.-byteOffset bytes)
                                       (.-byteLength bytes)))))})))}
          env #js {"MERKLE_BUCKET" bucket
                   "MERKLE_S3_PREFIX" "opaque"}]
      (-> (worker/reachable-cids! env root-cid)
          (.then
           (fn [reachable]
             (is (= #{root-cid object-cid} reachable))
             (done)))
          (.catch
           (fn [error]
             (is false
                 (str "opaque reachability rejected: " error))
             (done)))))))

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
          eavt-run-keys
          (->> (concat (:effects first-plan) (:effects second-plan))
               (keep (fn [{:keys [cid bytes] :as effect}]
                       (when (and (= :block/put (:effect/type effect))
                                  (= "kotobase/merkle-run"
                                     (get (ipld/decode bytes) "format"))
                                  (= "eavt" (get (ipld/decode bytes) "index")))
                         (str "test/blocks/" cid))))
               set)
          gets (atom [])
          first-prefix-page (atom nil)
          head-cid (get-in second-plan [:manifest :cid])
          bucket
          #js {:get
               (fn [key]
                 (swap! gets conj key)
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
             (reset! gets [])
             (worker/find-exact-entities! env "db-a" ["alice" "bob"])))
          (.then
           (fn [entities]
             (is (= #{"alice"} (set (keys entities))))
             (is (= 1 (count (filter #{"test/heads/db-a"} @gets)))
                 "the exact entity set resolves the head only once")
             (is (= (count eavt-run-keys)
                    (count (filter eavt-run-keys @gets)))
                 "overlapping selected run refs are loaded once by CID")
             (reset! gets [])
             (worker/find-index-prefixes!
              env "db-a" :aevt [["name"] ["role"]])))
          (.then
           (fn [rows]
             (is (= [["name" "alice" "Alice"]]
                    (mapv #(get % "components") rows)))
             (is (= 1 (count (filter #{"test/heads/db-a"} @gets)))
                 "an index-prefix batch resolves the head once")
             (worker/find-index-prefix-page!
              env "db-a" :aevt [["name"] ["role"]] {:limit 1})))
          (.then
           (fn [page]
             (reset! first-prefix-page page)
             (is (= [["name" "alice" "Alice"]]
                    (mapv #(get % "components") (:rows page))))
             (is (false? (:done? page)))
             (worker/find-index-prefix-page!
              env "db-a" :aevt [["name"] ["role"]]
              {:limit 1 :after (:cursor page)})))
          (.then
           (fn [page]
             (is (empty? (:rows page))
                 "a tombstone-only logical page still advances")
             (is (string? (:cursor page)))
             (is (not= (:cursor @first-prefix-page) (:cursor page)))
             (done)))
          (.catch
           (fn [error]
             (is false (str "MVCC entity read rejected: " error))
             (done)))))))

(deftest database-restore-pointer-reclaims-page-progress-with-etag-cas
  (async done
    (let [prefix "restore-state/"
          entries (atom {})
          version (atom 0)
          bucket
          #js {:get
               (fn [key]
                 (let [{:keys [value etag]} (get @entries key)]
                   (js/Promise.resolve
                    (when value
                      #js {:etag etag
                           :text (fn [] (js/Promise.resolve value))
                           :arrayBuffer
                           (fn []
                             (let [bytes
                                   (if (string? value)
                                     (.encode (js/TextEncoder.) value)
                                     value)]
                               (js/Promise.resolve
                                (.slice (.-buffer bytes)
                                        (.-byteOffset bytes)
                                        (+ (.-byteOffset bytes)
                                           (.-byteLength bytes))))))}))))
               :put
               (fn [key value opts]
                 (let [current (get @entries key)
                       condition (when opts (.-onlyIf opts))
                       matches (when condition (.-etagMatches condition))
                       absent (when condition
                                (.-etagDoesNotMatch condition))
                       won? (cond
                              matches (= matches (:etag current))
                              (= "*" absent) (nil? current)
                              :else true)]
                   (if-not won?
                     (js/Promise.resolve nil)
                     (let [etag (str "v" (swap! version inc))]
                       (swap! entries assoc key
                              {:value value :etag etag})
                       (js/Promise.resolve #js {:etag etag})))))}
          env #js {"MERKLE_BUCKET" bucket
                   "MERKLE_S3_PREFIX" "restore-state"}
          head (ipld/cid (ipld/encode {"head" 1}))
          task (database-restore/restore-task
                {:inventory-cid
                 (ipld/cid (ipld/encode {"inventory" 1}))
                 :target-db-id "target"
                 :head-cid head
                 :entry-count 2
                 :page-count 2})
          claim-opts {:task task :owner "worker-a" :token "token-a"
                      :now-ms 1000 :lease-ms 100}]
      (-> (worker/claim-database-restore! env claim-opts)
          (.then
           (fn [claim]
             (is (:claimed? claim))
             (is (= 1 (get-in claim [:pointer "attempt"])))
             (worker/advance-database-restore-checkpoint!
              env (merge claim
                         {:page-ordinal 0
                          :page-cid
                          (ipld/cid (ipld/encode {"page" 0}))
                          :entry-count 1
                          :restored 1
                          :already-present 0
                          :first-entry ["blocks" "cid-a"]
                          :last-entry ["blocks" "cid-a"]
                          :now-ms 1020
                          :lease-ms 100}))))
          (.then
           (fn [advanced]
             (is (:advanced? advanced))
             (is (= 1
                    (get-in advanced [:checkpoint "next-page"])))
             (worker/claim-database-restore!
              env (assoc claim-opts
                         :owner "worker-b"
                         :token "token-b"
                         :now-ms 1201))))
          (.then
           (fn [reclaimed]
             (is (:claimed? reclaimed))
             (is (= 2 (get-in reclaimed [:pointer "attempt"])))
             (is (= 1
                    (get-in reclaimed [:checkpoint "next-page"])))
             (is (= 1
                    (get-in reclaimed
                            [:checkpoint "processed-entries"])))
             (swap! entries assoc
                    (str prefix "heads/conflict")
                    {:value "different" :etag "conflict"})
             (worker/claim-database-restore!
              env (assoc claim-opts
                         :task
                         (database-restore/restore-task
                          {:inventory-cid
                           (ipld/cid (ipld/encode {"inventory" 1}))
                           :target-db-id "conflict"
                           :head-cid head
                           :entry-count 2
                           :page-count 2})
                         :owner "worker-c"
                         :token "token-c"
                         :now-ms 1300))))
          (.then
           (fn [conflict]
             (is (= :target-head-conflict (:reason conflict)))
             (done)))
          (.catch
           (fn [error]
             (is false
                 (str "database restore pointer rejected: " error
                      "\n" (.-stack error)))
             (done)))))))

(deftest checkpoint-compaction-streams-physical-blocks-with-a-bounded-working-set
  (async done
    (let [entries (atom {})
          version (atom 0)
          block-gets (atom 0)
          response
          (fn [{:keys [value etag]}]
            #js {:etag etag
                 :text (fn [] (js/Promise.resolve value))
                 :arrayBuffer
                 (fn []
                   (let [bytes (if (string? value)
                                 (.encode (js/TextEncoder.) value)
                                 value)]
                     (js/Promise.resolve
                      (.slice (.-buffer bytes) (.-byteOffset bytes)
                              (+ (.-byteOffset bytes) (.-byteLength bytes))))))})
          bucket
          #js {:get
               (fn [key]
                 (let [stored (get @entries key)]
                   (when (and stored
                              (str/includes? key "/blocks/")
                              (= "kotobase/merkle-run-block"
                                 (get (ipld/decode (:value stored)) "format")))
                     (swap! block-gets inc))
                   (js/Promise.resolve (when stored (response stored)))))
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
                   (if-not won?
                     (js/Promise.resolve nil)
                     (let [etag (str "v" (swap! version inc))]
                       (swap! entries assoc key {:value value :etag etag})
                       (js/Promise.resolve #js {:etag etag})))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "stream"}
          datoms (mapv (fn [n]
                         {:e (str "entity-" (.padStart (str n) 6 "0"))
                          :a "value" :v n})
                       (range 512))
          first-plan (lsm/flush-plan
                      {:db-id "db-stream" :epoch 1 :target-run-rows 512
                       :block-rows 64 :datoms datoms})
          first-head (get-in first-plan [:manifest :cid])
          second-plan (lsm/flush-plan
                       {:db-id "db-stream" :epoch 2 :previous first-head
                        :expected first-head :target-run-rows 512
                        :block-rows 64 :datoms datoms})]
      (-> (worker/apply-atomic-publication! env first-plan)
          (.then (fn [_] (worker/apply-atomic-publication! env second-plan)))
          (.then (fn [_]
                   (worker/compact-head! env "db-stream" 64 128 2)))
          (.then
           (fn [compacted?]
             (is compacted?)
             (is (pos? @block-gets)
                 "compaction reads physical data blocks rather than whole runs")
             (worker/get-head env "db-stream")))
          (.then
           (fn [{:keys [value]}]
             (worker/get-node! env value)))
          (.then
           (fn [manifest]
             (let [metrics (get-in manifest
                                   ["statistics" "stream-metrics"])]
               (is (true? (get-in manifest
                                  ["statistics" "streaming-compaction"])))
               (is (= @block-gets (get metrics "input-block-gets")))
               (is (<= (get metrics "max-buffered-input-rows") 128)
                   "one 64-row block per two overlapping runs is retained")
               (is (<= (get metrics "max-buffered-input-bytes")
                       (* 64 1024 1024)))
               (is (= 1536 (get metrics "output-rows"))
                   "three covering indexes each retain 512 logical rows")
               (done))))
          (.catch
           (fn [error]
             (is false (str "streaming compaction rejected: " error "\n"
                            (.-stack error)))
             (done)))))))

(deftest repeated-checkpoint-compaction-inherits-replaces-and-fences
  (async done
    (let [entries (atom {})
          version (atom 0)
          fail-next-head-cas? (atom false)
          response
          (fn [{:keys [value etag]}]
            #js {:etag etag
                 :text (fn []
                         (js/Promise.resolve
                          (if (string? value)
                            value
                            (.decode (js/TextDecoder.) value))))
                 :arrayBuffer
                 (fn []
                   (let [bytes (if (string? value)
                                 (.encode (js/TextEncoder.) value)
                                 value)]
                     (js/Promise.resolve
                      (.slice (.-buffer bytes) (.-byteOffset bytes)
                              (+ (.-byteOffset bytes) (.-byteLength bytes))))))})
          bucket
          #js {:get (fn [key]
                      (js/Promise.resolve
                       (some-> (get @entries key) response)))
               :put
               (fn [key value opts]
                 (let [current (get @entries key)
                       only-if (when opts (gobj/get opts "onlyIf"))
                       matches (when only-if (gobj/get only-if "etagMatches"))
                       absent (when only-if
                                (gobj/get only-if "etagDoesNotMatch"))
                       conditional? (some? only-if)
                       won? (or (not conditional?)
                                (and matches (= matches (:etag current)))
                                (and (= "*" absent) (nil? current)))
                       head? (str/includes? key "/heads/")]
                   (if (and head? @fail-next-head-cas?)
                     (do (reset! fail-next-head-cas? false)
                         (js/Promise.resolve nil))
                     (if-not won?
                       (js/Promise.resolve nil)
                       (let [etag (str "v" (swap! version inc))]
                         (swap! entries assoc key {:value value :etag etag})
                         (js/Promise.resolve #js {:etag etag}))))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "checkpoint"}
          head-key "checkpoint/heads/db-checkpoint"
          legacy-plan
          (lsm/flush-plan
           {:db-id "db-checkpoint" :epoch 0
            :datoms [{:e "zara" :a "role" :v "admin"}]})
          first-plan
          (lsm/flush-plan
           {:db-id "db-checkpoint" :epoch 1
            :previous (get-in legacy-plan [:manifest :cid])
            :expected (get-in legacy-plan [:manifest :cid])
            :datoms [{:e "alice" :a "role" :v "admin"}
                     {:e "bob" :a "role" :v "admin"}]})
          first-checkpoint-head (atom nil)
          second-checkpoint-head (atom nil)]
      (-> (worker/apply-atomic-publication! env legacy-plan)
          (.then (fn [published]
                   (is (:published? published))
                   (worker/apply-atomic-publication! env first-plan)))
          (.then (fn [published]
                   (is (:published? published))
                   (worker/compact-head! env "db-checkpoint" 1 4096 1)))
          (.then
           (fn [compacted?]
             (is compacted?)
             (reset! first-checkpoint-head (:value (get @entries head-key)))
             (let [second-plan
                   (lsm/flush-plan
                    {:db-id "db-checkpoint" :epoch 2
                     :previous @first-checkpoint-head
                     :expected @first-checkpoint-head
                     :datoms [{:e "alice" :a "role" :v "admin" :op :retract}
                              {:e "carol" :a "role" :v "admin"}]})]
               (worker/apply-atomic-publication! env second-plan))))
          (.then (fn [published]
                   (is (:published? published))
                   (worker/compact-head! env "db-checkpoint" 64 4096 1)))
          (.then
           (fn [compacted?]
             (is compacted?)
             (reset! second-checkpoint-head (:value (get @entries head-key)))
             (worker/find-entities! env "db-checkpoint" "")))
          (.then
           (fn [entities]
             (is (= #{"bob" "carol" "zara"} (set (keys entities)))
                 "the second checkpoint keeps inherited refs, legacy tail, and tombstones")
             (worker/find-index-prefix-page!
              env "db-checkpoint" :eavt [["alice"] ["bob"] ["zara"]]
              {:limit 10 :head-cid @first-checkpoint-head})))
          (.then
           (fn [page]
             (is (= #{"alice" "bob" "zara"}
                    (set (map #(first (get % "components")) (:rows page))))
                 "an old pinned checkpoint and its partial legacy tail remain readable")
             (worker/get-node! env @second-checkpoint-head)))
          (.then
           (fn [manifest]
             (is (= 2
                    (get-in manifest
                            ["statistics" "range-directory-version"])))
             (is (pos? (get-in manifest
                               ["statistics"
                                "range-directory-page-count"])))
             (worker/get-node!
              env (ipld/link-cid
                   (get-in manifest ["statistics" "range-directory"])))))
          (.then
           (fn [directory]
             (is (lsm/paged-range-directory? directory))
             (worker/get-node!
              env
              (ipld/link-cid
               (get (first
                     (lsm/range-directory-page-descriptors directory :eavt))
                    "cid")))))
          (.then
           (fn [directory-page]
             (is (= 1 (count (get directory-page "refs")))
                 "the repeated checkpoint replaces overlapping inherited refs")
             (let [third-plan
                   (lsm/flush-plan
                    {:db-id "db-checkpoint" :epoch 3
                     :previous @second-checkpoint-head
                     :expected @second-checkpoint-head
                     :datoms [{:e "alice" :a "role" :v "admin"}]})]
               (worker/apply-atomic-publication! env third-plan))))
          (.then
           (fn [published]
             (is (:published? published))
             (let [winner (:value (get @entries head-key))]
               (reset! fail-next-head-cas? true)
               (-> (worker/compact-head! env "db-checkpoint" 64 4096 1)
                   (.then (fn [compacted?]
                            (is (false? compacted?))
                            (is (= winner (:value (get @entries head-key)))
                                "a HeadCAS loser never publishes its directory")
                            (worker/compact-head! env "db-checkpoint" 64 4096 1)))))))
          (.then
           (fn [compacted?]
             (is compacted?)
             (worker/find-entities! env "db-checkpoint" "")))
          (.then
           (fn [entities]
             (is (= #{"alice" "bob" "carol" "zara"}
                    (set (keys entities))))
             (done)))
          (.catch
           (fn [error]
             (is false (str "repeated checkpoint compaction rejected: " error))
             (done)))))))

(deftest prefix-pages-skip-completed-physical-run-blocks
  (async done
    (let [plan (lsm/flush-plan
                {:db-id "db-blocks" :epoch 1 :target-run-rows 4096
                 :datoms (mapv (fn [n]
                                 {:e (str "team-" (.padStart (str n) 4 "0"))
                                  :a "member" :v "alice"})
                               (range 300))})
          effects (filter #(= :block/put (:effect/type %)) (:effects plan))
          blocks (into {} (map (fn [{:keys [cid bytes]}]
                                 [(str "paged/blocks/" cid) bytes])) effects)
          head-cid (get-in plan [:manifest :cid])
          run-ref (first (get-in plan [:manifest :node "indexes" "aevt" "l0"]))
          data-cids (set (map #(str "paged/blocks/" (ipld/link-cid (get % "cid")))
                              (get run-ref "blocks")))
          root-key (str "paged/blocks/" (ipld/link-cid (get run-ref "cid")))
          gets (atom [])
          track-concurrency? (atom false)
          active-data-gets (atom 0)
          max-data-gets (atom 0)
          bucket
          #js {:get
               (fn [key]
                 (swap! gets conj key)
                 (let [value (if (= key "paged/heads/db-blocks")
                               head-cid (get blocks key))
                       response
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
                                              (.-byteLength value))))))})]
                   (if (and @track-concurrency? (contains? data-cids key))
                     (js/Promise.
                      (fn [resolve _]
                        (let [active (swap! active-data-gets inc)]
                          (swap! max-data-gets max active)
                          (js/setTimeout
                           (fn []
                             (swap! active-data-gets dec)
                             (resolve response))
                           1))))
                     (js/Promise.resolve response))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "paged"}]
      (letfn [(step [after rows scanned]
                (-> (worker/find-index-prefix-page!
                     env "db-blocks" :aevt [["member"]]
                     (cond-> {:limit 64} after (assoc :after after)))
                    (.then
                     (fn [page]
                       (let [next-rows (into rows (:rows page))
                             next-scanned (conj scanned (:scanned-blocks page))]
                         (if (:done? page)
                           (do
                             (is (= 300 (count next-rows)))
                             (is (= [1 2 1 2 1] next-scanned))
                             (is (= 1 @max-data-gets)
                                 "a cursor never speculatively fetches its successor")
                             (is (not-any? #{root-key} @gets)
                                 "manifest block descriptors bypass the run root")
                             (is (= 7 (count (filter data-cids @gets)))
                                 "continuations skip data blocks entirely before the cursor")
                             (reset! gets [])
                             (cached-step nil [] [] 0))
                           (step (:cursor page) next-rows next-scanned)))))))
              (cached-step [after remainder rows fetched]
                (-> (worker/find-index-prefix-page!
                     env "db-blocks" :aevt [["member"]]
                     (cond-> {:limit 64 :remainder-max-bytes 1048576
                              :block-remainder remainder}
                       after (assoc :after after)))
                    (.then
                     (fn [page]
                       (let [next-rows (into rows (:rows page))
                             next-fetched (+ fetched (:fetched-blocks page))]
                         (if (:done? page)
                           (do
                             (is (= 300 (count next-rows)))
                             (is (= 3 next-fetched)
                                 "each physical block is fetched once across pages")
                             (is (= 3 (count (filter data-cids @gets)))
                                 "inline CID-verified remainder removes repeated R2 GETs")
                             (done))
                           (cached-step (:cursor page) (:block-remainder page)
                                        next-rows next-fetched)))))))]
        (-> (worker/find-index-prefixes!
             env "db-blocks" :aevt [["member"]])
            (.then
             (fn [rows]
               (is (= 300 (count rows))
                   "the compatibility full reader hydrates every data block")
               (is (some #{root-key} @gets)
                   "the full reader validates the run root")
               (reset! gets [])
               (reset! track-concurrency? true)
               (worker/find-index-prefix-page!
                env "db-blocks" :aevt [["member"]]
                {:limit 64 :head-cid head-cid})))
            (.then
             (fn [page]
               (is (= 64 (count (:rows page))))
               (is (not-any? #{"paged/heads/db-blocks"} @gets)
                   "a pinned page never re-reads the mutable head")
               (reset! gets [])
               (step nil [] [])))
            (.catch (fn [error]
                      (is false (str "subblock prefix page rejected: " error))
                      (done))))))))

(deftest prefix-pages-fetch-required-independent-runs-in-bounded-waves
  (async done
    (let [datoms-for (fn [numbers]
                       (mapv (fn [n]
                               {:e (str "team-" (.padStart (str n) 4 "0"))
                                :a "member" :v "alice"})
                             numbers))
          plan-a (lsm/flush-plan
                  {:db-id "db-wave" :epoch 1 :target-run-rows 4096
                   :datoms (datoms-for (range 0 260 2))})
          plan-b (lsm/flush-plan
                  {:db-id "db-wave" :epoch 2 :target-run-rows 4096
                   :previous (get-in plan-a [:manifest :cid])
                   :expected (get-in plan-a [:manifest :cid])
                   :datoms (datoms-for (range 1 260 2))})
          effects (filter #(= :block/put (:effect/type %))
                          (concat (:effects plan-a) (:effects plan-b)))
          blocks (into {} (map (fn [{:keys [cid bytes]}]
                                 [(str "wave/blocks/" cid) bytes])) effects)
          head-cid (get-in plan-b [:manifest :cid])
          refs [(first (get-in plan-a [:manifest :node "indexes" "aevt" "l0"]))
                (first (get-in plan-b [:manifest :node "indexes" "aevt" "l0"]))]
          data-cids (set (mapcat (fn [ref]
                                  (map #(str "wave/blocks/"
                                             (ipld/link-cid (get % "cid")))
                                       (get ref "blocks")))
                                refs))
          wave-byte-cap (apply max
                               (map #(get-in % ["blocks" 0 "encoded-bytes"])
                                    refs))
          gets (atom [])
          active (atom 0)
          max-active (atom 0)
          bucket
          #js {:get
               (fn [key]
                 (swap! gets conj key)
                 (let [value (if (= key "wave/heads/db-wave")
                               head-cid (get blocks key))
                       response
                       (when value
                         #js {:etag "stable"
                              :text (fn [] (js/Promise.resolve value))
                              :arrayBuffer
                              (fn []
                                (js/Promise.resolve
                                 (.slice (.-buffer value) (.-byteOffset value)
                                         (+ (.-byteOffset value)
                                            (.-byteLength value)))))})]
                   (if (contains? data-cids key)
                     (js/Promise.
                      (fn [resolve _]
                        (let [current (swap! active inc)]
                          (swap! max-active max current)
                          (js/setTimeout
                           (fn [] (swap! active dec) (resolve response)) 5))))
                     (js/Promise.resolve response))))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "wave"}]
      (-> (worker/find-index-prefix-page!
           env "db-wave" :aevt [["member"]]
           {:limit 64 :block-get-concurrency 2})
          (.then
           (fn [page]
             (is (= 64 (count (:rows page))))
             (is (= 2 (:scanned-runs page)))
             (is (= 2 (:scanned-blocks page)))
             (is (= 2 (:fetched-blocks page)))
             (is (pos? (:fetched-block-bytes page)))
             (is (zero? (:unknown-fetched-block-bytes page)))
             (is (= 2 (:max-concurrent-block-gets page)))
             (is (= (:fetched-block-bytes page)
                    (:max-wave-block-bytes page)))
             (is (= 2 @max-active)
                 "only independently selected runs overlap in one GET wave")
             (is (= 2 (count (filter data-cids @gets)))
                 "parallelism does not add a speculative successor GET")
             (reset! gets [])
             (reset! max-active 0)
             (worker/find-index-prefix-page!
              env "db-wave" :aevt [["member"]]
              {:limit 64 :block-get-concurrency 1})))
          (.then
           (fn [page]
             (is (= 64 (count (:rows page))))
             (is (= 1 (:max-concurrent-block-gets page)))
             (is (= 1 @max-active)
                 "a concurrency cap of one is enforced by observed GETs")
             (is (= 2 (count (filter data-cids @gets)))
                 "serial and parallel waves fetch the same required blocks")
             (reset! gets [])
             (reset! max-active 0)
             (worker/find-index-prefix-page!
              env "db-wave" :aevt [["member"]]
              {:limit 64 :block-get-concurrency 2
               :block-get-max-wave-bytes wave-byte-cap})))
          (.then
           (fn [page]
             (is (= 64 (count (:rows page))))
             (is (= 1 (:max-concurrent-block-gets page))
                 "the byte cap can reduce a nominally two-way wave")
             (is (= 1 @max-active))
             (is (<= (:max-wave-block-bytes page) wave-byte-cap))
             (is (= 2 (count (filter data-cids @gets))))
             (done)))
          (.catch
           (fn [error]
             (is false (str "independent run wave rejected: " error))
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

(deftest host-safe-epoch-oracle-unifies-root-kinds-and-clock-skew
  (async done
    (let [roots
          [(retention/root-node
            {:db-id "db-a" :kind :reader :id "reader"
             :manifest-cid "m7" :epoch 7 :expires-at 950})
           (retention/root-node
            {:db-id "db-a" :kind :replication :id "replica"
             :manifest-cid "m5" :epoch 5 :expires-at 1200})
           (retention/root-node
            {:db-id "db-a" :kind :backup :id "backup"
             :manifest-cid "m3" :epoch 3})
           (retention/root-node
            {:db-id "db-b" :kind :legal-hold :id "other"
             :manifest-cid "m1" :epoch 1})]
          entries
          (into {}
                (map-indexed
                 (fn [index root]
                   [(str "oracle/roots/" (get root "db-id") "/"
                         (get root "kind") "/" index)
                    (js/JSON.stringify (clj->js root))]))
                roots)
          bucket
          #js {:list
               (fn [opts]
                 (let [wanted (.-prefix opts)]
                   (js/Promise.resolve
                    #js {:objects
                         (clj->js
                          (mapv (fn [key] #js {:key key})
                                (filter #(str/starts-with? % wanted)
                                        (keys entries))))
                         :truncated false})))
               :get
               (fn [key]
                 (js/Promise.resolve
                  (when-let [value (get entries key)]
                    #js {:etag (str "etag/" key)
                         :text (fn [] (js/Promise.resolve value))})))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "oracle"
                   "MERKLE_RETENTION_CLOCK_SKEW_MS" "100"}]
      (-> (worker/retention-safe-epoch-oracle! env "db-a" 1000)
          (.then
           (fn [oracle]
             (is (= 3 (:safe-epoch oracle)))
             (is (= 900 (:effective-now oracle)))
             (is (= {"backup" 1 "reader" 1 "replication" 1}
                    (:active-by-kind oracle)))
             (worker/retention-safe-epoch! env "db-a" 1000)))
          (.then
           (fn [safe-epoch]
             (is (= 3 safe-epoch)
                 "compaction consumes the same explicit oracle decision")
             (done)))
          (.catch
           (fn [error]
             (is false (str "safe-epoch oracle rejected: " error))
             (done)))))))

(deftest database-backup-restores-head-and-reachable-cid-graph
  (async done
    (let [prefix "database-restore/"
          manifest (lsm/build-manifest {:db-id "source" :epoch 7})
          source-head (:cid manifest)
          backup-receipt (atom nil)
          external-backup-receipt (atom nil)
          restore-ready (atom nil)
          entries
          (atom {(str prefix "heads/source")
                 {:value source-head :etag "etag-1"}
                 (str prefix "blocks/" source-head)
                 {:value (:bytes manifest) :etag "etag-2"}})
          version (atom 2)
          bytes-object
          (fn [value etag]
            #js {:etag etag
                 :text
                 (fn []
                   (js/Promise.resolve
                    (if (string? value)
                      value
                      (.decode (js/TextDecoder.) value))))
                 :arrayBuffer
                 (fn []
                   (let [bytes (if (string? value)
                                 (.encode (js/TextEncoder.) value)
                                 value)]
                     (js/Promise.resolve
                      (.slice (.-buffer bytes)
                              (.-byteOffset bytes)
                              (+ (.-byteOffset bytes)
                                 (.-byteLength bytes))))))})
          bucket
          #js {:get
               (fn [key]
                 (let [{:keys [value etag]} (get @entries key)]
                   (js/Promise.resolve
                    (when value (bytes-object value etag)))))
               :put
               (fn [key value opts]
                 (let [current (get @entries key)
                       condition (some-> opts .-onlyIf)
                       matches (some-> condition .-etagMatches)
                       absent (some-> condition .-etagDoesNotMatch)
                       won? (cond
                              matches (= matches (:etag current))
                              (= "*" absent) (nil? current)
                              :else true)]
                   (if-not won?
                     (js/Promise.resolve nil)
                     (let [etag (str "etag-" (swap! version inc))
                           bytes (cond
                                   (string? value) value
                                   (instance? js/Uint8Array value) value
                                   (instance? js/ArrayBuffer value)
                                   (js/Uint8Array. value)
                                   :else value)]
                       (swap! entries assoc key {:value bytes :etag etag})
                       (js/Promise.resolve #js {:etag etag})))))
               :list
               (fn [opts]
                 (let [wanted (.-prefix opts)]
                   (js/Promise.resolve
                    #js {:objects
                         (clj->js
                          (mapv (fn [key] #js {:key key})
                                (filter #(str/starts-with? % wanted)
                                        (keys @entries))))
                         :truncated false})))
               :delete
               (fn [keys]
                 (doseq [key (js->clj keys)]
                   (swap! entries dissoc key))
                 (js/Promise.resolve nil))}
          env #js {"MERKLE_BUCKET" bucket
                   "MERKLE_S3_PREFIX" "database-restore"}]
      (-> (worker/step-resumable-database-backup!
           env "source" "daily-external" 6900)
          (.then
           (fn [started]
             (is (= :started (:phase started)))
             (worker/step-resumable-database-backup!
              env "source" "daily-external" 6901)))
          (.then
           (fn [traversed]
             (is (= :traversal-progress (:phase traversed)))
             (worker/step-resumable-database-backup!
              env "source" "daily-external" 6902)))
          (.then
           (fn [scanned]
             (is (= :traversal-completed (:phase scanned)))
             (worker/step-resumable-database-backup!
              env "source" "daily-external" 6903)))
          (.then
           (fn [indexed]
             (is (= :inventory-pages-completed (:phase indexed)))
             (worker/step-resumable-database-backup!
              env "source" "daily-external" 6904)))
          (.then
           (fn [completed]
             (is (:completed? completed))
             (is (= :completed (:phase completed)))
             (is (= 1 (:entries completed)))
             (reset! external-backup-receipt completed)
             (worker/step-resumable-database-backup!
              env "source" "daily-external" 6905)))
          (.then
           (fn [replayed]
             (is (:completed? replayed))
             (is (= :terminal (:reason replayed)))
             (worker/backup-database! env "source" "daily-7" 7000)))
          (.then
           (fn [backup]
             (is (= source-head (:head-cid backup)))
             (is (= 7 (:epoch backup)))
             (is (= 1 (:blocks backup)))
             (is (= 1 (:inventory-pages backup)))
             (is (= 1 (:inventory-directory-pages backup)))
             (is (= 1 (:inventory-directory-height backup)))
             (is (= "backup"
                    (get-in backup [:retention-root "kind"])))
             (is (= source-head
                    (get-in backup [:retention-root "manifest-cid"])))
             (reset! backup-receipt backup)
             (worker/backup-database! env "source" "daily-7" 8000)))
          (.then
           (fn [replayed-backup]
             (let [backup @backup-receipt]
               (is (:idempotent? replayed-backup))
               (is (= (:inventory backup)
                      (:inventory replayed-backup))
                   "created time is receipt metadata, not inventory identity")
             (swap! entries dissoc
                    (str prefix "heads/source")
                    (str prefix "blocks/" source-head))
             (-> (worker/prepare-database-restore-task!
                  env (:inventory @external-backup-receipt) "restored")
                 (.then
                  (fn [{:keys [task]}]
                    (worker/claim-database-restore!
                     env {:task task
                          :owner "restore-worker"
                          :token "restore-token"
                          :now-ms 9000
                          :lease-ms 1000})))
                 (.then
                  (fn [claim]
                    (is (:claimed? claim))
                    (worker/run-database-restore-page! env claim)))
                 (.then
                  (fn [advanced]
                    (is (:advanced? advanced))
                    (is (= 1
                           (get-in advanced
                                   [:checkpoint "next-page"])))
                    (is (= 1
                           (get-in advanced
                                   [:checkpoint
                                    "processed-entries"])))
                    (is (contains?
                         @entries
                         (str prefix "blocks/" source-head)))
                    (worker/begin-database-restore-verification!
                     env (assoc advanced
                                :now-ms 9050
                                :lease-ms 1000))))
                 (.then
                  (fn [verifying]
                    (is (:verification-advanced? verifying))
                    (is (= "verifying"
                           (get-in verifying
                                   [:checkpoint "status"])))
                    (worker/advance-database-restore-verification!
                     env (assoc verifying
                                :now-ms 9060
                                :lease-ms 1000))))
                 (.then
                  (fn [processed]
                    (is (:verification-advanced? processed))
                    (is (= source-head
                           (:verification-object processed)))
                    (worker/advance-database-restore-verification!
                     env (assoc processed
                                :now-ms 9070
                                :lease-ms 1000))))
                 (.then
                  (fn [ready]
                    (is (:ready? ready))
                    (is (= "ready-to-publish"
                           (get-in ready
                                   [:checkpoint "status"])))
                    (reset! restore-ready ready)
                    (worker/complete-database-restore!
                     env (assoc ready :etag "forced-pointer-cas-loss"))))
                 (.then
                  (fn [interrupted]
                    (is (false? (:completed? interrupted)))
                    (is (:head-published? interrupted))
                    (worker/claim-database-restore!
                     env {:task (:task @restore-ready)
                          :owner "recovery-worker"
                          :token "recovery-token"
                          :now-ms 11000
                          :lease-ms 1000})))
                 (.then
                  (fn [recovered]
                    (is (:claimed? recovered))
                    (is (:head-published? recovered))
                    (is (= "ready-to-publish"
                           (get-in recovered
                                   [:checkpoint "status"])))
                    (worker/complete-database-restore!
                     env recovered)))
                 (.then
                  (fn [completed]
                    (is (:completed? completed))
                    (is (= "completed"
                           (get-in completed
                                   [:checkpoint "status"])))
                    (is (= source-head
                           (:value
                            (get @entries
                                 (str prefix "heads/restored")))))
                    (worker/step-database-restore!
                     env (:inventory @external-backup-receipt) "restored"
                     {:owner "replay-worker"
                      :token "replay-token"
                      :now-ms 12000
                      :lease-ms 1000})))
                 (.then
                  (fn [terminal]
                    (is (= :completed (:phase terminal)))
                    (swap! entries dissoc
                           (str prefix "heads/restored"))
                    (worker/claim-database-restore!
                     env {:task (:task @restore-ready)
                          :owner "head-loss-recovery-worker"
                          :token "head-loss-recovery-token"
                          :now-ms 13000
                          :lease-ms 1000})))
                 (.then
                  (fn [head-loss-recovery]
                    (is (:claimed? head-loss-recovery))
                    (is (= "running"
                           (get-in head-loss-recovery
                                   [:checkpoint "status"])))
                    (is (= 0
                           (get-in head-loss-recovery
                                   [:checkpoint "next-page"])))
                    (is (= 3
                           (get-in head-loss-recovery
                                   [:checkpoint "attempt"])))
                    (swap! entries dissoc
                           (worker/database-restore-pointer-key
                            env "restored"
                            (str (get-in head-loss-recovery
                                         [:task :cid]))))
                    (worker/restore-database!
                     env (:inventory backup) "restored")))
                 (.then
                  (fn [restored]
                    (is (= 0 (:restored restored)))
                    (is (= 1 (:already-present restored)))
                    (is (:head-created? restored)
                        "head-loss recovery republishes the verified head")
                    (is (false? (:idempotent? restored)))
                    (is (= 1 (:verified-reachable restored)))
                    (is (= source-head
                           (:value
                            (get @entries
                                 (str prefix "heads/restored")))))
                    (worker/resolve-database-head! env "restored")))
                 (.then
                  (fn [resolved]
                    (is (= source-head (:head-cid resolved)))
                    (worker/restore-database!
                     env (:inventory backup) "restored")))
                 (.then
                  (fn [replayed]
                    (is (:idempotent? replayed))
                    (is (= 1 (:already-present replayed)))
                    (swap! entries assoc
                           (str prefix "heads/conflict")
                           {:value "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            :etag "conflict"})
                    (-> (worker/restore-database!
                         env (:inventory backup) "conflict")
                        (.then
                         (fn [_]
                           (is false
                               "a different target head must fence restore")))
                        (.catch
                         (fn [error]
                           (is (str/includes?
                                (str error) "target head exists")))))))))))
          (.then (fn [_] (done)))
          (.catch
           (fn [error]
             (is false (str "database restore rejected: " error
                            "\n" (.-stack error)))
             (done)))))))

(deftest database-backup-inventory-pages-bound-ten-thousand-entries
  (let [entries
        (mapv
         (fn [index]
           {:namespace (if (zero? (mod index 5)) "objects" "blocks")
            :cid (str (ipld/cid (ipld/encode {"inventory-index" index})))})
         (range 10000))
        entries (vec (sort-by (juxt :namespace :cid) entries))
        pages (worker/database-backup-inventory-pages entries)
        tree (worker/database-backup-inventory-tree pages)
        decoded
        (mapcat #(get-in % [:node "entries"]) pages)]
    (is (= 40 (count pages)))
    (is (= 10000 (reduce + (map #(get-in % [:descriptor "count"])
                                pages))))
    (is (every? #(<= (get-in % [:descriptor "count"]) 256) pages))
    (is (every? #(<= (get-in % [:descriptor "encoded-bytes"])
                     worker/database-backup-page-bytes)
                pages))
    (is (= (mapv (fn [{:keys [namespace cid]}]
                   {"namespace" namespace "cid" cid})
                 entries)
           (vec decoded)))
    (is (= (mapv :cid pages)
           (mapv :cid
                 (worker/database-backup-inventory-pages entries)))
        "the same inventory produces the same immutable page CIDs")
    (is (= 1 (:height tree)))
    (is (= 40 (get-in tree [:root "page-count"])))
    (is (= 10000 (get-in tree [:root "entry-count"])))
    (is (= 1 (count (:nodes tree))))
    (is (<= (.-byteLength (:bytes (peek (:nodes tree))))
            worker/database-backup-directory-bytes))))

(deftest database-backup-inventory-tree-root-remains-constant-size
  (let [entry {"namespace" "blocks"
               "cid" (str (ipld/cid (ipld/encode {"tree" "entry"})))}
        page-cid (str (ipld/cid (ipld/encode {"tree" "page"})))
        pages
        (mapv
         (fn [ordinal]
           {:descriptor
            {"cid" page-cid
             "ordinal" ordinal
             "count" 256
             "encoded-bytes" 1024
             "first" entry
             "last" entry}})
         (range 4097))
        tree (worker/database-backup-inventory-tree pages)
        root-node (peek (:nodes tree))
        incremental
        (reduce
         (fn [{:keys [carry nodes]} page]
           (let [next
                 (worker/append-database-backup-inventory-page
                  carry page)]
             {:carry (:carry next)
              :nodes (into nodes (:nodes next))}))
         {:carry [] :nodes []}
         pages)
        finalized
        (worker/finalize-database-backup-inventory-carry
         (:carry incremental))]
    (is (= 3 (:height tree)))
    (is (= 4097 (get-in tree [:root "page-count"])))
    (is (= (* 4097 256) (get-in tree [:root "entry-count"])))
    (is (= 68 (count (:nodes tree))))
    (is (= 2 (count (get-in root-node [:node "children"]))))
    (is (<= (.-byteLength (:bytes root-node))
            worker/database-backup-directory-bytes))
    (is (= (:root tree)
           (:root (worker/database-backup-inventory-tree pages)))
        "the same page descriptors produce the same constant-size root")
    (is (= (:root tree) (:root finalized))
        "bounded incremental carry produces the batch tree root")
    (is (= (set (map :cid (:nodes tree)))
           (set (map :cid
                     (into (:nodes incremental) (:nodes finalized)))))
        "incremental publication effects are byte-identical")))

(deftest gc-fences-active-database-backup-and-sweeps-completed-work
  (async done
    (let [prefix "test/"
          active-prefix
          (str prefix "scheduler/database-backup/db/active/")
          completed-prefix
          (str prefix "scheduler/database-backup/db/completed/")
          active-current (str active-prefix "current")
          active-frontier (str active-prefix "frontier/cid-active")
          completed-current (str completed-prefix "current")
          completed-frontier (str completed-prefix "frontier/cid-done")
          completed-entry (str completed-prefix "entries/blocks/cid-done")
          objects
          (atom
           {active-current
            (js/JSON.stringify
             (clj->js {"format" "kotobase/resumable-database-backup"
                       "version" 1
                       "db-id" "db"
                       "backup-id" "active"
                       "head-cid" "cid-active"
                       "status" "traversing"}))
            active-frontier "active-marker"
            completed-current
            (js/JSON.stringify
             (clj->js {"format" "kotobase/resumable-database-backup"
                       "version" 1
                       "db-id" "db"
                       "backup-id" "completed"
                       "head-cid" "cid-done"
                       "status" "completed"}))
            completed-frontier "done-marker"
            completed-entry ""})
          uploaded-at
          (fn [key]
            (js/Date. (if (str/starts-with? key active-prefix) 9500 0)))
          bucket
          #js {:get
               (fn [key]
                 (js/Promise.resolve
                  (when-let [value (get @objects key)]
                    #js {:etag (str "etag:" key)
                         :text (fn [] (js/Promise.resolve value))})))
               :put
               (fn [key value opts]
                 (let [conditional?
                       (= "*" (some-> opts .-onlyIf .-etagDoesNotMatch))]
                   (if (and conditional? (contains? @objects key))
                     (js/Promise.resolve nil)
                     (do (swap! objects assoc key value)
                         (js/Promise.resolve #js {:key key})))))
               :list
               (fn [opts]
                 (let [wanted (.-prefix opts)
                       listed
                       (->> (keys @objects)
                            (filter #(str/starts-with? % wanted))
                            (mapv
                             (fn [key]
                               #js {:key key
                                    :uploaded (uploaded-at key)})))]
                   (js/Promise.resolve
                    #js {:objects (clj->js listed)
                         :truncated false})))
               :delete
               (fn [keys]
                 (doseq [key (js->clj keys)]
                   (swap! objects dissoc key))
                 (js/Promise.resolve nil))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}]
      (-> (worker/gc-unreachable! env "db" 1000 false 10000)
          (.then
           (fn [audit]
             (is (= 2 (:database-backup-roots audit)))
             (is (= 1 (:active-database-backup-roots audit)))
             (is (= 3 (:stale-database-backup-work audit)))
             (is (= 3 (:candidates audit)))
             (worker/gc-unreachable! env "db" 1000 true 10000)))
          (.then
           (fn [sweep]
             (is (= 3 (:deleted sweep)))
             (is (= 3 (:backed-up sweep)))
             (is (contains? @objects active-current))
             (is (contains? @objects active-frontier))
             (is (nil? (get @objects completed-current)))
             (is (nil? (get @objects completed-frontier)))
             (is (nil? (get @objects completed-entry)))
             (done)))
          (.catch done)))))

(deftest resumable-database-backup-work-gc-bounds-and-resets-pages
  (async done
    (let [prefix "test/"
          work-prefix
          (str prefix "scheduler/database-backup/db/completed/")
          current-key (str work-prefix "current")
          active-prefix
          (str prefix "scheduler/database-backup/db/active/")
          active-current (str active-prefix "current")
          objects
          (atom
           (into
            {current-key
             (js/JSON.stringify
              (clj->js {"format" "kotobase/resumable-database-backup"
                        "version" 1
                        "db-id" "db"
                        "backup-id" "completed"
                        "status" "completed"}))
             active-current
             (js/JSON.stringify
              (clj->js {"format" "kotobase/resumable-database-backup"
                        "version" 1
                        "db-id" "db"
                        "backup-id" "active"
                        "status" "traversing"}))
             (str active-prefix "frontier/live") "live"}
            (map
             (fn [ordinal]
               [(str work-prefix "entries/blocks/"
                     (.padStart (str ordinal) 3 "0"))
                (str "entry-" ordinal)])
             (range 70))))
          versions (atom {})
          stored
          (fn [key value]
            (let [etag (str "v" (get @versions key 0))]
              (if (string? value)
                #js {:etag etag
                     :text (fn [] (js/Promise.resolve value))}
                #js {:etag etag
                     :arrayBuffer
                     (fn []
                       (js/Promise.resolve
                        (.slice (.-buffer value)
                                (.-byteOffset value)
                                (+ (.-byteOffset value)
                                   (.-byteLength value)))))})))
          bucket
          #js {:get
               (fn [key]
                 (js/Promise.resolve
                  (when-let [value (get @objects key)]
                    (stored key value))))
               :put
               (fn [key value opts]
                 (let [current (get @objects key)
                       only-if (some-> opts .-onlyIf)
                       matches (some-> only-if .-etagMatches)
                       absent (some-> only-if .-etagDoesNotMatch)
                       etag (str "v" (get @versions key 0))
                       won? (cond
                              matches (= matches etag)
                              (= "*" absent) (nil? current)
                              :else true)]
                   (if-not won?
                     (js/Promise.resolve nil)
                     (do
                       (swap! objects assoc key value)
                       (swap! versions update key (fnil inc 0))
                       (js/Promise.resolve
                        #js {:key key
                             :etag (str "v" (get @versions key))})))))
               :list
               (fn [opts]
                 (let [wanted (.-prefix opts)
                       limit (or (.-limit opts) 1000)
                       offset (js/parseInt (or (.-cursor opts) "0"))
                       keys (->> (keys @objects)
                                 (filter #(str/starts-with? % wanted))
                                 sort vec)
                       page (subvec keys
                                    (min offset (count keys))
                                    (min (+ offset limit) (count keys)))
                       next-offset (+ offset (count page))
                       truncated? (< next-offset (count keys))]
                   (js/Promise.resolve
                    #js {:objects
                         (clj->js
                          (mapv
                           (fn [key]
                             #js {:key key :uploaded (js/Date. 0)})
                           page))
                         :truncated truncated?
                         :cursor (when truncated? (str next-offset))})))
               :delete
               (fn [keys]
                 (doseq [key (js->clj keys)]
                   (swap! objects dissoc key))
                 (js/Promise.resolve nil))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}
          gc-pointer-key
          (worker/database-backup-work-gc-pointer-key env "gc-1")]
      (-> (worker/step-resumable-database-backup-work-gc!
           env "gc-1" 1000 10000)
          (.then
           (fn [started]
             (is (= :started (:phase started)))
             (let [pointer
                   (js->clj
                    (js/JSON.parse (get @objects gc-pointer-key)))
                   leased
                   (assoc pointer
                          "lease-owner" "other"
                          "lease-token" "other-token"
                          "lease-expires-at" 20000)]
               (swap! objects assoc gc-pointer-key
                      (js/JSON.stringify (clj->js leased)))
               (swap! versions update gc-pointer-key (fnil inc 0)))
             (worker/step-resumable-database-backup-work-gc!
              env "gc-1" 1000 10000)))
          (.then
           (fn [leased]
             (is (= :leased (:phase leased)))
             (is (= :leased (:reason leased)))
             (worker/step-resumable-database-backup-work-gc!
              env "gc-1" 1000 20001)))
          (.then
           (fn [first-page]
             (is (= :deleted-page (:phase first-page)))
             (is (= 61 (:deleted first-page))
                 "terminal and active-prefix objects occupy bounded page slots")
             (worker/step-resumable-database-backup-work-gc!
              env "gc-1" 1000 10000)))
          (.then
           (fn [second-page]
             (is (= :deleted-page (:phase second-page)))
             (is (= 9 (:deleted second-page)))
             (worker/step-resumable-database-backup-work-gc!
              env "gc-1" 1000 10000)))
          (.then
           (fn [completed]
             (is (= :completed (:phase completed)))
             (is (:completed? completed))
             (is (= 70 (get-in completed [:pointer "deleted"])))
             (is (contains? @objects current-key))
             (is (contains? @objects active-current))
             (is (contains? @objects (str active-prefix "frontier/live")))
             (is (empty?
                  (filter #(str/starts-with? %
                                             (str work-prefix "entries/"))
                          (keys @objects))))
             (done)))
          (.catch done)))))

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
          resumable-child-bytes (ipld/encode {"value" "resume"})
          resumable-child (ipld/cid resumable-child-bytes)
          checkpoint-bytes (ipld/encode {"spill" (ipld/link resumable-child)})
          checkpoint (ipld/cid checkpoint-bytes)
          ingress-workload-bytes (ipld/encode {"requests" []})
          ingress-workload (ipld/cid ingress-workload-bytes)
          orphan-bytes (ipld/encode {"orphan" true})
          orphan (ipld/cid orphan-bytes)
          orphan-object-bytes (ipld/encode {"opaque-orphan" true})
          orphan-object (ipld/cid orphan-object-bytes)
          prefix "test/"
          block-key #(str prefix "blocks/" %)
          object-key #(str prefix "objects/" %)
          stale-verification-key
          (str prefix
               "scheduler/database-restore/restored/task/verification/"
               orphan)
          objects (atom {(str prefix "heads/db-a") root-a
                         (str prefix "heads/db-b") root-b
                         (str prefix "scheduler/resumable/db-a/task/current")
                         (js/JSON.stringify
                          (clj->js {"format" "kotobase/resumable-pointer"
                                   "db-id" "db-a"
                                   "expected-head" root-a
                                   "status" "completed"
                                   "checkpoint" (str checkpoint)}))
                         (str prefix "scheduler/ingress/task/current")
                         (js/JSON.stringify
                          (clj->js {"format" "kotobase/resumable-ingress-job"
                                   "status" "queued"
                                   "deadline-at" 9007199254740991
                                   "workload" (str ingress-workload)}))
                         (block-key root-a) root-a-bytes
                         (block-key child-a) child-a-bytes
                         (block-key root-b) root-b-bytes
                         (block-key child-b) child-b-bytes
                         (block-key checkpoint) checkpoint-bytes
                         (block-key resumable-child) resumable-child-bytes
                         (block-key ingress-workload) ingress-workload-bytes
                         (block-key orphan) orphan-bytes
                         (object-key orphan-object) orphan-object-bytes
                         stale-verification-key
                         (js/JSON.stringify
                          (clj->js
                           {"format"
                            "kotobase/database-restore-verification-marker"
                            "version" 1
                            "cid" (str orphan)
                            "status" "done"}))})
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
               :put (fn [key value opts]
                      (let [conditional? (= "*" (some-> opts .-onlyIf
                                                         .-etagDoesNotMatch))]
                        (if (and conditional? (contains? @objects key))
                          (js/Promise.resolve nil)
                          (do
                            (swap! objects assoc key
                                   (if (instance? js/ArrayBuffer value)
                                     (js/Uint8Array. value)
                                     value))
                            (js/Promise.resolve #js {:key key})))))
               :list (fn [opts]
                       (let [wanted (.-prefix opts)
                             listed (->> (keys @objects)
                                         (filter #(.startsWith % wanted))
                                         (mapv (fn [key]
                                                 #js {:key key
                                                      :uploaded (js/Date. 0)})))]
                         (js/Promise.resolve #js {:objects (clj->js listed)
                                                 :truncated false})))
               :delete (fn [keys]
                         (doseq [key (js->clj keys)] (swap! objects dissoc key))
                         (js/Promise.resolve nil))}
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}]
      (-> (worker/gc-unreachable! env "db-a" 0 false)
          (.then (fn [audit]
                   (is (= 2 (:heads audit)))
                   (is (= 1 (:resumable-roots audit)))
                   (is (= 1 (:ingress-roots audit)))
                   (is (= 1 (:active-ingress-roots audit)))
                   (is (= 7 (:reachable audit)))
                   (is (= 3 (:candidates audit)))
                   (is (= 1 (:block-candidates audit)))
                   (is (= 1 (:object-candidates audit)))
                   (is (= 1
                          (:stale-database-restore-verification-markers
                           audit)))
                   (is (= 0 (:deleted audit)))
                   (is (= 1 (:inventory-passes audit)))
                   (worker/gc-unreachable! env "db-a" 0 true)))
          (.then (fn [sweep]
                   (is (= 3 (:deleted sweep)))
                   (is (= 2 (:inventory-passes sweep)))
                   (is (= 3 (:backed-up sweep)))
                   (is (string? (:backup-inventory sweep)))
                   (is (nil? (get @objects (block-key orphan))))
                   (is (nil? (get @objects (object-key orphan-object))))
                   (is (nil? (get @objects stale-verification-key)))
                   (is (every? #(contains? @objects (block-key %))
                               [root-a child-a root-b child-b
                                checkpoint resumable-child ingress-workload])
                       "other heads and resumable checkpoint graphs survive")
                   (-> (worker/restore-gc-inventory!
                        env (:backup-inventory sweep))
                       (.then (fn [restored]
                                (is (= 3 (:restored restored)))
                                (is (= orphan
                                       (ipld/cid
                                        (get @objects (block-key orphan)))))
                                (is (= orphan-object
                                       (ipld/cid
                                        (get @objects
                                             (object-key orphan-object)))))
                                (is (contains? @objects
                                               stale-verification-key))
                                (worker/restore-gc-inventory!
                                 env (:backup-inventory sweep))))
                       (.then (fn [replayed]
                                (is (= 3 (:already-present replayed)))
                                (let [backup-key
                                      (worker/gc-backup-object-key env orphan)
                                      good-backup (get @objects backup-key)]
                                  (swap! objects dissoc (block-key orphan))
                                  (swap! objects assoc backup-key
                                         (ipld/encode {"corrupt" true}))
                                  (-> (worker/restore-gc-inventory!
                                       env (:backup-inventory sweep))
                                      (.then
                                       (fn [_]
                                         (is false
                                             "corrupt backup must not restore")))
                                      (.catch
                                       (fn [error]
                                         (is (str/includes?
                                              (str error) "CID mismatch"))
                                         (is (nil? (get @objects
                                                       (block-key orphan))))
                                         (swap! objects assoc backup-key
                                                good-backup))))))))))
          (.then (fn [_]
                   (swap! objects dissoc
                          (object-key orphan-object)
                          stale-verification-key)
                   (let [second-orphan-bytes (ipld/encode {"orphan" 2})
                         second-orphan (ipld/cid second-orphan-bytes)
                         block-list-calls (atom 0)
                         changing-bucket
                         #js {:get (fn [key] (.get bucket key))
                              :list
                              (fn [opts]
                                (when (= (.-prefix opts) (str prefix "blocks/"))
                                  (when (= 2 (swap! block-list-calls inc))
                                    (swap! objects assoc
                                           (block-key second-orphan)
                                           second-orphan-bytes)))
                                (.list bucket opts))
                              :delete (fn [keys] (.delete bucket keys))}
                         changing-env #js {"MERKLE_BUCKET" changing-bucket
                                           "MERKLE_S3_PREFIX" "test"}]
                     (swap! objects assoc (block-key orphan) orphan-bytes)
                     (-> (worker/gc-unreachable!
                          changing-env "db-a" 0 true)
                         (.then (fn [result]
                                  (assoc result
                                         :second-orphan second-orphan)))))))
          (.then (fn [{:keys [second-orphan] :as changed}]
                   (is (= :inventory-changed (:aborted changed)))
                   (is (= 2 (:inventory-passes changed)))
                   (is (= 0 (:deleted changed)))
                   (is (contains? @objects (block-key orphan)))
                   (is (contains? @objects (block-key second-orphan)))
                   (swap! objects dissoc (block-key orphan)
                          (block-key second-orphan))
                   (swap! objects assoc
                          (str prefix "scheduler/resumable/db-a/task/current")
                          (js/JSON.stringify
                           (clj->js {"format" "kotobase/resumable-pointer"
                                    "db-id" "db-a"
                                    "expected-head" "stale-head"
                                    "status" "completed"
                                    "checkpoint" (str checkpoint)})))
                   (swap! objects assoc
                          (str prefix "scheduler/ingress/task/current")
                          (js/JSON.stringify
                           (clj->js {"format" "kotobase/resumable-ingress-job"
                                    "status" "failed"
                                    "deadline-at" 0
                                    "workload" (str ingress-workload)})))
                   (worker/gc-unreachable! env "db-a" 0 true)))
          (.then (fn [stale-sweep]
                   (is (= 0 (:active-resumable-roots stale-sweep)))
                   (is (= 1 (:stale-resumable-pointers stale-sweep)))
                   (is (= 0 (:active-ingress-roots stale-sweep)))
                   (is (= 1 (:stale-ingress-pointers stale-sweep)))
                   (is (= 3 (:block-candidates stale-sweep)))
                   (is (= 2 (:pointer-candidates stale-sweep)))
                   (is (= 5 (:deleted stale-sweep)))
                   (is (= 2 (:inventory-passes stale-sweep)))
                   (is (nil? (get @objects
                                  (str prefix
                                       "scheduler/resumable/db-a/task/current"))))
                   (is (nil? (get @objects (block-key checkpoint))))
                   (is (nil? (get @objects (block-key resumable-child))))
                   (is (nil? (get @objects (block-key ingress-workload))))
                   (is (nil? (get @objects
                                  (str prefix
                                       "scheduler/ingress/task/current"))))
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
                   (let [root-list-calls (atom 0)
                         concurrent-key (str prefix "roots/db-a/backup/concurrent")
                         concurrent-root
                         (retention/root-node
                          {:db-id "db-a" :kind :backup :id "concurrent"
                           :manifest-cid orphan :epoch 2})
                         fenced-bucket
                         #js {:get (fn [key] (.get bucket key))
                              :list
                              (fn [opts]
                                (when (= (.-prefix opts) (str prefix "roots/"))
                                  (when (= 2 (swap! root-list-calls inc))
                                    (swap! objects assoc concurrent-key
                                           (js/JSON.stringify
                                            (clj->js concurrent-root)))))
                                (.list bucket opts))
                              :delete (fn [keys] (.delete bucket keys))}
                         fenced-env #js {"MERKLE_BUCKET" fenced-bucket
                                         "MERKLE_S3_PREFIX" "test"}]
                     (worker/gc-unreachable! fenced-env "db-a" 0 true))))
          (.then (fn [fenced]
                   (is (= :roots-changed (:aborted fenced)))
                   (is (= 0 (:deleted fenced)))
                   (is (contains? @objects (block-key orphan))
                       "a concurrently registered backup root fences deletion")
                   (swap! objects dissoc
                          (str prefix "roots/db-a/backup/concurrent"))
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

(deftest resumable-execution-spills-resumes-and-fences-stale-workers
  (async done
    (let [prefix "test/"
          entries (atom {(str prefix "heads/db-r")
                         {:value "head-1" :etag "v-head"}})
          version (atom 0)
          bucket
          #js {:get
               (fn [key]
                 (let [{:keys [value etag]} (get @entries key)]
                   (js/Promise.resolve
                    (when value
                      #js {:etag etag
                           :text (fn [] (js/Promise.resolve value))
                           :arrayBuffer
                           (fn []
                             (let [bytes (if (string? value)
                                           (.encode (js/TextEncoder.) value)
                                           value)]
                               (js/Promise.resolve
                                (.slice (.-buffer bytes)
                                        (.-byteOffset bytes)
                                        (+ (.-byteOffset bytes)
                                           (.-byteLength bytes))))))}))))
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
                   (if-not won?
                     (js/Promise.resolve nil)
                     (let [etag (str "v" (swap! version inc))]
                       (swap! entries assoc key {:value value :etag etag})
                       (js/Promise.resolve #js {:etag etag}))))) }
          env #js {"MERKLE_BUCKET" bucket "MERKLE_S3_PREFIX" "test"}
          workload (ipld/cid (ipld/encode {"query" "frontier"}))
          task (resumable/task
                {:kind :join-frontier :db-id "db-r"
                 :expected-head "head-1" :workload-cid workload
                 :max-items 2 :max-bytes 1024})
          claim-opts {:task task :owner "worker-a" :token "token-a"
                      :now-ms 1000 :lease-ms 100}]
      (-> (worker/claim-resumable-execution! env claim-opts)
          (.then
           (fn [claim]
             (is (:claimed? claim))
             (is (= 1 (get-in claim [:pointer "attempt"])))
             (-> (worker/claim-resumable-execution!
                  env (assoc claim-opts :owner "worker-b"
                             :token "token-b" :now-ms 1001))
                 (.then
                  (fn [contender]
                    (is (= :leased (:reason contender)))
                    claim)))))
          (.then
           (fn [claim]
             (worker/advance-resumable-execution!
              env (merge claim
                         {:ordinal 0 :cursor-after {"offset" 2}
                          :payload [{"binding" "alice"}]
                          :item-count 1 :byte-count 24
                          :now-ms 1020 :lease-ms 100}))))
          (.then
           (fn [advanced]
             (is (:advanced? advanced))
             (is (= 1 (get-in advanced [:checkpoint "next-ordinal"])))
             (is (= {"offset" 2} (get-in advanced [:checkpoint "cursor"])))
             (worker/cancel-resumable-execution!
              env "db-r" (str (:cid task)) "operator-request")))
          (.then
           (fn [cancelled]
             (is (:cancelled? cancelled))
             (is (false? (:idempotent? cancelled)))
             (is (= "cancelled"
                    (get-in cancelled [:checkpoint "status"])))
             (is (= {"reason" "operator-request"}
                    (get-in cancelled [:checkpoint "error"])))
             (worker/cancel-resumable-execution!
              env "db-r" (str (:cid task)) "operator-request")))
          (.then
           (fn [cancelled]
             (is (:cancelled? cancelled))
             (is (:idempotent? cancelled))
             (is (some #(str/includes? % "/blocks/") (keys @entries)))
             (worker/claim-resumable-execution!
              env (assoc claim-opts :owner "worker-c"
                         :token "token-c" :now-ms 1200))))
          (.then
           (fn [terminal]
             (is (= :terminal (:reason terminal)))
             (is (= :cancelled (:status terminal)))
             (swap! entries assoc (str prefix "heads/db-r")
                    {:value "head-2" :etag "v-head-2"})
             (worker/claim-resumable-execution!
              env (assoc claim-opts :owner "worker-c"
                         :token "token-c" :now-ms 1200))))
          (.then
           (fn [stale]
             (is (= :stale-head (:reason stale)))
             (done)))
          (.catch
           (fn [error]
             (is false (str "resumable execution rejected: " error "\n"
                            (.-stack error)))
             (done)))))))
