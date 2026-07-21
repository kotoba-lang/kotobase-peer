(ns kotobase-peer.object-store.worker
  "S3-backed content-addressed block store. R2 uses its Worker binding; B2 uses
  the S3-compatible HTTP API with SigV4. Mutable head publication stays a
  separate operation because an immutable CAS block and compare-and-swap are
  different consistency contracts."
  (:require [clojure.string :as str]
            [goog.object :as gobj]
            [ipld.core :as ipld]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.object-store.s3-sigv4 :as sigv4]))

(defn- env [e k] (gobj/get e k))
(defn- prefix [e]
  (str (str/replace (or (env e "MERKLE_S3_PREFIX") "kotobase/merkle-lsm") #"^/+|/+$" "") "/"))
(defn block-key [e cid] (str (prefix e) "blocks/" cid))
(defn head-key [e db-id] (str (prefix e) "heads/" db-id))

(defn- b2-config [e]
  (when (every? #(seq (env e %))
                ["MERKLE_S3_ENDPOINT" "MERKLE_S3_BUCKET"
                 "MERKLE_S3_ACCESS_KEY_ID" "MERKLE_S3_SECRET_ACCESS_KEY"])
    {:endpoint (env e "MERKLE_S3_ENDPOINT")
     :bucket (env e "MERKLE_S3_BUCKET")
     :region (or (env e "MERKLE_S3_REGION") "us-west-004")
     :access-key (env e "MERKLE_S3_ACCESS_KEY_ID")
     :secret-key (env e "MERKLE_S3_SECRET_ACCESS_KEY")}))

(defn configured? [e]
  (boolean (or (env e "MERKLE_BUCKET") (b2-config e))))

(defn- s3-request! [config method key & [{:keys [body headers]}]]
  (-> (sigv4/signed-headers
       (assoc config :method method :key key :body body :headers headers))
      (.then (fn [{:keys [url headers]}]
               (js/fetch url #js {:method method :headers headers :body body})))))

(defn put-block!
  "Idempotently put CID bytes into R2 or a configured S3-compatible bucket."
  [e cid bytes]
  (let [key (block-key e cid)]
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (.put bucket key bytes)
      (if-let [config (b2-config e)]
        (-> (s3-request! config "PUT" key {:body bytes})
            (.then (fn [response]
                     (if (.-ok response)
                       response
                       (js/Promise.reject
                        (js/Error. (str "S3 block PUT failed: " (.-status response))))))))
        (js/Promise.reject (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured"))))))

(defn get-block!
  "Fetch immutable block bytes from R2 or an S3-compatible backend."
  [e cid]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (block-key e cid))
        (.then (fn [obj]
                 (if obj
                   (-> (.arrayBuffer obj) (.then #(js/Uint8Array. %)))
                   (js/Promise.reject
                    (ex-info "Merkle block not found" {:cid cid}))))))
    (if-let [config (b2-config e)]
      (-> (s3-request! config "GET" (block-key e cid))
          (.then (fn [response]
                   (if (.-ok response)
                     (-> (.arrayBuffer response) (.then #(js/Uint8Array. %)))
                     (js/Promise.reject
                      (ex-info "S3 Merkle block GET failed"
                               {:cid cid :status (.-status response)}))))))
      (js/Promise.reject
       (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured")))))

(defn get-node!
  "Fetch and DAG-CBOR decode an IPLD node."
  [e cid]
  (-> (get-block! e cid) (.then ipld/decode)))

(defn put-blocks!
  "Persist every :block/put effect concurrently. Runs and their manifest are
  immutable and independent; the caller publishes the mutable head only after
  this Promise resolves, preserving publication order without serial RTTs."
  [e effects]
  (->> effects
       (keep (fn [{:keys [effect/type cid bytes]}]
               (when (= :block/put type) (put-block! e cid bytes))))
       clj->js
       js/Promise.all))

(defn get-head
  "Read a mutable head and its ETag for a subsequent conditional PUT."
  [e db-id]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (head-key e db-id))
        (.then (fn [obj]
                 (if obj
                   (-> (.text obj)
                       (.then (fn [value] {:value value :etag (gobj/get obj "etag")})))
                   {:value nil :etag nil}))))
    (if-let [config (b2-config e)]
      (-> (s3-request! config "GET" (head-key e db-id))
          (.then (fn [response]
                   (cond
                     (= 404 (.-status response)) {:value nil :etag nil}
                     (.-ok response)
                     (-> (.text response)
                         (.then (fn [value]
                                  {:value value
                                   :etag (.get (.-headers response) "etag")})))
                     :else
                     (js/Promise.reject
                      (ex-info "S3 Merkle head GET failed"
                               {:db-id db-id :status (.-status response)}))))))
      (js/Promise.reject
       (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured")))))

(defn cas-head!
  "Compare-and-swap the mutable head. R2 uses native conditional puts. S3 uses
  signed If-Match/If-None-Match and must be explicitly enabled after verifying
  that the selected compatible provider implements conditional PutObject."
  [e db-id next etag]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.put bucket (head-key e db-id) next
              #js {:onlyIf (if etag
                             #js {:etagMatches etag}
                             #js {:etagDoesNotMatch "*"})})
        (.then boolean))
    (if-let [config (b2-config e)]
      (if (= "true" (env e "MERKLE_S3_CONDITIONAL_HEAD"))
        (let [condition (if etag {"if-match" etag} {"if-none-match" "*"})]
          (-> (s3-request! config "PUT" (head-key e db-id)
                           {:body next :headers condition})
              (.then (fn [response]
                       (cond
                         (.-ok response) true
                         (contains? #{409 412} (.-status response)) false
                         :else
                         (js/Promise.reject
                          (ex-info "S3 Merkle head conditional PUT failed"
                                   {:db-id db-id :status (.-status response)})))))))
        (js/Promise.reject
         (js/Error. "Set MERKLE_S3_CONDITIONAL_HEAD=true only for a backend with conditional PutObject")))
      (js/Promise.reject
       (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured")))))

(declare manifest-window! index-run-refs load-runs!)

(defn find-latest-entity!
  "Walk newest-first manifests and return the first EAVT datom set for entity.
  This is the correctness-oriented cutover reader; compaction/range indexes can
  later replace its bounded manifest walk without changing callers."
  ([e db-id entity] (find-latest-entity! e db-id entity 256))
  ([e db-id entity max-depth]
   (letfn [(scan [manifest-cid depth]
             (cond
               (nil? manifest-cid) (js/Promise.resolve nil)
               (>= depth max-depth)
               (js/Promise.reject
                (ex-info "Merkle manifest scan depth exceeded"
                         {:db-id db-id :entity entity :max-depth max-depth}))
               :else
               (-> (get-node! e manifest-cid)
                   (.then
                    (fn [manifest]
                      (let [refs (get-in manifest ["indexes" "eavt" "l0"])
                            previous (some-> (get manifest "previous") ipld/link-cid)
                            loads (mapv #(get-node! e (ipld/link-cid (get % "cid"))) refs)]
                        (-> (js/Promise.all (clj->js loads))
                            (.then
                             (fn [runs]
                               (let [rows (->> (array-seq runs)
                                               (mapcat #(get % "rows"))
                                               (filter #(= entity
                                                           (first (get % "components"))))
                                               vec)]
                                 (if (seq rows)
                                   rows
                                   (scan previous (inc depth)))))))))))))]
     (-> (get-head e db-id)
         (.then (fn [{:keys [value]}] (scan value 0)))))))

(defn find-entities!
  "Return {entity [EAVT rows]} for every entity whose id starts with PREFIX.
  Reads each manifest/run at most once. Intended for bounded list/event reads
  until range-directed L1 lookup lands."
  ([e db-id prefix] (find-entities! e db-id prefix 256))
  ([e db-id prefix max-depth]
   (-> (get-head e db-id)
       (.then
        (fn [{head-cid :value}]
          (-> (manifest-window! e head-cid max-depth)
              (.then
               (fn [{:keys [manifests tail]}]
                 (when tail
                   (throw (ex-info "Merkle entity scan depth exceeded"
                                   {:db-id db-id :prefix prefix
                                    :max-depth max-depth})))
                 (-> (load-runs! e (index-run-refs manifests :eavt))
                     (.then
                      (fn [runs]
                        (->> runs
                             (mapcat #(get-in % [:node "rows"]))
                             (filter (fn [row]
                                       (str/starts-with?
                                        (str (first (get row "components"))) prefix)))
                             (group-by #(first (get % "components")))))))))))))))

(defn- manifest-window!
  [e head-cid limit]
  (letfn [(step [cid remaining acc]
            (if (or (nil? cid) (zero? remaining))
              (js/Promise.resolve
               {:manifests acc :tail cid})
              (-> (get-node! e cid)
                  (.then (fn [node]
                           (step (some-> (get node "previous") ipld/link-cid)
                                 (dec remaining)
                                 (conj acc {:cid cid :node node})))))))]
    (step head-cid limit [])))

(defn- index-run-refs [manifests index]
  (mapcat (fn [{:keys [node]}]
            (mapcat val (get-in node ["indexes" (name index)])))
          manifests))

(defn- load-runs! [e refs]
  (-> (mapv (fn [ref]
              (-> (get-node! e (ipld/link-cid (get ref "cid")))
                  (.then (fn [node] {:node node}))))
            refs)
      clj->js
      js/Promise.all
      (.then #(vec (array-seq %)))))

(defn compact-head!
  "Compact the newest manifest window into range-partitioned L1 runs and
  publish it with R2 CAS. The untouched tail remains linked as :previous.
  Returns Promise<boolean>; false means a concurrent writer won the head."
  ([e db-id] (compact-head! e db-id 64 4096))
  ([e db-id window-size] (compact-head! e db-id window-size 4096))
  ([e db-id window-size target-run-rows]
   (-> (get-head e db-id)
       (.then
        (fn [{head-cid :value :keys [etag]}]
          (if-not head-cid
            false
            (-> (manifest-window! e head-cid window-size)
                (.then
                 (fn [{:keys [manifests tail]}]
                   (let [present (filter #(seq (index-run-refs manifests %)) lsm/indexes)
                         loads (mapv #(load-runs! e (index-run-refs manifests %)) present)]
                     (-> (js/Promise.all (clj->js loads))
                         (.then
                          (fn [loaded]
                            (let [epoch (apply max (map #(get-in % [:node "epoch"]) manifests))
                                  compacted (into {}
                                                  (map (fn [index runs]
                                                         [index (lsm/compact-runs-partitioned
                                                                 index db-id epoch
                                                                 target-run-rows runs)])
                                                       present (array-seq loaded)))
                                  manifest (lsm/build-manifest
                                            {:db-id db-id :epoch epoch :safe-epoch epoch
                                             :previous tail
                                             :indexes (into {}
                                                            (map (fn [[index runs]]
                                                                   [index {:l1 runs}]))
                                                            compacted)
                                             :statistics {"operation" "window-compaction"
                                                          "manifest-count" (count manifests)
                                                          "target-run-rows" target-run-rows
                                                          "output-run-count" (reduce + (map count (vals compacted)))}})
                                  effects (concat (mapcat :effects (mapcat identity (vals compacted)))
                                                  (:effects manifest))]
                              (-> (put-blocks! e effects)
                                  (.then (fn [_]
                                           (cas-head! e db-id (:cid manifest) etag))))))))))))))))))

(defn reachable-cids!
  "Walk decoded IPLD links from ROOT-CID and return a Promise<set<CID>>."
  [e root-cid]
  (let [seen (atom #{})]
    (letfn [(walk [frontier]
              (let [fresh (vec (remove @seen frontier))]
                (if (empty? fresh)
                  (js/Promise.resolve @seen)
                  (do
                    (swap! seen into fresh)
                    (-> (js/Promise.all
                         (clj->js
                          (map #(-> (get-node! e %)
                                    (.then lsm/linked-cids))
                               fresh)))
                        (.then (fn [links]
                                 (walk (into #{} cat (js->clj links))))))))))]
      (if root-cid (walk [root-cid]) (js/Promise.resolve #{})))))

(defn- list-r2-blocks! [bucket prefix]
  (letfn [(page [cursor acc]
            (-> (.list bucket
                       (clj->js (cond-> {:prefix prefix :limit 1000}
                                  cursor (assoc :cursor cursor))))
                (.then
                 (fn [result]
                   (let [objects (into acc (array-seq (gobj/get result "objects")))
                         truncated? (gobj/get result "truncated")]
                     (if truncated?
                       (page (gobj/get result "cursor") objects)
                       objects))))))]
    (page nil [])))

(defn gc-unreachable!
  "Mark from the current head and optionally sweep unreachable R2 blocks older
  than GRACE-MS. Deletion is explicit so callers can run a dry audit first."
  [e db-id grace-ms delete?]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (get-head e db-id)
        (.then
         (fn [{:keys [value]}]
           (-> (js/Promise.all
                #js [(reachable-cids! e value)
                     (list-r2-blocks! bucket (str (prefix e) "blocks/"))])
               (.then
                (fn [result]
                  (let [reachable (aget result 0)
                        objects (aget result 1)
                        cutoff (- (js/Date.now) grace-ms)
                        candidates
                        (->> objects
                             (filter
                              (fn [object]
                                (let [key (gobj/get object "key")
                                      cid (last (str/split key #"/"))
                                      uploaded (gobj/get object "uploaded")]
                                  (and (not (contains? reachable cid))
                                       uploaded
                                       (< (.getTime uploaded) cutoff)))))
                             (mapv #(gobj/get % "key")))]
                    (if (and delete? (seq candidates))
                      (-> (.delete bucket (clj->js candidates))
                          (.then (fn [_] {:reachable (count reachable)
                                         :candidates (count candidates)
                                         :deleted (count candidates)})))
                      {:reachable (count reachable)
                       :candidates (count candidates) :deleted 0}))))))))
    (js/Promise.reject
     (js/Error. "Reachability GC currently requires an R2 listing binding"))))
