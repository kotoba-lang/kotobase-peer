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
(defn object-key [e cid] (str (prefix e) "objects/" cid))
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

(defn put-object!
  "Idempotently put a packed immutable object. Logical block CIDs and byte
  ranges are described by a separate query bundle."
  [e cid bytes]
  (let [key (object-key e cid)]
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (.put bucket key bytes)
      (if-let [config (b2-config e)]
        (-> (s3-request! config "PUT" key {:body bytes})
            (.then (fn [response]
                     (if (.-ok response)
                       response
                       (js/Promise.reject
                        (ex-info "S3 object PUT failed"
                                 {:cid cid :status (.-status response)}))))))
        (js/Promise.reject
         (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured"))))))

(defn get-object-range!
  "Fetch exactly one byte range from a packed object. Works with R2 bindings
  and generic S3-compatible HTTP; no Cloudflare KV API is involved."
  [e cid offset length]
  (when-not (and (integer? offset) (not (neg? offset))
                 (integer? length) (pos? length))
    (throw (ex-info "Object range must be non-negative and non-empty"
                    {:offset offset :length length})))
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (object-key e cid)
              #js {:range #js {:offset offset :length length}})
        (.then (fn [obj]
                 (if obj
                   (-> (.arrayBuffer obj) (.then #(js/Uint8Array. %)))
                   (js/Promise.reject
                    (ex-info "Packed object not found" {:cid cid}))))))
    (if-let [config (b2-config e)]
      (-> (s3-request! config "GET" (object-key e cid)
                       {:headers {"range" (str "bytes=" offset "-"
                                                (dec (+ offset length)))}})
          (.then (fn [response]
                   (if (contains? #{200 206} (.-status response))
                     (-> (.arrayBuffer response) (.then #(js/Uint8Array. %)))
                     (js/Promise.reject
                      (ex-info "S3 object Range GET failed"
                               {:cid cid :status (.-status response)}))))))
      (js/Promise.reject
       (js/Error. "No MERKLE_BUCKET or MERKLE_S3_* backend configured")))))

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

(defn apply-view-effects!
  "Interpret immutable materialized-view publication effects. Query bundles
  use the normal block namespace; packed SST objects use the object namespace."
  [e effects]
  (->> effects
       (keep (fn [{:keys [effect/type cid bytes]}]
               (case type
                 :block/put (put-block! e cid bytes)
                 :object/put (put-object! e cid bytes)
                 nil)))
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

(defn compare-and-exchange-head!
  "Adapt the asynchronous R2/S3 ETag API to kotobase-peer.core's CAS result
   contract: return NEXT on success; on a lost race, return the actual winning
   head value. EXPECTED/NEXT are CID strings or nil. This performs a fresh head
   read to obtain the provider ETag, never treats a caller-supplied CID as an
   ETag, and re-reads after a conditional-put loss."
  [e db-id expected next]
  (-> (get-head e db-id)
      (.then
       (fn [{current :value :keys [etag]}]
         (if (not= current expected)
           current
           (-> (cas-head! e db-id next etag)
               (.then (fn [won?]
                        (if won?
                          next
                          (-> (get-head e db-id) (.then :value)))))))))))

(declare manifest-window! index-run-refs load-runs!)

(defn find-latest-entity!
  "Walk newest-first manifests and return the first EAVT datom set for entity.
  This is the correctness-oriented cutover reader; compaction/range indexes can
  later replace its bounded manifest walk without changing callers."
  ([e db-id entity] (find-latest-entity! e db-id entity 256))
  ([e db-id entity max-depth]
   (letfn [(scan-refs [refs previous depth]
             (let [refs (lsm/select-run-refs-by-first-component refs entity)
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
                          (scan previous (inc depth)))))))))
           (scan [manifest-cid depth]
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
                      (if-let [directory-link
                               (get-in manifest ["statistics" "range-directory"])]
                        (-> (get-node! e (ipld/link-cid directory-link))
                            (.then
                             (fn [directory]
                               (scan-refs
                                (lsm/range-directory-refs directory :eavt)
                                (some-> (get directory "previous") ipld/link-cid)
                                depth))))
                        (scan-refs
                         (index-run-refs [{:node manifest}] :eavt)
                         (some-> (get manifest "previous") ipld/link-cid)
                         depth)))))))]
     (-> (get-head e db-id)
         (.then (fn [{:keys [value]}] (scan value 0)))))))

(defn find-entities!
  "Return {entity [EAVT rows]} for every entity whose id starts with PREFIX.
  A checkpoint directory replaces the compacted portion of the manifest walk."
  ([e db-id prefix] (find-entities! e db-id prefix 256))
  ([e db-id prefix max-depth]
   (-> (get-head e db-id)
       (.then
        (fn [{head-cid :value}]
          (letfn [(collect [cid remaining refs]
                    (cond
                      (nil? cid) (js/Promise.resolve refs)
                      (zero? remaining)
                      (js/Promise.reject
                       (ex-info "Merkle entity scan depth exceeded"
                                {:db-id db-id :prefix prefix
                                 :max-depth max-depth}))
                      :else
                      (-> (get-node! e cid)
                          (.then
                           (fn [manifest]
                             (if-let [directory-link
                                      (get-in manifest ["statistics" "range-directory"])]
                               (-> (get-node! e (ipld/link-cid directory-link))
                                   (.then
                                    (fn [directory]
                                      (collect
                                       (some-> (get directory "previous") ipld/link-cid)
                                       (dec remaining)
                                       (into refs
                                             (lsm/range-directory-refs
                                              directory :eavt))))))
                               (collect
                                (some-> (get manifest "previous") ipld/link-cid)
                                (dec remaining)
                                (into refs
                                      (index-run-refs [{:node manifest}] :eavt)))))))))]
            (-> (collect head-cid max-depth [])
                (.then
                 (fn [refs]
                   (-> (load-runs!
                        e (lsm/select-run-refs-by-first-component refs prefix))
                       (.then
                        (fn [runs]
                          (->> runs
                               (mapcat #(get-in % [:node "rows"]))
                               (filter (fn [row]
                                         (str/starts-with?
                                          (str (first (get row "components"))) prefix)))
                               (group-by #(first (get % "components"))))))))))))))))

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

(defn- compact-index-ranges!
  "Process disjoint overlap components sequentially. Uploaded run bytes leave
  the live working set before the next range starts; only manifest refs stay."
  [e db-id index safe-epoch target-run-rows refs]
  (reduce
   (fn [result {:keys [refs]}]
     (.then result
            (fn [output-refs]
              (-> (load-runs! e refs)
                  (.then
                   (fn [runs]
                     (let [outputs (lsm/compact-runs-partitioned
                                    index db-id safe-epoch target-run-rows runs)]
                       (-> (put-blocks! e (mapcat :effects outputs))
                           (.then (fn [_]
                                    (into output-refs (map lsm/run-ref) outputs)))))))))))
   (js/Promise.resolve [])
   (lsm/overlapping-run-ranges refs)))

(defn- load-tail-checkpoint! [e tail]
  (if-not tail
    (js/Promise.resolve nil)
    (-> (get-node! e tail)
        (.then
         (fn [manifest]
           (when-let [directory-link
                      (get-in manifest ["statistics" "range-directory"])]
             (-> (get-node! e (ipld/link-cid directory-link))
                 (.then (fn [directory] {:directory directory})))))))))

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
                         epoch (apply max (map #(get-in % [:node "epoch"]) manifests))]
                     (-> (reduce
                          (fn [result index]
                            (.then result
                                   (fn [compacted]
                                     (-> (compact-index-ranges!
                                          e db-id index epoch target-run-rows
                                          (index-run-refs manifests index))
                                         (.then #(assoc compacted index %))))))
                          (js/Promise.resolve {})
                         present)
                         (.then
                          (fn [compacted]
                            (-> (load-tail-checkpoint! e tail)
                                (.then
                                 (fn [inherited]
                                   (let [directory-indexes
                                         (if inherited
                                           (lsm/merge-range-directory-indexes
                                            compacted (:directory inherited))
                                           compacted)
                                         directory-previous
                                         (if inherited
                                           (some-> (get (:directory inherited) "previous")
                                                   ipld/link-cid)
                                           tail)
                                         directory (lsm/build-range-directory
                                                    {:db-id db-id :epoch epoch
                                                     :indexes directory-indexes
                                                     :previous directory-previous})
                                         manifest (lsm/build-manifest
                                                   {:db-id db-id :epoch epoch
                                                    :safe-epoch epoch
                                                    :previous (when-not inherited tail)
                                                    :indexes (into {}
                                                                   (map (fn [[index runs]]
                                                                          [index {:l1 runs}]))
                                                                   compacted)
                                                    :statistics
                                                    {"operation" "window-compaction"
                                                     "range-directory" (ipld/link (:cid directory))
                                                     "inherited-checkpoint" (boolean inherited)
                                                     "manifest-count" (count manifests)
                                                     "target-run-rows" target-run-rows
                                                     "output-run-count"
                                                     (reduce + (map count (vals compacted)))}})
                                         effects (concat (:effects directory)
                                                         (:effects manifest))]
                                     (-> (put-blocks! e effects)
                                         (.then
                                          (fn [_]
                                            (cas-head! e db-id (:cid manifest)
                                                       etag)))))))))))))))))))))

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

(defn- all-r2-heads! [e bucket]
  (-> (list-r2-blocks! bucket (str (prefix e) "heads/"))
      (.then
       (fn [objects]
         (js/Promise.all
          (clj->js
           (map (fn [object]
                  (let [key (gobj/get object "key")]
                    (-> (.get bucket key)
                        (.then (fn [head]
                                 (when head
                                   (-> (.text head)
                                       (.then (fn [value]
                                                {:key key
                                                 :etag (gobj/get head "etag")
                                                 :value value})))))))))
                objects)))))
      (.then (fn [heads]
               (->> (array-seq heads)
                    (remove nil?)
                    (sort-by :key)
                    vec)))))

(defn gc-unreachable!
  "Globally mark from EVERY mutable R2 head and optionally sweep unreachable
  shared blocks older than GRACE-MS. DB-ID is retained for source compatibility
  but deliberately does not scope marking: block keys are globally deduplicated
  under one prefix, so marking only one database head could delete another
  database's live blocks. A second head/ETag snapshot fences detected publish
  races; GRACE-MS protects newly uploaded blocks. Deletion is explicit for
  dry-run-first operation."
  [e _db-id grace-ms delete?]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (all-r2-heads! e bucket)
        (.then
         (fn [heads-before]
           (-> (js/Promise.all
                #js [(-> (js/Promise.all
                           (clj->js (map #(reachable-cids! e (:value %)) heads-before)))
                          (.then (fn [sets] (into #{} cat (array-seq sets)))))
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
                      (-> (all-r2-heads! e bucket)
                          (.then
                           (fn [heads-after]
                             (if (= heads-before heads-after)
                               (-> (.delete bucket (clj->js candidates))
                                   (.then (fn [_] {:reachable (count reachable)
                                                  :heads (count heads-before)
                                                  :candidates (count candidates)
                                                  :deleted (count candidates)})))
                               {:reachable (count reachable)
                                :heads (count heads-before)
                                :candidates (count candidates)
                                :deleted 0
                                :aborted :heads-changed}))))
                      {:reachable (count reachable) :heads (count heads-before)
                       :candidates (count candidates) :deleted 0}))))))))
    (js/Promise.reject
     (js/Error. "Reachability GC currently requires an R2 listing binding"))))
