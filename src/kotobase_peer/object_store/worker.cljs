(ns kotobase-peer.object-store.worker
  "S3-backed content-addressed block store. R2 uses its Worker binding; B2 uses
  the S3-compatible HTTP API with SigV4. Mutable head publication stays a
  separate operation because an immutable CAS block and compare-and-swap are
  different consistency contracts."
  (:require [clojure.string :as str]
            [goog.object :as gobj]
            [ipld.core :as ipld]
            [kotobase-peer.atomic-publication :as publication]
            [kotobase-peer.compaction :as compaction]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.resumable-execution :as resumable]
            [kotobase-peer.retention :as retention]
            [kotobase-peer.object-store.s3-sigv4 :as sigv4]))

(defn- env [e k] (gobj/get e k))
(defn- prefix [e]
  (str (str/replace (or (env e "MERKLE_S3_PREFIX") "kotobase/merkle-lsm") #"^/+|/+$" "") "/"))
(defn block-key [e cid] (str (prefix e) "blocks/" cid))
(defn object-key [e cid] (str (prefix e) "objects/" cid))
(defn head-key [e db-id] (str (prefix e) "heads/" db-id))
(defn compaction-lease-key [e db-id]
  (str (prefix e) "scheduler/compaction/" (js/encodeURIComponent db-id) "/lease"))
(defn compaction-checkpoint-key [e db-id task-id token]
  (str (prefix e) "scheduler/compaction/" (js/encodeURIComponent db-id)
       "/checkpoints/" task-id "/" (js/encodeURIComponent token)))
(defn resumable-pointer-key [e db-id task-id]
  (str (prefix e) "scheduler/resumable/" (js/encodeURIComponent db-id)
       "/" task-id "/current"))
(defn retention-root-key [e db-id kind id]
  (str (prefix e) "roots/" (js/encodeURIComponent db-id) "/"
       (name kind) "/" (js/encodeURIComponent id)))

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

(declare get-head cas-head!)

(defn apply-atomic-publication!
  "Persist every immutable block/object in an atomic-publication plan, then
  conditionally publish its final root. No constituent head effect is executed.
  A failed immutable write rejects before the mutable head is touched."
  [e plan]
  (let [effects (vec (:effects plan))
        publication (peek effects)
        immutable (pop effects)]
    (when-not (and (= :head/cas (:effect/type publication))
                   (not-any? #(= :head/cas (:effect/type %)) immutable))
      (throw (ex-info "Atomic publication plan must end in exactly one HeadCAS"
                      {:effects (mapv :effect/type effects)})))
    (-> (apply-view-effects! e immutable)
        (.then (fn [_]
                 (-> (get-head e (:db-id publication))
                     (.then
                      (fn [{current :value :keys [etag]}]
                        (if (not= current (:expected publication))
                          {:published? false :actual current}
                          (-> (cas-head! e (:db-id publication)
                                         (:next publication) etag)
                              (.then
                               (fn [won?]
                                 {:published? won?
                                  :actual (when won? (:next publication))}))))))))))))

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

(defn resolve-database-head!
  "Resolve a legacy VersionManifest head or an EpochPublication head while
  retaining the mutable head identity and ETag needed for safe publication."
  [e db-id]
  (-> (get-head e db-id)
      (.then
       (fn [{head-cid :value :keys [etag]}]
         (if-not head-cid
           {:head-cid nil :base-cid nil :etag etag :publication nil}
           (-> (get-node! e head-cid)
               (.then
                (fn [node]
                  {:head-cid head-cid
                   :base-cid (publication/base-manifest-cid node head-cid)
                   :etag etag
                   :publication (when (publication/publication-node? node)
                                  node)}))))))))

(defn- load-publication-view-bundles!
  [e publication-node]
  (-> (mapv (fn [[view-id descriptor]]
              (-> (get-node! e (ipld/link-cid (get descriptor "bundle")))
                  (.then (fn [node] [view-id node]))))
            (get publication-node "views"))
      clj->js
      js/Promise.all
      (.then #(into {} (array-seq %)))))

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

(defn get-retention-root!
  "Read one mutable retention-root record and its provider ETag."
  [e db-id kind id]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (retention-root-key e db-id kind id))
        (.then
         (fn [object]
           (if object
             (-> (.text object)
                 (.then (fn [value]
                          {:root (retention/validate-node
                                  (js->clj (js/JSON.parse value)))
                           :etag (gobj/get object "etag")})))
             {:root nil :etag nil}))))
    (js/Promise.reject
     (js/Error. "Retention root registry currently requires an R2 binding"))))

(defn cas-retention-root!
  "Create or replace ROOT with R2 ETag CAS. ROOT may be root-node output or
  keyword-keyed options accepted by retention/root-node. A lost renewal or
  release race returns {:won? false}; it never overwrites the winner."
  [e root expected-etag]
  (let [node (if (contains? root "format")
               (retention/validate-node root)
               (retention/root-node root))
        kind (keyword (get node "kind"))
        db-id (get node "db-id")
        id (get node "id")]
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (.put bucket (retention-root-key e db-id kind id)
                (js/JSON.stringify (clj->js node))
                #js {:onlyIf (if expected-etag
                               #js {:etagMatches expected-etag}
                               #js {:etagDoesNotMatch "*"})})
          (.then (fn [result]
                   {:won? (boolean result)
                    :etag (when result (gobj/get result "etag"))
                    :root (when result node)})))
      (js/Promise.reject
       (js/Error. "Retention root registry currently requires an R2 binding")))))

(defn release-retention-root!
  "CAS a root to an inactive tombstone. Conditional replacement is used instead
  of delete so a stale reader cannot erase a concurrent lease renewal."
  [e root expected-etag released-at]
  (cas-retention-root! e (retention/release-node root released-at) expected-etag))

(declare manifest-window! index-run-refs load-runs! all-r2-retention-roots!
         find-entities!)

(defn- collect-index-run-refs!
  [e db-id base-cid index max-depth read-context]
  (letfn [(collect [cid remaining refs]
            (cond
              (nil? cid) (js/Promise.resolve refs)
              (zero? remaining)
              (js/Promise.reject
               (ex-info "Merkle entity scan depth exceeded"
                        (merge {:db-id db-id :max-depth max-depth}
                               read-context)))
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
                                     (lsm/range-directory-refs directory index))))))
                       (collect
                        (some-> (get manifest "previous") ipld/link-cid)
                        (dec remaining)
                        (into refs (index-run-refs [{:node manifest}] index)))))))))]
    (collect base-cid max-depth [])))

(defn- eavt-run-refs! [e db-id base-cid max-depth read-context]
  (collect-index-run-refs! e db-id base-cid :eavt max-depth read-context))

(defn find-latest-entity!
  "Return the MVCC-visible EAVT datom set for one exact entity."
  ([e db-id entity] (find-latest-entity! e db-id entity 256))
  ([e db-id entity max-depth]
   (-> (find-entities! e db-id entity max-depth)
       (.then #(get % entity)))))

(defn find-entities!
  "Return {entity [EAVT rows]} for every entity whose id starts with PREFIX.
  A checkpoint directory replaces the compacted portion of the manifest walk.
  All selected runs are MVCC-merged at the resolved base-manifest epoch before
  tombstones are removed and entities are grouped."
  ([e db-id prefix] (find-entities! e db-id prefix 256))
  ([e db-id prefix max-depth]
   (-> (resolve-database-head! e db-id)
       (.then
        (fn [{:keys [base-cid]}]
          (if-not base-cid
            {}
            (-> (get-node! e base-cid)
                (.then
                 (fn [base-manifest]
                   (let [query-epoch (get base-manifest "epoch")]
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
                                                 (get-in manifest
                                                         ["statistics"
                                                          "range-directory"])]
                                          (-> (get-node! e
                                                         (ipld/link-cid
                                                          directory-link))
                                              (.then
                                               (fn [directory]
                                                 (collect
                                                  (some->
                                                   (get directory "previous")
                                                   ipld/link-cid)
                                                  (dec remaining)
                                                  (into
                                                   refs
                                                   (lsm/range-directory-refs
                                                    directory :eavt))))))
                                          (collect
                                           (some-> (get manifest "previous")
                                                   ipld/link-cid)
                                           (dec remaining)
                                           (into refs
                                                 (index-run-refs
                                                  [{:node manifest}]
                                                  :eavt)))))))))]
                       (-> (collect base-cid max-depth [])
                           (.then
                            (fn [refs]
                              (-> (load-runs!
                                   e (lsm/select-run-refs-by-first-component
                                      refs prefix))
                                  (.then
                                   (fn [runs]
                                     (->> (lsm/visible-rows runs query-epoch)
                                          (filter
                                           (fn [row]
                                             (str/starts-with?
                                              (str (first
                                                    (get row "components")))
                                              prefix)))
                                          (group-by
                                           #(first
                                             (get % "components")))))))))))))))))))))

(defn find-exact-entities!
  "Return MVCC-visible EAVT rows for exact string entity ids. The manifest
  chain is walked once and selected run refs are deduplicated by CID."
  ([e db-id entities] (find-exact-entities! e db-id entities 256))
  ([e db-id entities max-depth]
   (let [entities (set (map str entities))]
     (if (empty? entities)
       (js/Promise.resolve {})
       (-> (resolve-database-head! e db-id)
           (.then
            (fn [{:keys [base-cid]}]
              (if-not base-cid
                {}
                (-> (get-node! e base-cid)
                    (.then
                     (fn [base-manifest]
                       (let [query-epoch (get base-manifest "epoch")]
                         (-> (eavt-run-refs! e db-id base-cid max-depth
                                             {:entities entities})
                             (.then
                              (fn [refs]
                                (let [selected
                                      (->> entities
                                           (mapcat
                                            #(lsm/select-run-refs-by-first-component
                                              refs %))
                                           (reduce
                                            (fn [by-cid ref]
                                              (assoc by-cid
                                                     (str (ipld/link-cid
                                                           (get ref "cid")))
                                                     ref)) {})
                                           vals vec)]
                                  (-> (load-runs! e selected)
                                      (.then
                                       (fn [runs]
                                         (->> (lsm/visible-rows runs query-epoch)
                                              (filter
                                               #(contains?
                                                 entities
                                                 (str (first
                                                       (get % "components")))))
                                              (group-by
                                               #(first
                                                 (get % "components")))))))))))))))))))))))

(defn find-index-prefixes!
  "Return MVCC-visible physical rows matching exact component PREFIXES in one
  index. The manifest chain is walked once and overlapping selected runs are
  loaded once by CID. Prefix first components are strings in the current host
  contract (entity or attribute); unsupported types fail closed."
  ([e db-id index prefixes]
   (find-index-prefixes! e db-id index prefixes 256))
  ([e db-id index prefixes max-depth]
   (let [prefixes (vec (distinct (map vec prefixes)))]
     (when-not (and (lsm/indexes index) (seq prefixes)
                    (every? #(and (seq %) (string? (first %))) prefixes))
       (throw (ex-info "Invalid Merkle index prefix batch"
                       {:index index :prefixes prefixes})))
     (-> (resolve-database-head! e db-id)
         (.then
          (fn [{:keys [base-cid]}]
            (if-not base-cid
              []
              (-> (get-node! e base-cid)
                  (.then
                   (fn [base-manifest]
                     (let [query-epoch (get base-manifest "epoch")]
                       (-> (collect-index-run-refs!
                            e db-id base-cid index max-depth
                            {:index index :prefix-count (count prefixes)})
                           (.then
                            (fn [refs]
                              (let [selected
                                    (->> prefixes
                                         (mapcat
                                          #(lsm/select-run-refs-by-first-component
                                            refs (first %)))
                                         (reduce
                                          (fn [by-cid ref]
                                            (assoc by-cid
                                                   (str (ipld/link-cid
                                                         (get ref "cid")))
                                                   ref)) {})
                                         vals vec)
                                    matches-prefix?
                                    (fn [row]
                                      (let [components (get row "components")]
                                        (some #(= % (subvec components 0
                                                            (min (count %)
                                                                 (count components))))
                                              prefixes)))]
                                (-> (load-runs! e selected)
                                    (.then
                                     (fn [runs]
                                       (->> (lsm/visible-rows runs query-epoch)
                                            (filter matches-prefix?) vec)))))))))))))))))))

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

(defn retention-safe-epoch!
  "Return the minimum epoch imposed by active R2 retention roots, or nil when
  no root constrains compaction. NOW-MS is injectable for deterministic hosts."
  ([e db-id] (retention-safe-epoch! e db-id (js/Date.now)))
  ([e db-id now-ms]
   (if-let [bucket (env e "MERKLE_BUCKET")]
     (-> (all-r2-retention-roots! e bucket)
         (.then (fn [roots]
                  (retention/minimum-safe-epoch
                   (->> roots
                        (mapv :root)
                        (filterv #(= db-id (get % "db-id"))))
                   now-ms))))
     (js/Promise.resolve nil))))

(defn compact-head!
  "Compact the newest manifest window into range-partitioned L1 runs and
  publish it with R2 CAS. The untouched tail remains linked as :previous.
  Returns Promise<boolean>; false means a concurrent writer won the head."
  ([e db-id] (compact-head! e db-id 64 4096))
  ([e db-id window-size] (compact-head! e db-id window-size 4096))
  ([e db-id window-size target-run-rows]
   (-> (retention-safe-epoch! e db-id)
       (.then #(compact-head! e db-id window-size target-run-rows %))))
  ([e db-id window-size target-run-rows root-safe-epoch]
   (-> (resolve-database-head! e db-id)
       (.then
        (fn [{:keys [head-cid base-cid etag publication]}]
          (if-not head-cid
            false
            (-> (manifest-window! e base-cid window-size)
                (.then
                 (fn [{:keys [manifests tail]}]
                   (let [present (filter #(seq (index-run-refs manifests %)) lsm/indexes)
                         epoch (apply max (map #(get-in % [:node "epoch"]) manifests))
                         safe-epoch (if root-safe-epoch
                                      (min epoch root-safe-epoch)
                                      epoch)]
                     (-> (reduce
                          (fn [result index]
                            (.then result
                                   (fn [compacted]
                                     (-> (compact-index-ranges!
                                          e db-id index safe-epoch target-run-rows
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
                                                    :safe-epoch safe-epoch
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
                                         base-effects (concat (:effects directory)
                                                              (:effects manifest))]
                                     (-> (if publication
                                           (load-publication-view-bundles!
                                            e publication)
                                           (js/Promise.resolve nil))
                                         (.then
                                          (fn [view-bundle-nodes]
                                            (let [rebase
                                                  (when publication
                                                    (publication/rebase-plan
                                                     {:db-id db-id
                                                      :expected head-cid
                                                      :publication-node publication
                                                      :base-manifest manifest
                                                      :view-bundle-nodes
                                                      view-bundle-nodes}))
                                                  effects
                                                  (concat base-effects
                                                          (when rebase
                                                            (butlast
                                                             (:effects rebase))))
                                                  next-head
                                                  (if rebase
                                                    (get-in rebase
                                                            [:result :publication])
                                                    (:cid manifest))]
                                              (-> (put-blocks! e effects)
                                                  (.then
                                                   (fn [_]
                                                     (cas-head! e db-id next-head
                                                                etag)))))))))))))))))))))))))

(defn get-compaction-lease!
  "Read and validate the mutable per-database compaction lease."
  [e db-id]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (compaction-lease-key e db-id))
        (.then
         (fn [object]
           (if object
             (-> (.text object)
                 (.then
                  (fn [value]
                    {:lease (compaction/validate-lease-node
                             (js->clj (js/JSON.parse value)))
                     :etag (gobj/get object "etag")})))
             {:lease nil :etag nil}))))
    (js/Promise.reject
     (js/Error. "Compaction scheduler currently requires an R2 binding"))))

(defn compaction-pressure!
  "Inspect a bounded newest-first window. Scheduling is warranted when either
  the manifest backlog reaches MIN-MANIFESTS or an index reaches L0-THRESHOLD."
  [e head-cid min-manifests l0-threshold]
  (when-not (and (pos-int? min-manifests) (pos-int? l0-threshold))
    (throw (ex-info "Compaction pressure thresholds must be positive"
                    {:min-manifests min-manifests :l0-threshold l0-threshold})))
  (-> (get-node! e head-cid)
      (.then
       (fn [head-node]
         (manifest-window! e
                           (publication/base-manifest-cid head-node head-cid)
                           min-manifests)))
      (.then
       (fn [{:keys [manifests tail]}]
         (let [l0-count (reduce
                         +
                         (for [{:keys [node]} manifests
                               index lsm/indexes]
                           (count (get-in node ["indexes" (name index) "l0"]))))
               manifest-count (count manifests)]
           {:needed? (or (>= manifest-count min-manifests)
                         (>= l0-count l0-threshold))
            :manifest-count manifest-count
            :l0-count l0-count
            :tail? (boolean tail)})))))

(defn claim-compaction-lease!
  "Claim one deterministic head task using R2 ETag CAS. An active lease is
  never stolen; an expired/completed lease increments attempt and may be
  reclaimed. TOKEN and NOW-MS are injectable for tests and external schedulers."
  [e {:keys [db-id owner window-size target-run-rows lease-ms now-ms token
             min-manifests l0-threshold]
      :or {window-size 64 target-run-rows 4096 lease-ms 60000
           min-manifests 8 l0-threshold 4}}]
  (let [now-ms (or now-ms (js/Date.now))
        token (or token (str (random-uuid)))]
    (when-not (and (string? db-id) (seq db-id)
                   (string? owner) (seq owner) (pos-int? lease-ms))
      (throw (ex-info "Compaction claim requires db-id, owner, and positive lease-ms"
                      {:db-id db-id :owner owner :lease-ms lease-ms})))
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (get-head e db-id)
          (.then
           (fn [{expected-head :value}]
             (if-not expected-head
               {:claimed? false :reason :no-head}
               (-> (compaction-pressure! e expected-head min-manifests l0-threshold)
                   (.then
                    (fn [{:keys [needed?] :as pressure}]
                      (if-not needed?
                        (assoc pressure :claimed? false :reason :no-pressure)
                        (-> (get-compaction-lease! e db-id)
                            (.then
                             (fn [{current :lease :keys [etag]}]
                               (if (and current
                                        (compaction/lease-active? current now-ms))
                                 {:claimed? false :reason :leased
                                  :owner (get current "owner")
                                  :expires-at (get current "expires-at")}
                                 (let [task (compaction/scheduler-task
                                             {:db-id db-id
                                              :expected-head expected-head
                                              :window-size window-size
                                              :target-run-rows target-run-rows})
                                       lease (compaction/lease-node
                                              {:task task :owner owner :token token
                                               :attempt (inc (or (get current "attempt") 0))
                                               :acquired-at now-ms
                                               :expires-at (+ now-ms lease-ms)})
                                       condition (if etag
                                                   #js {:etagMatches etag}
                                                   #js {:etagDoesNotMatch "*"})]
                                   (-> (.put bucket (compaction-lease-key e db-id)
                                             (js/JSON.stringify (clj->js lease))
                                             #js {:onlyIf condition})
                                       (.then
                                        (fn [result]
                                          (if result
                                            {:claimed? true
                                             :task task
                                             :lease lease
                                             :pressure pressure
                                             :lease-etag (gobj/get result "etag")}
                                            {:claimed? false
                                             :reason :cas-lost})))))))))))))))))
      (js/Promise.reject
       (js/Error. "Compaction scheduler currently requires an R2 binding")))))

(defn renew-compaction-lease!
  "Extend a running lease with ETag fencing. A stale worker receives won? false."
  [e lease etag lease-ms now-ms]
  (when-not (and (pos-int? lease-ms) (integer? now-ms))
    (throw (ex-info "Lease renewal requires positive lease-ms and integer now-ms"
                    {:lease-ms lease-ms :now-ms now-ms})))
  (let [lease (compaction/validate-lease-node lease)
        _ (when-not (= "running" (get lease "status"))
            (throw (ex-info "Only a running compaction lease may be renewed"
                            {:status (get lease "status")})))
        new-expiry (+ now-ms lease-ms)
        _ (when (<= new-expiry (get lease "expires-at"))
            (throw (ex-info "Lease renewal must extend expiry"
                            {:current (get lease "expires-at")
                             :requested new-expiry})))
        renewed (assoc lease "expires-at" new-expiry)]
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (.put bucket (compaction-lease-key e (get lease "db-id"))
                (js/JSON.stringify (clj->js renewed))
                #js {:onlyIf #js {:etagMatches etag}})
          (.then (fn [result]
                   {:won? (boolean result)
                    :lease (when result renewed)
                    :etag (when result (gobj/get result "etag"))})))
      (js/Promise.reject
       (js/Error. "Compaction scheduler currently requires an R2 binding")))))

(defn- finish-compaction-task!
  [e {:keys [task lease lease-etag]} outcome completed-at]
  (let [db-id (get lease "db-id")
        token (get lease "token")
        checkpoint {"format" "kotobase/compaction-checkpoint"
                    "version" 1
                    "task-id" (:id task)
                    "db-id" db-id
                    "expected-head" (get lease "expected-head")
                    "token" token
                    "attempt" (get lease "attempt")
                    "outcome" (name outcome)
                    "completed-at" completed-at}
        terminal (assoc lease "status" (if (= outcome :error) "failed" "completed"))]
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (.put bucket (compaction-checkpoint-key e db-id (:id task) token)
                (js/JSON.stringify (clj->js checkpoint))
                #js {:onlyIf #js {:etagDoesNotMatch "*"}})
          (.then
           (fn [_]
             (-> (.put bucket (compaction-lease-key e db-id)
                       (js/JSON.stringify (clj->js terminal))
                       #js {:onlyIf #js {:etagMatches lease-etag}})
                 (.then (fn [result]
                          {:task-id (:id task) :outcome outcome
                           :checkpoint checkpoint
                           :lease-fenced? (not (boolean result))}))))))
      (js/Promise.reject
       (js/Error. "Compaction scheduler currently requires an R2 binding")))))

(defn run-compaction-once!
  "Claim, execute, checkpoint, and terminally fence one database compaction.
  HeadCAS remains the publication authority; lease loss cannot publish stale
  output, and every attempt writes an immutable token-scoped checkpoint."
  [e opts]
  (-> (claim-compaction-lease! e opts)
      (.then
       (fn [{:keys [claimed? task lease] :as claim}]
         (if-not claimed?
           claim
           (-> (get-head e (get lease "db-id"))
               (.then
                (fn [{current :value}]
                  (if (not= current (get lease "expected-head"))
                    (finish-compaction-task! e claim :stale-head (js/Date.now))
                    (-> (compact-head! e (get lease "db-id")
                                       (get-in task [:node "window-size"])
                                       (get-in task [:node "target-run-rows"]))
                        (.then #(finish-compaction-task!
                                 e claim (if % :published :cas-lost) (js/Date.now)))))))
               (.catch (fn [_]
                         (finish-compaction-task! e claim :error (js/Date.now))))))))))

(defn run-compaction-batch!
  "Run database compactions in deterministic bounded batches."
  [e database-opts max-concurrency]
  (reduce
   (fn [result batch]
     (.then result
            (fn [completed]
              (-> (js/Promise.all
                   (clj->js (mapv #(run-compaction-once! e %) batch)))
                  (.then #(into completed (array-seq %)))))))
   (js/Promise.resolve [])
   (compaction/bounded-batches max-concurrency database-opts)))

(defn get-resumable-pointer!
  "Read the mutable attempt pointer and its immutable checkpoint. The pointer
  is coordination only; checkpoint CID verification remains in get-node!."
  [e db-id task-id]
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (.get bucket (resumable-pointer-key e db-id task-id))
        (.then
         (fn [object]
           (if-not object
             {:pointer nil :checkpoint nil :etag nil}
             (-> (.text object)
                 (.then
                  (fn [value]
                    (let [pointer (js->clj (js/JSON.parse value))]
                      (-> (get-node! e (get pointer "checkpoint"))
                          (.then
                           (fn [checkpoint]
                             {:pointer pointer :checkpoint checkpoint
                              :etag (gobj/get object "etag")})))))))))))
    (js/Promise.reject
     (js/Error. "Resumable execution currently requires an R2 binding"))))

(defn claim-resumable-execution!
  "Claim or reclaim one deterministic snapshot-pinned task. Task and initial
  checkpoint are immutable blocks written before the small R2 ETag-CAS pointer.
  Active attempts are not stolen."
  [e {:keys [task owner token now-ms lease-ms]
      :or {lease-ms 60000}}]
  (let [now-ms (or now-ms (js/Date.now))
        token (or token (str (random-uuid)))
        task-id (str (:cid task))
        db-id (get-in task [:node "db-id"])
        expected-head (get-in task [:node "expected-head"])]
    (when-not (and (:bytes task) (string? owner) (seq owner)
                   (string? token) (seq token) (integer? now-ms)
                   (pos-int? lease-ms))
      (throw (ex-info "Invalid resumable execution claim"
                      {:owner owner :now-ms now-ms :lease-ms lease-ms})))
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (get-head e db-id)
          (.then
           (fn [{current-head :value}]
             (if (not= expected-head current-head)
               {:claimed? false :reason :stale-head
                :expected-head expected-head :current-head current-head}
               (-> (get-resumable-pointer! e db-id task-id)
                   (.then
                    (fn [{current :pointer :keys [etag]}]
                      (if (and current
                               (= "running" (get current "status"))
                               (> (get current "expires-at" 0) now-ms))
                        {:claimed? false :reason :leased
                         :owner (get current "owner")
                         :expires-at (get current "expires-at")}
                        (let [attempt (inc (or (get current "attempt") 0))
                              checkpoint
                              (resumable/initial-checkpoint
                               {:task task :token token :attempt attempt})
                              pointer
                              {"format" "kotobase/resumable-pointer"
                               "version" 1
                               "task-id" task-id
                               "db-id" db-id
                               "expected-head" expected-head
                               "checkpoint" (str (:cid checkpoint))
                               "owner" owner "token" token
                               "attempt" attempt
                               "expires-at" (+ now-ms lease-ms)
                               "status" "running"}
                              condition (if etag
                                          #js {:etagMatches etag}
                                          #js {:etagDoesNotMatch "*"})]
                          (-> (js/Promise.all
                               #js [(put-block! e (:cid task) (:bytes task))
                                    (put-block! e (:cid checkpoint)
                                                (:bytes checkpoint))])
                              (.then
                               (fn [_]
                                 (-> (.put bucket
                                           (resumable-pointer-key e db-id task-id)
                                           (js/JSON.stringify (clj->js pointer))
                                           #js {:onlyIf condition})
                                     (.then
                                      (fn [result]
                                        (if result
                                          {:claimed? true :task task
                                           :pointer pointer
                                           :checkpoint (:node checkpoint)
                                           :etag (gobj/get result "etag")}
                                          {:claimed? false
                                           :reason :cas-lost}))))))))))))))))
      (js/Promise.reject
       (js/Error. "Resumable execution currently requires an R2 binding")))))

(defn advance-resumable-execution!
  "Persist one spill and successor checkpoint, then advance the mutable pointer
  with ETag CAS. A moved database head or pointer fences publication; immutable
  orphan blocks remain safe for GC."
  [e {:keys [task pointer checkpoint etag ordinal cursor-after payload
             item-count byte-count now-ms lease-ms]
      :or {lease-ms 60000}}]
  (let [db-id (get pointer "db-id")
        task-id (get pointer "task-id")
        token (get pointer "token")
        attempt (get pointer "attempt")
        expected-head (get pointer "expected-head")
        advanced (resumable/advance
                  {:task task :checkpoint checkpoint :token token
                   :attempt attempt :ordinal ordinal
                   :cursor-after cursor-after :payload payload
                   :item-count item-count :byte-count byte-count})
        now-ms (or now-ms (js/Date.now))]
    (when-not (and (= task-id (str (:cid task)))
                   (= (get pointer "checkpoint")
                      (str (resumable/checkpoint-cid checkpoint))))
      (throw (ex-info "Resumable advance pointer/checkpoint mismatch"
                      {:task-id task-id
                       :checkpoint (get pointer "checkpoint")})))
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (get-head e db-id)
          (.then
           (fn [{current-head :value}]
             (if (not= expected-head current-head)
               {:advanced? false :reason :stale-head
                :current-head current-head}
               (-> (js/Promise.all
                    #js [(put-block! e (get-in advanced [:spill :cid])
                                     (get-in advanced [:spill :bytes]))
                         (put-block! e (get-in advanced [:checkpoint :cid])
                                     (get-in advanced [:checkpoint :bytes]))])
                   (.then
                    (fn [_]
                      (let [next-pointer
                            (assoc pointer
                                   "checkpoint"
                                   (str (get-in advanced [:checkpoint :cid]))
                                   "expires-at" (+ now-ms lease-ms))]
                        (-> (.put bucket
                                  (resumable-pointer-key e db-id task-id)
                                  (js/JSON.stringify (clj->js next-pointer))
                                  #js {:onlyIf #js {:etagMatches etag}})
                            (.then
                             (fn [result]
                               (if result
                                 {:advanced? true :task task
                                  :pointer next-pointer
                                  :checkpoint
                                  (get-in advanced [:checkpoint :node])
                                  :spill (get-in advanced [:spill :node])
                                  :etag (gobj/get result "etag")}
                                 {:advanced? false
                                  :reason :pointer-fenced}))))))))))))
      (js/Promise.reject
       (js/Error. "Resumable execution currently requires an R2 binding")))))

(defn finish-resumable-execution!
  "Publish a terminal immutable checkpoint and CAS the pointer to terminal.
  Completion is fenced by both the expected database head and pointer ETag."
  [e {:keys [task pointer checkpoint etag status result-cid error]}]
  (let [db-id (get pointer "db-id")
        task-id (get pointer "task-id")
        expected-head (get pointer "expected-head")
        terminal (resumable/finish
                  {:task task :checkpoint checkpoint
                   :token (get pointer "token")
                   :attempt (get pointer "attempt")
                   :status status :result-cid result-cid :error error})]
    (when-not (and (= task-id (str (:cid task)))
                   (= (get pointer "checkpoint")
                      (str (resumable/checkpoint-cid checkpoint))))
      (throw (ex-info "Resumable finish pointer/checkpoint mismatch"
                      {:task-id task-id
                       :checkpoint (get pointer "checkpoint")})))
    (if-let [bucket (env e "MERKLE_BUCKET")]
      (-> (get-head e db-id)
          (.then
           (fn [{current-head :value}]
             (if (not= expected-head current-head)
               {:finished? false :reason :stale-head
                :current-head current-head}
               (-> (put-block! e (:cid terminal) (:bytes terminal))
                   (.then
                    (fn [_]
                      (let [next-pointer
                            (assoc pointer
                                   "checkpoint" (str (:cid terminal))
                                   "status" (name status))]
                        (-> (.put bucket
                                  (resumable-pointer-key e db-id task-id)
                                  (js/JSON.stringify (clj->js next-pointer))
                                  #js {:onlyIf #js {:etagMatches etag}})
                            (.then
                             (fn [result]
                               (if result
                                 {:finished? true :pointer next-pointer
                                  :checkpoint (:node terminal)
                                  :etag (gobj/get result "etag")}
                                 {:finished? false
                                  :reason :pointer-fenced}))))))))))))
      (js/Promise.reject
       (js/Error. "Resumable execution currently requires an R2 binding")))))

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
           (mapv (fn [object]
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

(defn- all-r2-retention-roots! [e bucket]
  (-> (list-r2-blocks! bucket (str (prefix e) "roots/"))
      (.then
       (fn [objects]
         (if (seq objects)
           (js/Promise.all
            (clj->js
             (mapv (fn [object]
                     (let [key (gobj/get object "key")]
                       (-> (.get bucket key)
                           (.then
                            (fn [stored]
                              (when stored
                                (-> (.text stored)
                                    (.then
                                     (fn [value]
                                       {:key key
                                        :etag (gobj/get stored "etag")
                                        :root (retention/validate-node
                                               (js->clj (js/JSON.parse value)))})))))))))
                   objects)))
           (js/Promise.resolve #js []))))
      (.then (fn [roots]
               (->> (array-seq roots)
                    (remove nil?)
                    (sort-by :key)
                    vec)))))

(defn- all-r2-resumable-roots! [e bucket]
  (let [fetch-pointer
        (fn [object]
          (let [key (gobj/get object "key")]
            (-> (.get bucket key)
                (.then
                 (fn [stored]
                   (when stored
                     (-> (.text stored)
                         (.then
                          (fn [value]
                            (let [pointer (js->clj (js/JSON.parse value))]
                              (when-let [checkpoint (get pointer "checkpoint")]
                                {:key key
                                 :etag (gobj/get stored "etag")
                                 :uploaded-ms (some-> (gobj/get object "uploaded")
                                                      .getTime)
                                 :db-id (get pointer "db-id")
                                 :expected-head (get pointer "expected-head")
                                 :status (get pointer "status")
                                 :checkpoint checkpoint})))))))))))]
    (-> (list-r2-blocks! bucket (str (prefix e) "scheduler/resumable/"))
        (.then
         (fn [objects]
           (let [pointers
                 (filterv #(str/ends-with? (gobj/get % "key") "/current")
                          objects)]
             (if (seq pointers)
               (js/Promise.all (clj->js (mapv fetch-pointer pointers)))
               (js/Promise.resolve #js [])))))
        (.then (fn [roots]
                 (->> (array-seq roots)
                      (remove nil?)
                      (sort-by :key)
                      vec))))))

(defn- all-r2-ingress-roots! [e bucket]
  (let [fetch-job
        (fn [object]
          (let [key (gobj/get object "key")]
            (-> (.get bucket key)
                (.then
                 (fn [stored]
                   (when stored
                     (-> (.text stored)
                         (.then
                          (fn [value]
                            (let [job (js->clj (js/JSON.parse value))]
                              (when-let [workload (get job "workload")]
                                {:key key
                                 :etag (gobj/get stored "etag")
                                 :uploaded-ms
                                 (some-> (gobj/get object "uploaded") .getTime)
                                 :status (get job "status")
                                 :deadline-at (get job "deadline-at")
                                 :workload workload})))))))))))]
    (-> (list-r2-blocks! bucket (str (prefix e) "scheduler/ingress/"))
        (.then
         (fn [objects]
           (let [jobs (filterv #(str/ends-with? (gobj/get % "key") "/current")
                               objects)]
             (if (seq jobs)
               (js/Promise.all (clj->js (mapv fetch-job jobs)))
               (js/Promise.resolve #js [])))))
        (.then (fn [roots]
                 (->> (array-seq roots)
                      (remove nil?)
                      (sort-by :key)
                      vec))))))

(defn- gc-root-snapshot! [e bucket]
  (-> (js/Promise.all #js [(all-r2-heads! e bucket)
                           (all-r2-retention-roots! e bucket)
                           (all-r2-resumable-roots! e bucket)
                           (all-r2-ingress-roots! e bucket)])
      (.then (fn [values]
               {:heads (aget values 0)
                :roots (aget values 1)
                :resumable-roots (aget values 2)
                :ingress-roots (aget values 3)}))
      (.catch (fn [error]
                (js/Promise.reject
                 (js/Error. (str "GC root snapshot failed: " error)))))))

(defn- gc-audit! [e bucket {:keys [heads roots resumable-roots ingress-roots]}
                  grace-ms now-ms]
  (let [head-by-key (into {} (map (juxt :key :value)) heads)
        active-resumable-roots
        (filterv (fn [{:keys [db-id expected-head status]}]
                   (and (contains? #{"running" "completed"} status)
                        (= expected-head
                           (get head-by-key (head-key e db-id)))))
                 resumable-roots)
        active-roots (filterv #(retention/active? (:root %) now-ms) roots)
        active-ingress-roots
        (filterv (fn [{:keys [status deadline-at]}]
                   (and (contains? #{"queued" "running"} status)
                        (> (or deadline-at 0) now-ms)))
                 ingress-roots)
        root-cids (concat (map :value heads)
                          (map #(get-in % [:root "manifest-cid"]) active-roots)
                          (map :checkpoint active-resumable-roots)
                          (map :workload active-ingress-roots))
        cutoff (- now-ms grace-ms)
        stale-resumable-pointer-keys
        (->> resumable-roots
             (remove (set active-resumable-roots))
             (filter (fn [{:keys [uploaded-ms]}]
                       (and uploaded-ms (< uploaded-ms cutoff))))
             (mapv :key))
        stale-ingress-pointer-keys
        (->> ingress-roots
             (remove (set active-ingress-roots))
             (filter (fn [{:keys [uploaded-ms]}]
                       (and uploaded-ms (< uploaded-ms cutoff))))
             (mapv :key))]
    (-> (js/Promise.all
         #js [(-> (js/Promise.all
                    (clj->js (map #(reachable-cids! e %) root-cids)))
                   (.then (fn [sets] (into #{} cat (array-seq sets)))))
              (list-r2-blocks! bucket (str (prefix e) "blocks/"))])
        (.then
         (fn [result]
           (let [reachable (aget result 0)
                 candidates
                 (->> (aget result 1)
                      (filter
                       (fn [object]
                         (let [key (gobj/get object "key")
                               cid (last (str/split key #"/"))
                               uploaded (gobj/get object "uploaded")]
                           (and (not (contains? reachable cid))
                                uploaded
                                (< (.getTime uploaded) cutoff)))))
                      (mapv #(gobj/get % "key")))]
             {:reachable (count reachable)
              :heads (count heads)
              :resumable-roots (count resumable-roots)
              :active-resumable-roots (count active-resumable-roots)
              :stale-resumable-pointers (count stale-resumable-pointer-keys)
              :ingress-roots (count ingress-roots)
              :active-ingress-roots (count active-ingress-roots)
              :stale-ingress-pointers (count stale-ingress-pointer-keys)
              :retention-roots (count roots)
              :active-retention-roots (count active-roots)
              :safe-epoch (retention/minimum-safe-epoch (mapv :root roots) now-ms)
              :candidate-keys (into candidates
                                    (concat stale-resumable-pointer-keys
                                            stale-ingress-pointer-keys))
              :block-candidates (count candidates)
              :pointer-candidates (+ (count stale-resumable-pointer-keys)
                                     (count stale-ingress-pointer-keys))
              :candidates (+ (count candidates)
                             (count stale-resumable-pointer-keys)
                             (count stale-ingress-pointer-keys))})))
        (.catch (fn [error]
                  (js/Promise.reject
                   (js/Error. (str "GC mark audit failed: " error))))))))

(defn gc-unreachable!
  "Globally mark from every mutable R2 head, resumable execution checkpoint,
  and every active retention root. A resumable pointer is active only while
  its expected head is still the current head for its database; stale or
  explicitly released pointers are swept after the same grace period and
  two-snapshot fence as unreachable blocks. Queued/running ingress driver
  workloads remain roots through their deadline; failed/expired job pointers
  follow the same fenced sweep. It then optionally sweeps shared
  blocks older than GRACE-MS. Leased reader and
  replication roots expire at NOW-MS; legal-hold and release roots remain until
  CAS-released. A second complete head/root ETag snapshot fences detected
  publication, renewal, and release races. DB-ID remains for source compatibility."
  ([e db-id grace-ms delete?]
   (gc-unreachable! e db-id grace-ms delete? (js/Date.now)))
  ([e _db-id grace-ms delete? now-ms]
   (if-let [bucket (env e "MERKLE_BUCKET")]
     (-> (gc-root-snapshot! e bucket)
         (.then
          (fn [snapshot-before]
            (try
              (-> (gc-audit! e bucket snapshot-before grace-ms now-ms)
                  (.then
                   (fn [{:keys [candidate-keys] :as audit}]
                     (let [result (dissoc audit :candidate-keys)]
                       (if (and delete? (seq candidate-keys))
                         (-> (gc-root-snapshot! e bucket)
                             (.then
                              (fn [snapshot-after]
                                (if (= snapshot-before snapshot-after)
                                  (-> (.delete bucket (clj->js candidate-keys))
                                      (.then #(assoc result :deleted
                                                     (count candidate-keys))))
                                  (assoc result :deleted 0 :aborted :roots-changed)))))
                         (assoc result :deleted 0))))))
              (catch :default error
                (js/Promise.reject
                 (js/Error. (str "GC audit setup failed: " error))))))))
     (js/Promise.reject
      (js/Error. "Reachability GC currently requires an R2 listing binding")))))
