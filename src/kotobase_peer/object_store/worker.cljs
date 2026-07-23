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
(defn retention-clock-skew-ms [e]
  (let [raw (env e "MERKLE_RETENTION_CLOCK_SKEW_MS")]
    (if (nil? raw)
      30000
      (let [text (str raw)
            parsed (js/Number text)]
        (when-not (and (re-matches #"[0-9]+" text)
                       (js/Number.isSafeInteger parsed))
          (throw (ex-info "MERKLE_RETENTION_CLOCK_SKEW_MS must be a non-negative safe integer"
                          {:value raw})))
        parsed))))
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
(defn gc-backup-object-key [e cid]
  (str (prefix e) "gc-backups/objects/" cid))
(defn gc-inventory-key [e cid]
  (str (prefix e) "gc-backups/inventories/" cid))
(defn database-backup-inventory-key [e cid]
  (str (prefix e) "database-backups/inventories/" cid))
(defn database-backup-page-key [e cid]
  (str (prefix e) "database-backups/pages/" cid))

(def database-backup-page-entries 256)
(def database-backup-page-bytes (* 64 1024))

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

(defn resolve-database-snapshot!
  "Resolve one caller-pinned immutable head without consulting the mutable
  head key. HEAD-CID may identify either a legacy VersionManifest or an
  EpochPublication."
  [e head-cid]
  (when-not (and (string? head-cid) (seq head-cid))
    (throw (ex-info "Pinned database head CID must be non-empty"
                    {:head-cid head-cid})))
  (-> (get-node! e head-cid)
      (.then
       (fn [node]
         {:head-cid head-cid
          :base-cid (publication/base-manifest-cid node head-cid)
          :etag nil
          :publication (when (publication/publication-node? node) node)}))))

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

(declare manifest-window! index-run-refs load-run! load-runs! all-r2-retention-roots!
         find-entities!)

(defn- validated-run-block-rows [descriptor block expected-index expected-tenant]
  (let [rows (get block "rows")
        first-row (first rows)
        last-row (peek rows)]
    (when-not
     (and (= "kotobase/merkle-run-block" (get block "format"))
          (= lsm/format-version (get block "version"))
          (= (get descriptor "ordinal") (get block "ordinal"))
          (= (get descriptor "count") (count rows) (get block "count"))
          (= (get descriptor "min-key") (get block "min-key")
             (get first-row "key"))
          (= (get descriptor "max-key") (get block "max-key")
             (get last-row "key"))
          (= (get descriptor "logical-min") (get block "logical-min")
             (lsm/row-logical-key first-row))
          (= (get descriptor "logical-max") (get block "logical-max")
             (lsm/row-logical-key last-row))
          (or (nil? (get descriptor "encoded-bytes"))
              (= (get descriptor "encoded-bytes")
                 (.-byteLength (ipld/encode block))))
          (or (nil? expected-index)
              (= (name expected-index) (get block "index")))
          (or (nil? expected-tenant)
              (= (str expected-tenant) (get block "tenant"))))
      (throw (ex-info "Malformed Merkle run data block"
                      {:descriptor descriptor
                       :index expected-index :tenant expected-tenant})))
    rows))

(defn- valid-block-remainder? [remainder]
  (and (vector? remainder)
       (every? (fn [entry]
                 (and (map? entry)
                      (string? (:cid entry))
                      (seq (:cid entry))
                      (map? (:block entry))))
               remainder)))

(defn- bounded-block-remainder [loaded cursor max-bytes]
  (if (or (nil? cursor) (not (pos-int? max-bytes)))
    []
    (reduce
     (fn [entries {:keys [descriptor] :as entry}]
       (if (pos? (compare (get descriptor "logical-max") cursor))
         (let [candidate (conj entries (select-keys entry [:cid :block]))]
           (if (<= (.-byteLength (ipld/encode candidate)) max-bytes)
             candidate
             entries))
         entries))
     []
     (sort-by (juxt #(get-in % [:descriptor "logical-min"]) :cid)
              (vals loaded)))))

(defn- load-directory-pages!
  [e db-id directory index descriptors]
  (let [load-page
          (fn [descriptor]
            (-> (get-node! e (ipld/link-cid (get descriptor "cid")))
                (.then
                 (fn [page]
                   (get
                    (lsm/validate-range-directory-page
                     page db-id (get directory "epoch") index
                     (get directory "page-refs")
                     (get directory "page-bytes"))
                    "refs")))))]
    ;; Keep provider concurrency bounded even when a 10M directory has
    ;; hundreds of leaves.
    (reduce
     (fn [result wave]
       (.then result
              (fn [refs]
                (-> (js/Promise.all (clj->js (mapv load-page wave)))
                    (.then #(into refs (mapcat identity (array-seq %))))))))
     (js/Promise.resolve [])
     (partition-all 4 descriptors))))

(defn- load-directory-index-refs!
  [e db-id directory index first-components]
  (if-not (lsm/paged-range-directory? directory)
    (js/Promise.resolve (lsm/range-directory-refs directory index))
    (let [all (lsm/range-directory-page-descriptors directory index)
          selected
          (if (seq first-components)
            (->> first-components
                 (mapcat #(lsm/select-run-refs-by-first-component all %))
                 (reduce (fn [by-cid descriptor]
                           (assoc by-cid
                                  (str (ipld/link-cid (get descriptor "cid")))
                                  descriptor))
                         {})
                 vals vec)
            (vec all))]
      (load-directory-pages! e db-id directory index selected))))

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
                              (let [directory
                                    (lsm/validate-range-directory
                                     directory db-id (get manifest "epoch"))]
                                (-> (load-directory-index-refs!
                                     e db-id directory index
                                     (or (:first-components read-context)
                                         (:entities read-context)))
                                    (.then
                                     (fn [directory-refs]
                                       (collect
                                        (some-> (get directory "previous")
                                                ipld/link-cid)
                                        (dec remaining)
                                        (into refs directory-refs)))))))))
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
                                                 (let [directory
                                                       (lsm/validate-range-directory
                                                        directory db-id
                                                        (get manifest "epoch"))]
                                                   (->
                                                    (load-directory-index-refs!
                                                     e db-id directory :eavt
                                                     (when (seq prefix) [prefix]))
                                                    (.then
                                                     (fn [directory-refs]
                                                       (collect
                                                        (some->
                                                         (get directory "previous")
                                                         ipld/link-cid)
                                                        (dec remaining)
                                                        (into refs
                                                              directory-refs)))))))))
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
                            {:index index :prefix-count (count prefixes)
                             :first-components (mapv first prefixes)})
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

(defn find-index-prefix-page!
  "Bounded-memory MVCC prefix page. Selected run blocks are fetched and folded
  on demand. A continuation never speculatively fetches its successor block;
  only LIMIT+1 logical-key candidates survive between reads.
  AFTER is the opaque logical-key cursor returned by the previous page."
  [e db-id index prefixes
   {:keys [after limit max-depth head-cid block-remainder remainder-max-bytes
           block-get-concurrency block-get-max-wave-bytes]
    :or {limit 256 max-depth 256 block-remainder [] remainder-max-bytes 0
         block-get-concurrency 4 block-get-max-wave-bytes 4194304}}]
  (let [prefixes (vec (distinct (map vec prefixes)))]
    (when-not (and (lsm/indexes index) (seq prefixes) (pos-int? limit)
                   (pos-int? max-depth)
                   (pos-int? block-get-concurrency)
                   (pos-int? block-get-max-wave-bytes)
                   (nat-int? remainder-max-bytes)
                   (valid-block-remainder? block-remainder)
                   (or (zero? remainder-max-bytes)
                       (<= (.-byteLength (ipld/encode block-remainder))
                           remainder-max-bytes))
                   (or (nil? after) (string? after))
                   (or (nil? head-cid)
                       (and (string? head-cid) (seq head-cid)))
                   (every? #(and (seq %) (string? (first %))) prefixes))
      (throw (ex-info "Invalid bounded Merkle index prefix page"
                      {:index index :prefixes prefixes :after after
                       :limit limit :max-depth max-depth
                       :head-cid head-cid
                       :block-get-concurrency block-get-concurrency
                       :block-get-max-wave-bytes block-get-max-wave-bytes
                       :remainder-max-bytes remainder-max-bytes})))
    (-> (if head-cid
          (resolve-database-snapshot! e head-cid)
          (resolve-database-head! e db-id))
        (.then
         (fn [{:keys [base-cid]}]
           (if-not base-cid
             {:rows [] :cursor nil :done? true :scanned-runs 0}
             (-> (get-node! e base-cid)
                 (.then
                  (fn [base-manifest]
                    (let [query-epoch (get base-manifest "epoch")]
                      (-> (collect-index-run-refs!
                           e db-id base-cid index max-depth
                           {:index index :prefix-count (count prefixes)})
                          (.then
                           (fn [refs]
                             (let [remainder-by-cid
                                   (into {} (map (juxt :cid :block))
                                         block-remainder)
                                   selected
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
                                        (sort-by key)
                                        (mapv val))
                                   matches-prefix?
                                   (fn [row]
                                     (let [components (get row "components")]
                                       (some #(= % (subvec components 0
                                                           (min (count %)
                                                                (count components))))
                                             prefixes)))]
                               (letfn [(cutoff [candidates]
                                         (when (> (count candidates) limit)
                                           (nth (sort (keys candidates)) limit)))
                                       (skip-block? [candidates descriptor]
                                         (let [lo (get descriptor "logical-min")
                                               hi (get descriptor "logical-max")
                                               page-cutoff (cutoff candidates)]
                                           (or (and after hi
                                                    (not (pos? (compare hi after))))
                                               (and page-cutoff lo
                                                    (pos? (compare lo page-cutoff))))))
                                       (load-block! [descriptor]
                                         (let [cid (str (ipld/link-cid
                                                         (get descriptor "cid")))]
                                           (if-let [block (get remainder-by-cid cid)]
                                             (do
                                               (when-not (= cid (str (ipld/cid
                                                                     (ipld/encode block))))
                                                 (throw
                                                  (ex-info
                                                   "Merkle block remainder CID mismatch"
                                                   {:cid cid})))
                                               (js/Promise.resolve
                                                [descriptor block false cid]))
                                             (-> (get-node! e cid)
                                                 (.then #(vector descriptor % true cid))))))
                                       (fold-loaded-block
                                         [state [descriptor block fetched? cid]]
                                         (let [rows (validated-run-block-rows
                                                     descriptor block index db-id)
                                               candidates (:candidates state)]
                                           (cond-> (-> state
                                                       (update :scanned-blocks inc)
                                                       (assoc-in [:loaded-blocks cid]
                                                                 {:cid cid :block block
                                                                  :descriptor descriptor}))
                                             fetched? (update :fetched-blocks inc)
                                             (and fetched?
                                                  (pos-int?
                                                   (get descriptor
                                                        "encoded-bytes")))
                                             (update :fetched-block-bytes +
                                                     (get descriptor
                                                          "encoded-bytes"))
                                             (and fetched?
                                                  (not (pos-int?
                                                        (get descriptor
                                                             "encoded-bytes"))))
                                             (update
                                              :unknown-fetched-block-bytes inc)
                                             (not (skip-block? candidates descriptor))
                                             (assoc :candidates
                                                    (lsm/visible-page-add-run
                                                     candidates rows query-epoch after
                                                     limit matches-prefix?)))))
                                       (take-wave [eligible]
                                         (reduce
                                          (fn [planned candidate]
                                            (let [estimate
                                                  (or (get-in candidate
                                                              [:descriptor
                                                               "encoded-bytes"])
                                                      block-get-max-wave-bytes)
                                                  next-bytes
                                                  (+ (reduce
                                                      + 0
                                                      (map
                                                       #(or
                                                         (get-in
                                                          % [:descriptor
                                                             "encoded-bytes"])
                                                         block-get-max-wave-bytes)
                                                       planned))
                                                     estimate)]
                                              (if (or
                                                   (>= (count planned)
                                                       block-get-concurrency)
                                                   (and (seq planned)
                                                        (> next-bytes
                                                           block-get-max-wave-bytes)))
                                                (reduced planned)
                                                (conj planned candidate))))
                                          [] eligible))
                                       (fold-block-waves [state queues]
                                         (let [eligible
                                               (->> queues
                                                    (keep-indexed
                                                     (fn [queue-index descriptors]
                                                       (when-let [remaining
                                                                  (seq
                                                                   (drop-while
                                                                    #(skip-block?
                                                                      (:candidates state) %)
                                                                    descriptors))]
                                                         {:queue-index queue-index
                                                          :descriptor (first remaining)
                                                          :remaining (vec (rest remaining))})))
                                                    (sort-by (fn [{:keys [queue-index descriptor]}]
                                                               [(get descriptor "logical-min")
                                                                queue-index])))
                                               planned (vec (take-wave eligible))]
                                           (if (empty? planned)
                                             (js/Promise.resolve state)
                                             (-> (mapv #(load-block! (:descriptor %))
                                                       planned)
                                                 clj->js js/Promise.all
                                                 (.then
                                                  (fn [loaded]
                                                    (let [loaded (vec (array-seq loaded))
                                                          next-queues
                                                          (reduce
                                                           (fn [current
                                                                {:keys [queue-index remaining]}]
                                                             (assoc current queue-index remaining))
                                                           queues planned)
                                                          next-state
                                                          (-> (reduce fold-loaded-block state loaded)
                                                              (update
                                                               :max-concurrent-block-gets max
                                                               (count (filter #(nth % 2) loaded)))
                                                              (update
                                                               :max-wave-block-bytes max
                                                               (reduce
                                                                + 0
                                                                (keep
                                                                 (fn [[descriptor _ fetched?]]
                                                                   (when fetched?
                                                                     (get descriptor
                                                                          "encoded-bytes")))
                                                                 loaded))))]
                                                      (fold-block-waves
                                                       next-state next-queues))))))))
                                       (fold-legacy-ref [pending ref]
                                         (.then
                                          pending
                                          (fn [state]
                                            (-> (load-run! e ref)
                                                (.then
                                                 (fn [run]
                                                   (cond->
                                                    (-> state
                                                        (assoc :candidates
                                                               (lsm/visible-page-add-run
                                                                (:candidates state)
                                                                (:rows run)
                                                                query-epoch after limit
                                                                matches-prefix?))
                                                        (update :scanned-runs inc)
                                                        (update :scanned-blocks inc)
                                                        (update :fetched-blocks inc))
                                                     (pos-int?
                                                      (get ref "encoded-bytes"))
                                                     (update
                                                      :fetched-block-bytes +
                                                      (get ref "encoded-bytes"))
                                                     (pos-int?
                                                      (get ref "encoded-bytes"))
                                                     (update
                                                      :max-wave-block-bytes max
                                                      (get ref "encoded-bytes"))
                                                     (not
                                                      (pos-int?
                                                       (get ref "encoded-bytes")))
                                                     (update
                                                      :unknown-fetched-block-bytes
                                                      inc))))))))]
                                 (let [block-refs (filterv #(seq (get % "blocks")) selected)
                                       legacy-refs (filterv #(not (seq (get % "blocks"))) selected)
                                       initial {:candidates {} :scanned-runs 0
                                                :scanned-blocks 0 :fetched-blocks 0
                                                :fetched-block-bytes 0
                                                :unknown-fetched-block-bytes 0
                                                :max-concurrent-block-gets 0
                                                :max-wave-block-bytes 0
                                                :loaded-blocks {}}]
                                   (-> (reduce fold-legacy-ref
                                               (js/Promise.resolve initial)
                                               legacy-refs)
                                     (.then
                                      (fn [state]
                                        (fold-block-waves
                                         (update state :scanned-runs + (count block-refs))
                                         (mapv #(vec (sort-by (fn [descriptor]
                                                               (get descriptor "ordinal"))
                                                             (get % "blocks")))
                                               block-refs))))
                                     (.then
                                      (fn [state]
                                        (let [result (lsm/visible-page-result
                                                      (:candidates state) limit)]
                                          (assoc result
                                                 :scanned-runs (:scanned-runs state)
                                                 :scanned-blocks (:scanned-blocks state)
                                                 :fetched-blocks (:fetched-blocks state)
                                                 :fetched-block-bytes
                                                 (:fetched-block-bytes state)
                                                 :unknown-fetched-block-bytes
                                                 (:unknown-fetched-block-bytes state)
                                                 :max-concurrent-block-gets
                                                 (:max-concurrent-block-gets state)
                                                 :max-wave-block-bytes
                                                 (:max-wave-block-bytes state)
                                                 :block-remainder
                                                 (bounded-block-remainder
                                                  (:loaded-blocks state)
                                                  (:cursor result)
                                                  remainder-max-bytes))))))))))))))))))))))

(defn- manifest-window!
  [e head-cid limit]
  (letfn [(step [cid remaining acc]
            (if (or (nil? cid) (zero? remaining))
              (js/Promise.resolve
               {:manifests acc :tail cid})
              (-> (get-node! e cid)
                  (.then (fn [node]
                           ;; A range directory replaces the manifest indexes
                           ;; for the checkpointed prefix. Keep that manifest as
                           ;; an atomic boundary; reading only its own indexes
                           ;; would silently omit refs inherited by directory.
                           (if (get-in node ["statistics" "range-directory"])
                             {:manifests acc :tail cid}
                             (step (some-> (get node "previous") ipld/link-cid)
                                   (dec remaining)
                                   (conj acc {:cid cid :node node}))))))))]
    (step head-cid limit [])))

(defn- index-run-refs [manifests index]
  (mapcat (fn [{:keys [node]}]
            (mapcat val (get-in node ["indexes" (name index)])))
          manifests))

(defn- load-run! [e ref]
  (-> (get-node! e (ipld/link-cid (get ref "cid")))
      (.then
       (fn [node]
         (if-let [blocks (seq (get node "blocks"))]
           (-> (mapv #(get-node! e (ipld/link-cid (get % "cid")))
                     (sort-by #(get % "ordinal") blocks))
               clj->js
               js/Promise.all
               (.then (fn [loaded]
                        (let [rows
                              (vec
                               (mapcat (fn [descriptor block]
                                         (validated-run-block-rows
                                          descriptor block
                                          (keyword (get node "index"))
                                          (get node "tenant")))
                                       (sort-by #(get % "ordinal") blocks)
                                       (array-seq loaded)))]
                          (when-not (= (get node "count") (count rows))
                            (throw (ex-info "Merkle run block count mismatch"
                                            {:expected (get node "count")
                                             :actual (count rows)})))
                          {:node node :rows rows}))))
           {:node node :rows (get node "rows")})))))

(defn- load-runs! [e refs]
  (-> (mapv #(load-run! e %) refs)
      clj->js
      js/Promise.all
      (.then #(vec (array-seq %)))))

(def ^:private default-stream-limits
  {:max-open-runs 64
   :max-input-block-bytes (* 2 1024 1024)
   ;; Four disjoint components may run concurrently. Each component is capped
   ;; at 16 MiB so the aggregate physical input budget remains 64 MiB.
   :max-working-input-bytes (* 16 1024 1024)
   :max-inline-run-rows 4096
   :max-logical-group-rows 65536})
(def ^:private compact-range-concurrency 4)

(defn- run-cursor!
  [e index limits metrics ref]
  (let [descriptors (vec (sort-by #(get % "ordinal") (get ref "blocks")))]
    (if (seq descriptors)
      (js/Promise.resolve
       {:ref ref :descriptors descriptors :next-block 0 :rows [] :row-index 0
        :loaded-bytes 0})
      (let [encoded-bytes (get ref "encoded-bytes")]
        (when (or (nil? encoded-bytes)
                  (> encoded-bytes (:max-input-block-bytes limits))
                  (> (+ (:current-buffered-input-bytes @metrics) encoded-bytes)
                     (:max-working-input-bytes limits)))
          (throw (ex-info "Inline Merkle run exceeds streaming byte bound"
                          {:index index :encoded-bytes encoded-bytes
                           :max-input-block-bytes
                           (:max-input-block-bytes limits)
                           :max-working-input-bytes
                           (:max-working-input-bytes limits)})))
        (-> (load-run! e ref)
          (.then
           (fn [{:keys [rows]}]
             (when (> (count rows) (:max-inline-run-rows limits))
               (throw (ex-info "Inline Merkle run exceeds streaming bound"
                               {:index index :rows (count rows)
                                :max-inline-run-rows
                                (:max-inline-run-rows limits)})))
             (swap! metrics
                    (fn [m]
                      (let [current-rows
                            (+ (:current-buffered-input-rows m) (count rows))
                            current-bytes
                            (+ (:current-buffered-input-bytes m) encoded-bytes)]
                        (-> m
                            (assoc :current-buffered-input-rows current-rows
                                   :current-buffered-input-bytes current-bytes)
                            (update :max-buffered-input-rows max current-rows)
                            (update :max-buffered-input-bytes max current-bytes)))))
             {:ref ref :descriptors [] :next-block 0
              :rows (vec rows) :row-index 0
              :loaded-bytes encoded-bytes})))))))

(defn- cursor-row [cursor]
  (get (:rows cursor) (:row-index cursor)))

(defn- ensure-cursor-row!
  "Load at most one physical data block for a cursor. A consumed block is
  replaced, never accumulated, so the live input is bounded by open runs."
  [e index tenant limits metrics cursor]
  (cond
    (cursor-row cursor) (js/Promise.resolve cursor)
    (>= (:next-block cursor) (count (:descriptors cursor)))
    (js/Promise.resolve nil)
    :else
    (let [descriptor (get (:descriptors cursor) (:next-block cursor))
          encoded-bytes (get descriptor "encoded-bytes")]
      (when (or (nil? encoded-bytes)
                (> encoded-bytes (:max-input-block-bytes limits)))
        (throw (ex-info "Merkle input block lacks a valid streaming byte bound"
                        {:index index :encoded-bytes encoded-bytes
                         :max-input-block-bytes
                         (:max-input-block-bytes limits)})))
      (when (> (+ (:current-buffered-input-bytes @metrics) encoded-bytes)
               (:max-working-input-bytes limits))
        (throw
         (ex-info "Compaction input working set exceeds streaming byte bound"
                  {:index index
                   :current-buffered-input-bytes
                   (:current-buffered-input-bytes @metrics)
                   :next-block-bytes encoded-bytes
                   :max-working-input-bytes
                   (:max-working-input-bytes limits)})))
      (-> (get-node! e (ipld/link-cid (get descriptor "cid")))
          (.then
           (fn [block]
             (let [rows (vec (validated-run-block-rows
                              descriptor block index tenant))]
               (swap! metrics
                      (fn [m]
                        (let [current-rows
                              (+ (:current-buffered-input-rows m) (count rows))
                              current-bytes
                              (+ (:current-buffered-input-bytes m) encoded-bytes)]
                          (-> m
                              (update :input-block-gets inc)
                              (update :input-block-bytes + encoded-bytes)
                              (assoc :current-buffered-input-rows current-rows
                                     :current-buffered-input-bytes current-bytes)
                              (update :max-buffered-input-rows max current-rows)
                              (update :max-buffered-input-bytes max current-bytes)))))
               (assoc cursor
                      :next-block (inc (:next-block cursor))
                      :rows rows :row-index 0
                      :loaded-bytes encoded-bytes))))))))

(defn- advance-cursor!
  [e index tenant limits metrics cursor]
  (let [next-index (inc (:row-index cursor))
        exhausted? (>= next-index (count (:rows cursor)))
        cursor (assoc cursor :row-index next-index)]
    (when exhausted?
      (swap! metrics
             (fn [m]
               (-> m
                   (update :current-buffered-input-rows - (count (:rows cursor)))
                   (update :current-buffered-input-bytes -
                           (:loaded-bytes cursor))))))
    (ensure-cursor-row!
     e index tenant limits metrics
     (cond-> cursor exhausted?
       (assoc :rows [] :row-index 0 :loaded-bytes 0)))))

(defn- initialize-cursors!
  [e index tenant limits metrics refs]
  (when (> (count refs) (:max-open-runs limits))
    (throw (ex-info "Compaction overlap exceeds streaming open-run bound"
                    {:index index :runs (count refs)
                     :max-open-runs (:max-open-runs limits)})))
  ;; Initialize sequentially. Each cursor retains no more than one data block;
  ;; avoiding Promise.all also prevents a burst of all input blocks at once.
  (reduce
   (fn [result ref]
     (.then result
            (fn [cursors]
              (-> (run-cursor! e index limits metrics ref)
                  (.then #(ensure-cursor-row!
                           e index tenant limits metrics %))
                  (.then #(cond-> cursors % (conj %)))))))
   (js/Promise.resolve []) refs))

(defn- next-cursor-index [cursors]
  (first
   (reduce-kv
    (fn [[best-index best-key :as best] i cursor]
      (if-let [row (cursor-row cursor)]
        (let [key (get row "key")]
          (if (or (nil? best-key)
                  (neg? (compare key best-key))
                  (and (= key best-key) (< i best-index)))
            [i key]
            best))
        best))
    [nil nil] cursors)))

(defn- take-logical-group!
  [e index tenant limits metrics cursors]
  (when-let [first-index (next-cursor-index cursors)]
    (let [logical-key (lsm/row-logical-key
                       (cursor-row (get cursors first-index)))]
      (letfn [(step [current rows]
                (if-let [i (next-cursor-index current)]
                  (let [cursor (get current i)
                        row (cursor-row cursor)]
                    (if (= logical-key (lsm/row-logical-key row))
                      (do
                        (when (>= (count rows)
                                  (:max-logical-group-rows limits))
                          (throw
                           (ex-info "Logical key exceeds streaming compaction bound"
                                    {:index index :logical-key logical-key
                                     :max-logical-group-rows
                                     (:max-logical-group-rows limits)})))
                        (-> (advance-cursor!
                             e index tenant limits metrics cursor)
                            (.then
                             (fn [next-cursor]
                               (step (vec (remove nil?
                                                  (assoc current i next-cursor)))
                                     (conj rows row))))))
                      (js/Promise.resolve
                       {:cursors (vec (remove nil? current)) :rows rows})))
                  (js/Promise.resolve {:cursors [] :rows rows})))]
        (step cursors [])))))

(defn- retained-group-entries [safe-epoch rows]
  (let [{newer true older false}
        (group-by #(> (get % "epoch") safe-epoch) (distinct rows))]
    (mapv (fn [row]
            {:components (get row "components")
             :epoch (get row "epoch")
             :op (keyword (get row "op"))
             :value (get row "value")})
          (concat newer (take 1 older)))))

(defn- put-output-run!
  [e index tenant entries metrics]
  (let [run (lsm/build-run index tenant entries)
        puts (filterv #(= :block/put (:effect/type %)) (:effects run))
        put-bytes (reduce + (map #(.-byteLength (:bytes %)) puts))]
    (-> (put-blocks! e (:effects run))
        (.then
         (fn [_]
           (swap! metrics
                  (fn [m]
                    (-> m
                        (update :output-runs inc)
                        (update :output-rows + (:count run))
                        (update :output-object-puts + (count puts))
                        (update :output-object-bytes + put-bytes))))
           (lsm/run-ref run))))))

(defn- compact-run-component-streaming!
  [e db-id index safe-epoch target-run-rows limits refs]
  (let [metrics
        (atom {:input-runs (count refs)
               :input-block-gets 0 :input-block-bytes 0
               :current-buffered-input-rows 0
               :current-buffered-input-bytes 0
               :max-buffered-input-rows 0 :max-buffered-input-bytes 0
               :output-runs 0 :output-rows 0
               :output-object-puts 0 :output-object-bytes 0})]
    (letfn [(flush-pending [output-refs pending]
              (if (seq pending)
                (-> (put-output-run! e index db-id pending metrics)
                    (.then (fn [ref] [(conj output-refs ref) []])))
                (js/Promise.resolve [output-refs []])))
            (step [cursors output-refs pending]
              (if (empty? cursors)
                (.then (flush-pending output-refs pending)
                       (fn [[refs _]]
                         {:refs refs
                          :metrics
                          (dissoc @metrics
                                  :current-buffered-input-rows
                                  :current-buffered-input-bytes)}))
                (.then
                 (take-logical-group!
                  e index db-id limits metrics cursors)
                 (fn [{next-cursors :cursors rows :rows}]
                   (let [entries (retained-group-entries safe-epoch rows)]
                     (if (and (seq pending)
                              (> (+ (count pending) (count entries))
                                 target-run-rows))
                       (.then
                        (flush-pending output-refs pending)
                        (fn [[next-refs _]]
                          (step next-cursors next-refs entries)))
                       (step next-cursors output-refs
                             (into pending entries))))))))]
      (-> (initialize-cursors! e index db-id limits metrics refs)
          (.then #(step % [] []))))))

(defn- merge-stream-metrics [left right]
  (reduce-kv
   (fn [totals k value]
     (assoc totals k
            ((if (contains? #{:max-buffered-input-rows
                              :max-buffered-input-bytes}
                            k)
               max +)
             (get totals k 0) value)))
   left right))

(defn- promoted-component-result [refs]
  {:refs (vec refs)
   :metrics
   {:input-runs 0
    :input-block-gets 0 :input-block-bytes 0
    :max-buffered-input-rows 0 :max-buffered-input-bytes 0
    :output-runs 0 :output-rows 0
    :output-object-puts 0 :output-object-bytes 0
    :promoted-runs (count refs)}})

(defn- compact-index-ranges!
  "Stream disjoint overlap components in bounded parallel waves. Each
  component holds one bounded data block per open input run and one bounded
  output run; no component materializes its complete row set."
  [e db-id index safe-epoch target-run-rows refs]
  (reduce
   (fn [result wave]
     (.then
      result
      (fn [{output-refs :refs aggregate :metrics}]
        (.then
         (js/Promise.all
          (clj->js
           (mapv
            (fn [{:keys [refs]}]
              (if (= 1 (count refs))
                (js/Promise.resolve (promoted-component-result refs))
                (compact-run-component-streaming!
                 e db-id index safe-epoch target-run-rows
                 default-stream-limits refs)))
            wave)))
         (fn [results]
           (let [results (vec (array-seq results))
                 wave-refs (mapcat :refs results)
                 ;; Concurrent component peaks may coincide, so sum their
                 ;; individual peaks within a wave. Across sequential waves
                 ;; only the maximum is retained.
                 wave-metrics (reduce
                               #(merge-with + %1 (:metrics %2))
                               {} results)]
             {:refs (into output-refs wave-refs)
              :metrics (merge-stream-metrics
                        aggregate wave-metrics)}))))))
   (js/Promise.resolve {:refs [] :metrics {}})
   (partition-all compact-range-concurrency
                  (lsm/overlapping-run-ranges refs))))

(defn- load-tail-checkpoint! [e db-id tail]
  (if-not tail
    (js/Promise.resolve nil)
    (-> (get-node! e tail)
        (.then
         (fn [manifest]
           (when-let [directory-link
                      (get-in manifest ["statistics" "range-directory"])]
             (-> (get-node! e (ipld/link-cid directory-link))
                 (.then
                  (fn [directory]
                    (let [source
                          (lsm/validate-range-directory
                           directory db-id (get manifest "epoch"))]
                      {:directory source
                       :source-directory source}))))))))))

(defn retention-safe-epoch-oracle!
  "Return the explicit clock-skew-conservative R2 retention decision consumed
  by compaction and GC. NOW-MS is injectable for deterministic hosts."
  ([e db-id] (retention-safe-epoch-oracle! e db-id (js/Date.now)))
  ([e db-id now-ms]
   (if-let [bucket (env e "MERKLE_BUCKET")]
     (-> (all-r2-retention-roots! e bucket)
         (.then (fn [roots]
                  (retention/safe-epoch-oracle
                   (->> roots
                        (mapv :root)
                        (filterv #(= db-id (get % "db-id"))))
                   now-ms (retention-clock-skew-ms e)))))
     (js/Promise.resolve
      (retention/safe-epoch-oracle [] now-ms (retention-clock-skew-ms e))))))

(defn retention-safe-epoch!
  "Return the safe epoch from the same explicit oracle used by GC."
  ([e db-id] (retention-safe-epoch! e db-id (js/Date.now)))
  ([e db-id now-ms]
   (-> (retention-safe-epoch-oracle! e db-id now-ms)
       (.then (fn [oracle] (:safe-epoch oracle))))))

(defn compact-head!
  "Compact the newest manifest window into range-partitioned L1 runs and
  publish it with R2 CAS. The untouched tail remains linked as :previous.
  Returns Promise<boolean>; false means a concurrent writer won the head."
  ([e db-id] (compact-head! e db-id 64 4096))
  ([e db-id window-size] (compact-head! e db-id window-size 4096))
  ([e db-id window-size target-run-rows]
   (-> (retention-safe-epoch-oracle! e db-id)
       (.then #(compact-head! e db-id window-size target-run-rows
                              (:safe-epoch %)))))
  ([e db-id window-size target-run-rows root-safe-epoch]
   (-> (resolve-database-head! e db-id)
       (.then
        (fn [{:keys [head-cid base-cid etag publication]}]
          (if-not head-cid
            false
            (-> (manifest-window! e base-cid window-size)
                (.then
                 (fn [{:keys [manifests tail]}]
                   (if (empty? manifests)
                     false
                     (let [present (filter #(seq (index-run-refs manifests %))
                                           lsm/indexes)
                           epoch (apply max
                                        (map #(get-in % [:node "epoch"])
                                             manifests))
                           safe-epoch (if root-safe-epoch
                                        (min epoch root-safe-epoch)
                                        epoch)]
                       (-> (load-tail-checkpoint! e db-id tail)
                           (.then
                            (fn [inherited]
                              (-> (reduce
                                   (fn [result index]
                                     (.then
                                      result
                                      (fn [compacted]
                                        (let [current
                                              (index-run-refs manifests index)
                                              source
                                              (:source-directory inherited)
                                              selection-promise
                                              (cond
                                                (nil? inherited)
                                                (js/Promise.resolve
                                                 {:selection
                                                  {:inputs current
                                                   :untouched []}
                                                  :untouched-pages []
                                                  :selected-page-count 0})

                                                (lsm/paged-range-directory?
                                                 source)
                                                (let [page-selection
                                                      (lsm/checkpoint-directory-page-selection
                                                       current source index)]
                                                  (->
                                                   (load-directory-pages!
                                                    e db-id source index
                                                    (:selected-pages
                                                     page-selection))
                                                   (.then
                                                    (fn [selected-refs]
                                                      {:selection
                                                       (lsm/checkpoint-compaction-selection
                                                        current
                                                        {"indexes"
                                                         {(name index)
                                                          selected-refs}}
                                                        index)
                                                       :untouched-pages
                                                       (:untouched-pages
                                                        page-selection)
                                                       :selected-page-count
                                                       (count
                                                        (:selected-pages
                                                         page-selection))}))))

                                                :else
                                                (js/Promise.resolve
                                                 {:selection
                                                  (lsm/checkpoint-compaction-selection
                                                   current (:directory inherited)
                                                   index)
                                                  :untouched-pages []
                                                  :selected-page-count 0}))]
                                          (->
                                           selection-promise
                                           (.then
                                            (fn [{:keys [selection
                                                        untouched-pages
                                                        selected-page-count]}]
                                              (->
                                               (compact-index-ranges!
                                                e db-id index safe-epoch
                                                target-run-rows
                                                (:inputs selection))
                                               (.then
                                                #(assoc
                                                  compacted index
                                                  (assoc
                                                   %
                                                   :untouched
                                                   (:untouched selection)
                                                   :untouched-pages
                                                   untouched-pages
                                                   :selected-page-count
                                                   selected-page-count)))))))))))
                                  (js/Promise.resolve {})
                                  present)
                                  (.then
                                   (fn [compaction-results]
                                     (let [output-indexes
                                           (into {}
                                                 (map (fn [[index result]]
                                                        [index (:refs result)]))
                                                 compaction-results)
                                           compacted
                                           (into {}
                                                 (map
                                                  (fn [[index result]]
                                                    [index
                                                     (vec
                                                      (concat
                                                       (:refs result)
                                                       (:untouched result)))]))
                                                 compaction-results)
                                           stream-metrics
                                           (reduce
                                            (fn [totals result]
                                              (merge-stream-metrics
                                               totals (:metrics result)))
                                            {} (vals compaction-results))
                                           paged-inherited?
                                           (and inherited
                                                (lsm/paged-range-directory?
                                                 (:source-directory inherited)))
                                           directory-indexes
                                           (if (and inherited
                                                    (not paged-inherited?))
                                             (lsm/merge-range-directory-indexes
                                              compacted (:directory inherited))
                                             compacted)
                                         directory-previous
                                         (if inherited
                                           (some-> (get (:source-directory
                                                        inherited)
                                                       "previous")
                                                   ipld/link-cid)
                                           tail)
                                         new-pages
                                         (lsm/build-range-directory-pages
                                          {:db-id db-id :epoch epoch
                                           :indexes directory-indexes})
                                         new-page-descriptors
                                         (into {}
                                               (map (fn [[index pages]]
                                                      [(keyword index)
                                                       (mapv :descriptor
                                                             pages)]))
                                               new-pages)
                                         inherited-page-indexes
                                         (when paged-inherited?
                                           (into {}
                                                 (map
                                                  (fn [[index descriptors]]
                                                    [(keyword index)
                                                     descriptors]))
                                                 (get (:source-directory
                                                       inherited)
                                                      "indexes")))
                                         directory-page-indexes
                                         (if paged-inherited?
                                           (reduce
                                            (fn [all [index result]]
                                              (assoc
                                               all index
                                               (vec
                                                (concat
                                                 (:untouched-pages result)
                                                 (get
                                                  new-page-descriptors
                                                  index [])))))
                                            inherited-page-indexes
                                            compaction-results)
                                           new-page-descriptors)
                                         directory
                                         (lsm/build-paged-range-directory-root
                                          {:db-id db-id :epoch epoch
                                           :indexes directory-page-indexes
                                           :previous directory-previous})
                                         new-page-list (mapcat val new-pages)
                                         directory-effects
                                         (concat
                                          (mapcat :effects new-page-list)
                                          (:effects directory))
                                         new-page-bytes
                                         (reduce +
                                                 (map #(.-byteLength (:bytes %))
                                                      new-page-list))
                                         directory-page-count
                                         (reduce +
                                                 (map count
                                                      (vals
                                                       directory-page-indexes)))
                                         reused-page-count
                                         (if paged-inherited?
                                           (- directory-page-count
                                              (count new-page-list))
                                           0)
                                         selected-page-count
                                         (reduce +
                                                 (map :selected-page-count
                                                      (vals
                                                       compaction-results)))
                                         manifest (lsm/build-manifest
                                                   {:db-id db-id :epoch epoch
                                                    :safe-epoch safe-epoch
                                                    :previous (when-not inherited tail)
                                                    :indexes (into {}
                                                                   (map (fn [[index runs]]
                                                                          [index {:l1 runs}]))
                                                                   output-indexes)
                                                    :statistics
                                                    {"operation" "window-compaction"
                                                     "range-directory" (ipld/link (:cid directory))
                                                     "inherited-checkpoint" (boolean inherited)
                                                     "manifest-count" (count manifests)
                                                     "target-run-rows" target-run-rows
                                                     "streaming-compaction" true
                                                     "range-directory-version"
                                                     lsm/range-directory-version
                                                     "range-directory-page-refs"
                                                     lsm/default-range-directory-page-refs
                                                     "range-directory-page-byte-limit"
                                                     lsm/default-range-directory-page-bytes
                                                     "range-directory-page-count"
                                                     directory-page-count
                                                     "range-directory-page-put-count"
                                                     (count new-page-list)
                                                     "range-directory-reused-page-count"
                                                     reused-page-count
                                                     "range-directory-page-get-count"
                                                     selected-page-count
                                                     "range-directory-root-bytes"
                                                     (.-byteLength
                                                      (:bytes directory))
                                                     "range-directory-page-bytes"
                                                     new-page-bytes
                                                     "range-directory-page-put-bytes"
                                                     new-page-bytes
                                                     "stream-metrics"
                                                     (into {}
                                                           (map (fn [[k v]]
                                                                  [(name k) v]))
                                                           stream-metrics)
                                                     "output-run-count"
                                                     (reduce +
                                                             (map count
                                                                  (vals
                                                                   output-indexes)))}})
                                         base-effects (concat directory-effects
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
                                                                etag))))))))))))))))))))))))))

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
                               (not= "running" (get current "status")))
                        {:claimed? false :reason :terminal
                         :status (keyword (get current "status"))}
                        (if (and current
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
                                           :reason :cas-lost})))))))))))))))))
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

(defn cancel-resumable-execution!
  "Atomically fence a running resumable task with a terminal cancelled
  checkpoint. The pointer ETag, rather than a lease token supplied by the
  operator, arbitrates cancellation against an in-flight advance. Repeating a
  successful cancellation is idempotent; completed and failed results are
  never overwritten."
  [e db-id task-id reason]
  (when-not (and (string? db-id) (seq db-id)
                 (string? task-id) (seq task-id)
                 (string? reason) (seq reason))
    (throw (ex-info "Invalid resumable cancellation"
                    {:db-id db-id :task-id task-id :reason reason})))
  (if-let [bucket (env e "MERKLE_BUCKET")]
    (-> (get-resumable-pointer! e db-id task-id)
        (.then
         (fn [{:keys [pointer checkpoint etag]}]
           (cond
             (nil? pointer)
             {:cancelled? false :reason :not-found}

             (= "cancelled" (get pointer "status"))
             {:cancelled? true :idempotent? true
              :pointer pointer :checkpoint checkpoint :etag etag}

             (not= "running" (get pointer "status"))
             {:cancelled? false :reason :terminal
              :status (keyword (get pointer "status"))}

             :else
             (let [task-cid (ipld/link-cid (get checkpoint "task"))]
               (-> (get-node! e task-cid)
                   (.then
                    (fn [task-node]
                      (let [task {:cid task-cid :node task-node}
                            terminal
                            (resumable/finish
                             {:task task :checkpoint checkpoint
                              :token (get pointer "token")
                              :attempt (get pointer "attempt")
                              :status :cancelled
                              :error {"reason" reason}})
                            next-pointer
                            (assoc pointer
                                   "checkpoint" (str (:cid terminal))
                                   "status" "cancelled")]
                        (-> (put-block! e (:cid terminal) (:bytes terminal))
                            (.then
                             (fn [_]
                               (-> (.put
                                    bucket
                                    (resumable-pointer-key e db-id task-id)
                                    (js/JSON.stringify
                                     (clj->js next-pointer))
                                    #js {:onlyIf
                                         #js {:etagMatches etag}})
                                   (.then
                                    (fn [result]
                                      (if result
                                        {:cancelled? true
                                         :idempotent? false
                                         :pointer next-pointer
                                         :checkpoint (:node terminal)
                                         :etag (gobj/get result "etag")}
                                        {:cancelled? false
                                         :reason :pointer-fenced}))))))))))))))))
    (js/Promise.reject
     (js/Error. "Resumable execution currently requires an R2 binding"))))

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
        oracle (retention/safe-epoch-oracle
                (mapv :root roots) now-ms (retention-clock-skew-ms e))
        active-root-nodes (set (:active-roots oracle))
        active-resumable-roots
        (filterv (fn [{:keys [db-id expected-head status]}]
                   (and (contains? #{"running" "completed"} status)
                        (= expected-head
                           (get head-by-key (head-key e db-id)))))
                 resumable-roots)
        active-roots (filterv #(contains? active-root-nodes (:root %)) roots)
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
              :safe-epoch (:safe-epoch oracle)
              :retention-active-by-kind (:active-by-kind oracle)
              :retention-clock-skew-ms (:clock-skew-ms oracle)
              :candidate-keys (->> (concat candidates
                                           stale-resumable-pointer-keys
                                           stale-ingress-pointer-keys)
                                   distinct sort vec)
              :block-candidates (count candidates)
              :pointer-candidates (+ (count stale-resumable-pointer-keys)
                                     (count stale-ingress-pointer-keys))
              :candidates (+ (count candidates)
                             (count stale-resumable-pointer-keys)
                             (count stale-ingress-pointer-keys))})))
        (.catch (fn [error]
                  (js/Promise.reject
                   (js/Error. (str "GC mark audit failed: " error))))))))

(defn- gc-backup-bucket [e]
  (or (env e "MERKLE_GC_BACKUP_BUCKET") (env e "MERKLE_BUCKET")))

(defn- stored-bytes! [stored]
  (if (gobj/get stored "arrayBuffer")
    (-> (.arrayBuffer stored) (.then #(js/Uint8Array. %)))
    (-> (.text stored) (.then #(.encode (js/TextEncoder.) %)))))

(defn- restorable-gc-key? [e key]
  (some #(str/starts-with? key %)
        [(str (prefix e) "blocks/")
         (str (prefix e) "scheduler/resumable/")
         (str (prefix e) "scheduler/ingress/")]))

(defn- verify-gc-entry-bytes! [e key expected-cid bytes]
  (let [actual (str (ipld/cid bytes))
        block-prefix (str (prefix e) "blocks/")]
    (when-not (= expected-cid actual)
      (throw (ex-info "GC backup content CID mismatch"
                      {:key key :expected expected-cid :actual actual})))
    (when (and (str/starts-with? key block-prefix)
               (not= (subs key (count block-prefix)) actual))
      (throw (ex-info "GC block key does not match its content CID"
                      {:key key :actual actual})))
    bytes))

(defn- put-immutable-gc-backup! [bucket key expected-cid bytes]
  (-> (.put bucket key bytes #js {:onlyIf #js {:etagDoesNotMatch "*"}})
      (.then
       (fn [written]
         (if written
           true
           (-> (.get bucket key)
               (.then
                (fn [existing]
                  (if-not existing
                    (js/Promise.reject
                     (ex-info "GC backup put lost without existing winner"
                              {:key key :cid expected-cid}))
                    (-> (stored-bytes! existing)
                        (.then
                         (fn [existing-bytes]
                           (let [actual (str (ipld/cid existing-bytes))]
                             (when-not (= expected-cid actual)
                               (throw
                                (ex-info "GC immutable backup CID conflict"
                                         {:key key :expected expected-cid
                                          :actual actual})))
                             false)))))))))))))

(defn- backup-one-candidate! [e primary backup-bucket key]
  (when-not (restorable-gc-key? e key)
    (throw (ex-info "GC candidate is outside restorable namespaces"
                    {:key key})))
  (-> (.get primary key)
      (.then
       (fn [stored]
         (if-not stored
           (js/Promise.reject
            (ex-info "GC candidate disappeared before backup" {:key key}))
           (-> (stored-bytes! stored)
               (.then
                (fn [bytes]
                  (let [content-cid (str (ipld/cid bytes))]
                    (verify-gc-entry-bytes! e key content-cid bytes)
                    (-> (put-immutable-gc-backup!
                         backup-bucket
                         (gc-backup-object-key e content-cid)
                         content-cid bytes)
                        (.then
                         (fn [_]
                           {"key" key
                            "content-cid" content-cid
                            "bytes" (.-byteLength bytes)}))))))))))))

(defn- backup-and-delete-candidates!
  [e primary result candidates now-ms]
  (if-let [backup-bucket (gc-backup-bucket e)]
    (-> (reduce
         (fn [pending key]
           (.then pending
                  (fn [entries]
                    (-> (backup-one-candidate! e primary backup-bucket key)
                        (.then #(conj entries %))))))
         (js/Promise.resolve [])
         candidates)
        (.then
         (fn [entries]
           (let [node {"format" "kotobase/gc-backup-inventory"
                       "version" 1
                       "prefix" (prefix e)
                       "created-at" now-ms
                       "candidates" entries}
                 bytes (ipld/encode node)
                 cid (str (ipld/cid bytes))]
             (-> (put-immutable-gc-backup!
                  backup-bucket (gc-inventory-key e cid) cid bytes)
                 (.then
                  (fn [_]
                    (-> (.delete primary (clj->js candidates))
                        (.then
                         #(assoc result
                                 :deleted (count candidates)
                                 :inventory-passes 2
                                 :backup-inventory cid
                                 :backed-up (count entries)))))))))))
    (js/Promise.reject
     (js/Error. "GC delete requires MERKLE_BUCKET or MERKLE_GC_BACKUP_BUCKET"))))

(defn- validate-gc-inventory [e cid node]
  (let [entries (get node "candidates")]
    (when-not (and (= "kotobase/gc-backup-inventory" (get node "format"))
                   (= 1 (get node "version"))
                   (= (prefix e) (get node "prefix"))
                   (integer? (get node "created-at"))
                   (vector? entries)
                   (every? (fn [entry]
                             (and (map? entry)
                                  (string? (get entry "key"))
                                  (restorable-gc-key? e (get entry "key"))
                                  (string? (get entry "content-cid"))
                                  (integer? (get entry "bytes"))
                                  (not (neg? (get entry "bytes")))))
                           entries))
      (throw (ex-info "Invalid GC backup inventory" {:cid cid :node node})))
    node))

(defn- restore-existing-gc-entry! [e primary counts key content-cid]
  (-> (.get primary key)
      (.then
       (fn [existing]
         (if-not existing
           (js/Promise.reject
            (ex-info "GC restore CAS lost without winner" {:key key}))
           (-> (stored-bytes! existing)
               (.then
                (fn [existing-bytes]
                  (verify-gc-entry-bytes! e key content-cid existing-bytes)
                  (update counts :already-present inc)))))))))

(defn- restore-one-gc-entry!
  [e primary backup-bucket counts entry]
  (let [key (get entry "key")
        content-cid (get entry "content-cid")]
    (-> (.get backup-bucket (gc-backup-object-key e content-cid))
        (.then
         (fn [stored]
           (if-not stored
             (js/Promise.reject
              (ex-info "GC backup object not found"
                       {:key key :cid content-cid}))
             (stored-bytes! stored))))
        (.then
         (fn [bytes]
           (verify-gc-entry-bytes! e key content-cid bytes)
           (-> (.put primary key bytes
                     #js {:onlyIf #js {:etagDoesNotMatch "*"}})
               (.then
                (fn [written]
                  (if written
                    (update counts :restored inc)
                    (restore-existing-gc-entry!
                     e primary counts key content-cid))))))))))

(defn- load-gc-inventory! [e backup-bucket inventory-cid]
  (-> (.get backup-bucket (gc-inventory-key e inventory-cid))
      (.then
       (fn [stored]
         (if-not stored
           (js/Promise.reject
            (ex-info "GC backup inventory not found" {:cid inventory-cid}))
           (stored-bytes! stored))))
      (.then
       (fn [inventory-bytes]
         (when-not (= inventory-cid (str (ipld/cid inventory-bytes)))
           (throw (ex-info "GC inventory CID mismatch" {:cid inventory-cid})))
         (validate-gc-inventory
          e inventory-cid (ipld/decode inventory-bytes))))))

(defn restore-gc-inventory!
  "Restore a backed-up GC inventory without overwriting newer state. Every
  backup object is CID-verified; existing destination bytes must match exactly."
  [e inventory-cid]
  (let [primary (env e "MERKLE_BUCKET")
        backup-bucket (gc-backup-bucket e)]
    (if-not (and primary backup-bucket)
      (js/Promise.reject
       (js/Error. "GC restore requires MERKLE_BUCKET and a backup bucket"))
      (-> (load-gc-inventory! e backup-bucket inventory-cid)
          (.then
           (fn [inventory]
             (reduce
              (fn [pending entry]
                (.then pending
                       (fn [counts]
                         (restore-one-gc-entry!
                          e primary backup-bucket counts entry))))
              (js/Promise.resolve {:inventory inventory-cid
                                   :restored 0 :already-present 0})
              (get inventory "candidates"))))))))

(defn- ensure-backup-retention-root!
  [e db-id backup-id head-cid epoch]
  (-> (get-retention-root! e db-id :backup backup-id)
      (.then
       (fn [{existing :root :keys [etag]}]
         (cond
           (and existing
                (= head-cid (get existing "manifest-cid"))
                (= epoch (get existing "epoch"))
                (nil? (get existing "released-at")))
           {:root existing :etag etag :idempotent? true}

           existing
           (js/Promise.reject
            (ex-info "Database backup id already pins another snapshot"
                     {:db-id db-id :backup-id backup-id
                      :existing-head (get existing "manifest-cid")
                      :requested-head head-cid}))

           :else
           (.then
            (cas-retention-root!
             e {:db-id db-id :kind :backup :id backup-id
                :manifest-cid head-cid :epoch epoch}
             nil)
            (fn [created]
              (if (:won? created)
                (assoc created :idempotent? false)
                (ensure-backup-retention-root!
                 e db-id backup-id head-cid epoch)))))))))

(defn- database-storage-key [e namespace cid]
  (case namespace
    "blocks" (block-key e cid)
    "objects" (object-key e cid)
    (throw (ex-info "Unknown database backup namespace"
                    {:namespace namespace :cid cid}))))

(defn- read-database-storage-entry!
  [e primary cid]
  (let [read-object
        (fn []
          (-> (.get primary (object-key e cid))
              (.then
               (fn [stored]
                 (if-not stored
                   (js/Promise.reject
                    (ex-info "Database backup link not found" {:cid cid}))
                   (-> (stored-bytes! stored)
                       (.then
                        (fn [bytes]
                          (verify-gc-entry-bytes!
                           e (object-key e cid) cid bytes)
                          {:namespace "objects" :cid cid :bytes bytes
                           :links []}))))))))]
    (-> (.get primary (block-key e cid))
        (.then
         (fn [stored]
           (if-not stored
             (read-object)
             (-> (stored-bytes! stored)
                 (.then
                  (fn [bytes]
                    (verify-gc-entry-bytes!
                     e (block-key e cid) cid bytes)
                    {:namespace "blocks" :cid cid :bytes bytes
                     :links (lsm/linked-cids (ipld/decode bytes))})))))))))

(defn- backup-database-storage-graph!
  [e primary backup-bucket root-cid]
  (let [seen (atom {})]
    (letfn
        [(walk [frontier]
           (let [fresh (->> frontier
                            (remove #(contains? @seen %))
                            distinct sort vec)]
             (if (empty? fresh)
               (js/Promise.resolve
                (->> (vals @seen)
                     (sort-by (juxt :namespace :cid))
                     vec))
               (-> (reduce
                    (fn [pending wave]
                      (.then
                       pending
                       (fn [loaded]
                         (-> (js/Promise.all
                              (clj->js
                               (mapv
                                (fn [cid]
                                  (-> (read-database-storage-entry!
                                       e primary cid)
                                      (.then
                                       (fn [entry]
                                         (-> (put-immutable-gc-backup!
                                              backup-bucket
                                              (gc-backup-object-key e cid)
                                              cid (:bytes entry))
                                             (.then (fn [_] entry)))))))
                                wave)))
                             (.then #(into loaded (array-seq %)))))))
                    (js/Promise.resolve [])
                    (partition-all 4 fresh))
                   (.then
                    (fn [loaded]
                      (swap! seen into
                             (map (fn [entry] [(:cid entry) entry]) loaded))
                      (walk (mapcat :links loaded))))))))]
      (walk [root-cid]))))

(defn- backup-inventory-entry
  [{:keys [namespace cid]}]
  {"namespace" namespace "cid" cid})

(defn database-backup-inventory-pages
  "Build deterministic, byte-bounded immutable inventory pages. ENTRIES must
  already be ordered by [namespace CID]."
  [entries]
  (->> entries
       (partition-all database-backup-page-entries)
       (map-indexed
        (fn [ordinal page-entries]
          (let [node {"format" "kotobase/database-backup-inventory-page"
                      "version" 1
                      "ordinal" ordinal
                      "entries" (mapv backup-inventory-entry page-entries)}
                bytes (ipld/encode node)
                encoded-bytes (.-byteLength bytes)]
            (when (> encoded-bytes database-backup-page-bytes)
              (throw
               (ex-info "Database backup inventory page exceeds byte limit"
                        {:ordinal ordinal
                         :entries (count page-entries)
                         :encoded-bytes encoded-bytes
                         :maximum database-backup-page-bytes})))
            {:cid (str (ipld/cid bytes))
             :bytes bytes
             :node node
             :descriptor
             {"cid" (str (ipld/cid bytes))
              "ordinal" ordinal
              "count" (count page-entries)
              "encoded-bytes" encoded-bytes
              "first" (backup-inventory-entry (first page-entries))
              "last" (backup-inventory-entry (peek (vec page-entries)))}})))
       vec))

(defn- publish-database-backup-pages!
  [e backup-bucket pages]
  (reduce
   (fn [pending wave]
     (.then
      pending
      (fn [_]
        (js/Promise.all
         (clj->js
          (mapv
           (fn [{:keys [cid bytes]}]
             (put-immutable-gc-backup!
              backup-bucket (database-backup-page-key e cid) cid bytes))
           wave))))))
   (js/Promise.resolve nil)
   (partition-all 4 pages)))

(defn backup-database!
  "Pin one immutable database snapshot, copy every reachable IPLD block into
  the immutable backup namespace, and publish a content-addressed inventory.
  BACKUP-ID is durable and idempotent for exactly one head CID."
  ([e db-id backup-id]
   (backup-database! e db-id backup-id (js/Date.now)))
  ([e db-id backup-id now-ms]
   (let [primary (env e "MERKLE_BUCKET")
         backup-bucket (gc-backup-bucket e)]
     (when-not (and (string? db-id) (seq db-id)
                    (string? backup-id) (seq backup-id)
                    (<= (count backup-id) 256)
                    (integer? now-ms) (not (neg? now-ms)))
       (throw (ex-info "Invalid database backup request"
                       {:db-id db-id :backup-id backup-id :now-ms now-ms})))
     (if-not (and primary backup-bucket)
       (js/Promise.reject
        (js/Error. "Database backup requires MERKLE_BUCKET and a backup bucket"))
       (-> (resolve-database-head! e db-id)
           (.then
            (fn [{:keys [head-cid base-cid]}]
              (if-not head-cid
                (js/Promise.reject
                 (ex-info "Database head not found" {:db-id db-id}))
                (-> (get-node! e base-cid)
                    (.then
                     (fn [manifest]
                       (let [epoch (get manifest "epoch")]
                         (when-not (and (integer? epoch) (not (neg? epoch)))
                           (throw
                            (ex-info "Database manifest has no valid epoch"
                                     {:db-id db-id :base-cid base-cid})))
                         (-> (ensure-backup-retention-root!
                              e db-id backup-id head-cid epoch)
                             (.then
                              (fn [root]
                                (-> (backup-database-storage-graph!
                                     e primary backup-bucket head-cid)
                                    (.then
                                     (fn [entries]
                                       (let [inventory-entries
                                             (mapv
                                              #(select-keys
                                                % [:namespace :cid])
                                              entries)
                                             pages
                                             (database-backup-inventory-pages
                                              inventory-entries)
                                             node
                                             {"format"
                                              "kotobase/database-backup-inventory"
                                              "version" 2
                                              "source-prefix" (prefix e)
                                              "db-id" db-id
                                              "backup-id" backup-id
                                              "head-cid" head-cid
                                              "base-cid" base-cid
                                              "epoch" epoch
                                              "entry-count" (count entries)
                                              "page-entry-limit"
                                              database-backup-page-entries
                                              "page-byte-limit"
                                              database-backup-page-bytes
                                              "pages"
                                              (mapv :descriptor pages)}
                                             bytes (ipld/encode node)
                                             cid (str (ipld/cid bytes))]
                                         (-> (publish-database-backup-pages!
                                              e backup-bucket pages)
                                             (.then
                                              (fn [_]
                                                (put-immutable-gc-backup!
                                                 backup-bucket
                                                 (database-backup-inventory-key
                                                  e cid)
                                                 cid bytes)))
                                             (.then
                                              (fn [_]
                                                         {:inventory cid
                                                          :head-cid head-cid
                                                          :base-cid base-cid
                                                          :epoch epoch
                                                          :observed-at now-ms
                                                          :inventory-pages
                                                          (count pages)
                                                          :inventory-root-bytes
                                                          (.-byteLength bytes)
                                                          :maximum-page-bytes
                                                          (reduce
                                                           max 0
                                                           (map #(get-in
                                                                  % [:descriptor
                                                                     "encoded-bytes"])
                                                                pages))
                                                          :entries
                                                          (count entries)
                                                          :blocks
                                                          (count
                                                           (filter
                                                            #(= "blocks"
                                                                (:namespace %))
                                                            entries))
                                                          :objects
                                                          (count
                                                           (filter
                                                            #(= "objects"
                                                                (:namespace %))
                                                            entries))
                                                          :bytes
                                                          (reduce
                                                           + 0
                                                           (map #(.-byteLength
                                                                  (:bytes %))
                                                                entries))
                                                          :retention-root
                                                          (:root root)
                                                          :idempotent?
                                                          (:idempotent?
                                                           root)}))))))))))))))))))))))

(defn- valid-database-backup-entry? [entry]
  (and (map? entry)
       (contains? #{"blocks" "objects"} (get entry "namespace"))
       (string? (get entry "cid"))
       (re-matches #"b[a-z2-7]+" (get entry "cid"))))

(defn- normalized-database-backup-entries [entries]
  (mapv (fn [entry]
          [(get entry "namespace") (get entry "cid")])
        entries))

(defn- validate-materialized-database-backup-entries
  [cid node entries]
  (let [normalized (normalized-database-backup-entries entries)]
    (when-not
     (and (vector? entries)
          (= normalized (vec (sort (distinct normalized))))
          (every? valid-database-backup-entry? entries)
          (some #{["blocks" (get node "head-cid")]} normalized)
          (some #{["blocks" (get node "base-cid")]} normalized))
      (throw (ex-info "Invalid database backup inventory entries"
                      {:cid cid :node node})))
    entries))

(defn- valid-database-backup-page-descriptor? [index descriptor]
  (and (map? descriptor)
       (= index (get descriptor "ordinal"))
       (string? (get descriptor "cid"))
       (re-matches #"b[a-z2-7]+" (get descriptor "cid"))
       (pos-int? (get descriptor "count"))
       (<= (get descriptor "count") database-backup-page-entries)
       (pos-int? (get descriptor "encoded-bytes"))
       (<= (get descriptor "encoded-bytes") database-backup-page-bytes)
       (valid-database-backup-entry? (get descriptor "first"))
       (valid-database-backup-entry? (get descriptor "last"))))

(defn- validate-database-backup-inventory [e cid node]
  (let [version (get node "version")
        common?
        (and (= "kotobase/database-backup-inventory" (get node "format"))
             (contains? #{1 2} version)
             (= (prefix e) (get node "source-prefix"))
             (string? (get node "db-id"))
             (string? (get node "backup-id"))
             (string? (get node "head-cid"))
             (string? (get node "base-cid"))
             (integer? (get node "epoch")))]
    (when-not common?
      (throw (ex-info "Invalid database backup inventory"
                      {:cid cid :node node})))
    (case version
      1
      (do
        (validate-materialized-database-backup-entries
         cid node (get node "entries"))
        node)

      2
      (let [pages (get node "pages")]
        (when-not
         (and (vector? pages)
              (seq pages)
              (every? identity
                      (map-indexed
                       valid-database-backup-page-descriptor? pages))
              (= (get node "entry-count")
                 (reduce + 0 (map #(get % "count") pages)))
              (= database-backup-page-entries
                 (get node "page-entry-limit"))
              (= database-backup-page-bytes
                 (get node "page-byte-limit")))
          (throw (ex-info "Invalid paged database backup inventory"
                          {:cid cid :node node})))
        node))))

(defn- load-database-backup-page!
  [e backup-bucket descriptor]
  (let [cid (get descriptor "cid")]
    (-> (.get backup-bucket (database-backup-page-key e cid))
        (.then
         (fn [stored]
           (if-not stored
             (js/Promise.reject
              (ex-info "Database backup inventory page not found"
                       {:cid cid}))
             (stored-bytes! stored))))
        (.then
         (fn [bytes]
           (when-not (= cid (str (ipld/cid bytes)))
             (throw
              (ex-info "Database backup inventory page CID mismatch"
                       {:cid cid})))
           (let [page (ipld/decode bytes)
                 entries (get page "entries")]
             (when-not
              (and (= "kotobase/database-backup-inventory-page"
                      (get page "format"))
                   (= 1 (get page "version"))
                   (= (get descriptor "ordinal") (get page "ordinal"))
                   (= (get descriptor "count") (count entries))
                   (= (get descriptor "encoded-bytes")
                      (.-byteLength bytes))
                   (= (get descriptor "first") (first entries))
                   (= (get descriptor "last") (peek entries))
                   (every? valid-database-backup-entry? entries))
               (throw
                (ex-info "Invalid database backup inventory page"
                         {:cid cid :descriptor descriptor})))
             entries))))))

(defn- materialize-database-backup-pages!
  [e backup-bucket inventory]
  (reduce
   (fn [pending wave]
     (.then
      pending
      (fn [entries]
        (-> (js/Promise.all
             (clj->js
              (mapv #(load-database-backup-page!
                      e backup-bucket %)
                    wave)))
            (.then #(into entries (mapcat identity (array-seq %))))))))
   (js/Promise.resolve [])
   (partition-all 4 (get inventory "pages"))))

(defn- load-database-backup-inventory!
  [e backup-bucket inventory-cid]
  (-> (.get backup-bucket
            (database-backup-inventory-key e inventory-cid))
      (.then
       (fn [stored]
         (if-not stored
           (js/Promise.reject
            (ex-info "Database backup inventory not found"
                     {:cid inventory-cid}))
           (stored-bytes! stored))))
      (.then
       (fn [bytes]
         (when-not (= inventory-cid (str (ipld/cid bytes)))
           (throw
            (ex-info "Database backup inventory CID mismatch"
                     {:cid inventory-cid})))
         (let [inventory
               (validate-database-backup-inventory
                e inventory-cid (ipld/decode bytes))]
           (if (= 1 (get inventory "version"))
             inventory
             (-> (materialize-database-backup-pages!
                  e backup-bucket inventory)
                 (.then
                  (fn [entries]
                    (validate-materialized-database-backup-entries
                     inventory-cid inventory entries)
                    (assoc inventory "entries" entries))))))))))

(defn- restore-database-entries!
  [e primary backup-bucket inventory-cid entries]
  (reduce
   (fn [pending wave]
     (.then
      pending
      (fn [counts]
        (-> (js/Promise.all
             (clj->js
              (mapv
               (fn [entry]
                 (let [namespace (get entry "namespace")
                       cid (get entry "cid")]
                 (restore-one-gc-entry!
                  e primary backup-bucket
                  {:restored 0 :already-present 0}
                  {"key" (database-storage-key e namespace cid)
                   "content-cid" cid})))
               wave)))
            (.then
             (fn [wave-counts]
               (reduce
                (fn [total item]
                  (-> total
                      (update :restored + (:restored item))
                      (update :already-present + (:already-present item))))
                counts
                (array-seq wave-counts))))))))
   (js/Promise.resolve
    {:inventory inventory-cid :restored 0 :already-present 0})
   (partition-all 4 entries)))

(defn- publish-restored-head!
  [e target-db-id head-cid counts]
  (-> (get-head e target-db-id)
      (.then
       (fn [{current :value}]
         (cond
           (= current head-cid)
           (assoc counts :head-cid head-cid
                  :head-created? false :idempotent? true)

           current
           (js/Promise.reject
            (ex-info "Database restore target head exists"
                     {:target-db-id target-db-id
                      :actual current :expected head-cid}))

           :else
           (-> (cas-head! e target-db-id head-cid nil)
               (.then
                (fn [won?]
                  (if won?
                    (assoc counts :head-cid head-cid
                           :head-created? true :idempotent? false)
                    (-> (get-head e target-db-id)
                        (.then
                         (fn [{winner :value}]
                           (if (= winner head-cid)
                             (assoc counts :head-cid head-cid
                                    :head-created? false :idempotent? true)
                             (js/Promise.reject
                              (ex-info "Database restore head CAS fenced"
                                       {:target-db-id target-db-id
                                        :actual winner
                                        :expected head-cid})))))))))))))))

(defn- verify-restored-database!
  [e primary backup-bucket inventory-cid inventory target-db-id entries result]
  (-> (backup-database-storage-graph!
       e primary backup-bucket (get inventory "head-cid"))
      (.then
       (fn [reachable]
         (let [actual (mapv (fn [{:keys [namespace cid]}]
                              [namespace cid])
                            reachable)
               expected (mapv (fn [entry]
                                [(get entry "namespace") (get entry "cid")])
                              entries)]
         (when-not (= expected actual)
           (throw
            (ex-info "Restored database reachability mismatch"
                     {:inventory inventory-cid
                      :expected (count expected)
                      :actual (count actual)})))
         (assoc result
                :verified-reachable (count actual)
                :source-db-id (get inventory "db-id")
                :target-db-id target-db-id))))))

(defn restore-database!
  "Restore every immutable block from INVENTORY-CID and create TARGET-DB-ID's
  mutable head. An existing equal head is idempotent; a different head fences
  the restore and is never overwritten."
  [e inventory-cid target-db-id]
  (let [primary (env e "MERKLE_BUCKET")
        backup-bucket (gc-backup-bucket e)]
    (when-not (and (string? inventory-cid)
                   (re-matches #"b[a-z2-7]+" inventory-cid)
                   (string? target-db-id) (seq target-db-id))
      (throw (ex-info "Invalid database restore request"
                      {:inventory-cid inventory-cid
                       :target-db-id target-db-id})))
    (if-not (and primary backup-bucket)
      (js/Promise.reject
       (js/Error. "Database restore requires MERKLE_BUCKET and a backup bucket"))
      (-> (load-database-backup-inventory!
           e backup-bucket inventory-cid)
          (.then
           (fn [inventory]
             (let [head-cid (get inventory "head-cid")
                   entries (get inventory "entries")]
               (-> (restore-database-entries!
                    e primary backup-bucket inventory-cid entries)
                   (.then
                    #(publish-restored-head!
                      e target-db-id head-cid %))
                   (.then
                    #(verify-restored-database!
                      e primary backup-bucket inventory-cid inventory
                      target-db-id entries %))))))))))

(defn- delete-confirmed-inventory!
  [e bucket snapshot-before result first-candidates grace-ms now-ms]
  (-> (gc-root-snapshot! e bucket)
      (.then
       (fn [snapshot-after]
         (if (not= snapshot-before snapshot-after)
           (assoc result :deleted 0 :inventory-passes 1
                  :aborted :roots-changed)
           (-> (gc-audit! e bucket snapshot-after grace-ms now-ms)
               (.then
                (fn [{second-candidates :candidate-keys}]
                  (-> (gc-root-snapshot! e bucket)
                      (.then
                       (fn [snapshot-final]
                         (cond
                           (not= snapshot-after snapshot-final)
                           (assoc result :deleted 0 :inventory-passes 2
                                  :aborted :roots-changed)

                           (not= first-candidates second-candidates)
                           (assoc result :deleted 0 :inventory-passes 2
                                  :aborted :inventory-changed)

                           :else
                           (backup-and-delete-candidates!
                            e bucket result first-candidates now-ms)))))))))))))

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
                   (fn [{first-candidates :candidate-keys :as audit}]
                     (let [result (dissoc audit :candidate-keys)]
                       (if (and delete? (seq first-candidates))
                         (delete-confirmed-inventory!
                          e bucket snapshot-before result first-candidates
                          grace-ms now-ms)
                         (assoc result :deleted 0 :inventory-passes 1))))))
              (catch :default error
                (js/Promise.reject
                 (js/Error. (str "GC audit setup failed: " error))))))))
     (js/Promise.reject
      (js/Error. "Reachability GC currently requires an R2 listing binding")))))
