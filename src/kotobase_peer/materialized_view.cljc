(ns kotobase-peer.materialized-view
  "Pure, browser-oriented IPLD materialized views.

  Logical blocks remain independently content-addressed, while many blocks are
  packed into one large object.  A small immutable query bundle maps key ranges
  to byte ranges, so a browser can answer a bounded query with one bundle fetch
  and one HTTP Range request without a local database."
  (:require [ipld.core :as ipld]))

(def format-version 1)

(defn- byte-count [bytes]
  #?(:clj (alength ^bytes bytes)
     :cljs (.-byteLength bytes)))

(defn- concat-bytes [parts]
  #?(:clj
     (let [size (reduce + (map byte-count parts))
           out (byte-array size)]
       (loop [offset 0, parts (seq parts)]
         (if-let [part (first parts)]
           (let [length (byte-count part)]
             (System/arraycopy ^bytes part 0 out offset length)
             (recur (+ offset length) (next parts)))
           out)))
     :cljs
     (let [size (reduce + (map byte-count parts))
           out (js/Uint8Array. size)]
       (loop [offset 0, parts (seq parts)]
         (if-let [part (first parts)]
           (do (.set out part offset)
               (recur (+ offset (byte-count part)) (next parts)))
           out)))))

(defn- slice-bytes [bytes offset length]
  #?(:clj (java.util.Arrays/copyOfRange ^bytes bytes offset (+ offset length))
     :cljs (.slice bytes offset (+ offset length))))

(defn view-key
  "Portable ordered key for materialized views. Components are length framed;
  callers should put their most selective/range component first."
  [components]
  (apply str (map (fn [component]
                    (let [s (pr-str component)]
                      (str (count s) ":" s)))
                  components)))

(defn object-put [cid bytes]
  {:effect/type :object/put :cid cid :bytes bytes})

(defn object-range-get [cid offset length]
  {:effect/type :object/range-get :cid cid :offset offset :length length})

(defn- encode-block [view-id epoch rows]
  (let [node {"format" "kotobase/materialized-view-block"
              "version" format-version
              "view-id" (str view-id)
              "epoch" epoch
              "count" (count rows)
              "min-key" (get (first rows) "key")
              "max-key" (get (peek rows) "key")
              "rows" rows}
        bytes (ipld/encode node)]
    {:node node :bytes bytes :cid (ipld/cid bytes)}))

(defn build-view
  "Build a deterministic packed materialized view.

  ENTRIES are {:key string :value IPLD-value}; duplicate keys are allowed and
  retain deterministic value ordering. BLOCK-ROWS controls the browser read
  granularity. The result contains one :object/put for the packed bytes and one
  :block/put for the small query bundle."
  [{:keys [view-id epoch entries block-rows source-manifest plan-cid sorted?
           previous-bundle mode]
    :or {block-rows 512}}]
  (when-not (and (integer? epoch) (not (neg? epoch)))
    (throw (ex-info "View epoch must be a non-negative integer" {:epoch epoch})))
  (when-not (and (integer? block-rows) (pos? block-rows))
    (throw (ex-info "View block rows must be positive" {:block-rows block-rows})))
  (let [row-seq (map (fn [{:keys [key value op] :or {op :assert}}]
                       (when-not (string? key)
                         (throw (ex-info "View key must be a string" {:key key})))
                       (when-not (#{:assert :retract} op)
                         (throw (ex-info "View op must be :assert or :retract" {:op op})))
                       {"key" key "op" (name op) "value" value})
                     entries)
        rows (if sorted?
               (let [rows (vec row-seq)]
                 (when-not (every? #(not (pos? %))
                                   (map #(compare (get %1 "key") (get %2 "key"))
                                        rows (next rows)))
                   (throw (ex-info "sorted? view entries are not key ordered" {})))
                 rows)
               (->> row-seq
                    (sort-by (juxt #(get % "key") #(pr-str (get % "value"))))
                    vec))
        blocks (mapv #(encode-block view-id epoch (vec %))
                     (partition-all block-rows rows))
        pack-bytes (concat-bytes (map :bytes blocks))
        pack-cid (ipld/cid pack-bytes)
        descriptors (loop [offset 0, blocks blocks, result []]
                      (if-let [block (first blocks)]
                        (let [length (byte-count (:bytes block))]
                          (recur (+ offset length) (next blocks)
                                 (conj result
                                       {"cid" (ipld/link (:cid block))
                                        "offset" offset
                                        "length" length
                                        "count" (get (:node block) "count")
                                        "min-key" (get (:node block) "min-key")
                                        "max-key" (get (:node block) "max-key")})))
                        result))
        bundle-node (cond->
                     {"format" "kotobase/query-bundle"
                      "version" format-version
                      "view-id" (str view-id)
                      "epoch" epoch
                      "mode" (name (or mode :base))
                      "count" (count rows)
                      "pack-cid" (ipld/link pack-cid)
                      "pack-bytes" (byte-count pack-bytes)
                      "blocks" descriptors}
                      source-manifest (assoc "source-manifest" (ipld/link source-manifest))
                      plan-cid (assoc "plan-cid" (ipld/link plan-cid))
                      previous-bundle (assoc "previous-bundle" (ipld/link previous-bundle)))
        bundle-bytes (ipld/encode bundle-node)
        bundle-cid (ipld/cid bundle-bytes)]
    {:view-id (str view-id)
     :epoch epoch
     :count (count rows)
     :blocks blocks
     :pack-cid pack-cid
     :pack-bytes pack-bytes
     :bundle {:node bundle-node :bytes bundle-bytes :cid bundle-cid}
     :effects [(object-put pack-cid pack-bytes)
               {:effect/type :block/put :cid bundle-cid :bytes bundle-bytes}]}))

(defn build-view-delta
  "Build one incremental materialized-view L0 pack. CHANGES are
  {:key string :value value :op :assert|:retract}. The previous bundle link
  pins the older view generation; newest-first merge applies tombstones."
  [{:keys [view-id epoch changes previous-bundle block-rows source-manifest plan-cid]}]
  (when-not previous-bundle
    (throw (ex-info "View delta requires a previous bundle CID" {})))
  (doseq [{:keys [op]} changes]
    (when-not (#{:assert :retract} op)
      (throw (ex-info "View delta op must be :assert or :retract" {:op op}))))
  (build-view {:view-id view-id :epoch epoch :entries changes
               :block-rows (or block-rows 512)
               :source-manifest source-manifest :plan-cid plan-cid
               :previous-bundle previous-bundle :mode :delta}))

(defn build-datom-projection
  "Bridge the peer's existing RisingWave-style `view-rows` result into a
  browser-addressable packed view. Retractions are absent from current-state
  projections; callers pass the pinned source manifest/epoch explicitly."
  [{:keys [view-id epoch rows block-rows source-manifest plan-cid]}]
  (build-view
   {:view-id view-id
    :epoch epoch
    :block-rows (or block-rows 512)
    :source-manifest source-manifest
    :plan-cid plan-cid
    :entries (keep (fn [{:keys [e a v_edn added] :or {added true}}]
                     (when added
                       {:key (view-key [e a])
                        :value {"e" e "a" a "v-edn" v_edn}}))
                   rows)}))

(defn- overlaps? [descriptor lower upper]
  (and (or (nil? lower) (not (neg? (compare (get descriptor "max-key") lower))))
       (or (nil? upper) (not (pos? (compare (get descriptor "min-key") upper))))))

(defn select-blocks
  "Use only bundle metadata to select blocks overlapping inclusive bounds."
  [bundle lower upper]
  (let [blocks (vec (get bundle "blocks"))
        ;; Sparse bundle indexes are ordered. Binary seek prevents browser
        ;; point queries from scanning metadata for every partition.
        start (if (or (nil? lower) (empty? blocks))
                0
                (loop [lo 0 hi (count blocks)]
                  (if (< lo hi)
                    (let [mid (quot (+ lo hi) 2)]
                      (if (neg? (compare (get (nth blocks mid) "max-key") lower))
                        (recur (inc mid) hi)
                        (recur lo mid)))
                    lo)))]
    (->> (subvec blocks start)
         (take-while #(or (nil? upper)
                          (not (pos? (compare (get % "min-key") upper)))))
         (filter #(overlaps? % lower upper))
         vec)))

(defn coalesce-block-ranges
  "Coalesce physically adjacent logical blocks into bounded object fetches.
  Each logical descriptor remains present for independent CID verification."
  [descriptors max-range-bytes]
  (when-not (and (integer? max-range-bytes) (pos? max-range-bytes))
    (throw (ex-info "Maximum coalesced range must be positive"
                    {:max-range-bytes max-range-bytes})))
  (reduce
   (fn [fetches descriptor]
     (let [offset (get descriptor "offset")
           length (get descriptor "length")]
       (if-let [fetch (peek fetches)]
         (if (and (= offset (+ (:offset fetch) (:length fetch)))
                  (<= (+ (:length fetch) length) max-range-bytes))
           (conj (pop fetches)
                 (-> fetch
                     (update :length + length)
                     (update :descriptors conj descriptor)))
           (conj fetches {:offset offset :length length
                          :descriptors [descriptor]}))
         [{:offset offset :length length :descriptors [descriptor]}])))
   [] descriptors))

(defn range-query-plan
  "Compile a bounded browser query into declarative object Range GET effects."
  [{:keys [bundle lower upper limit max-range-bytes]
    :or {max-range-bytes 1048576}}]
  (let [descriptors (select-blocks bundle lower upper)
        fetches (coalesce-block-ranges descriptors max-range-bytes)
        pack-cid (ipld/link-cid (get bundle "pack-cid"))]
    {:view-id (get bundle "view-id")
     :epoch (get bundle "epoch")
     :lower lower :upper upper :limit limit
     :descriptors descriptors
     :fetches fetches
     :estimated-requests (count fetches)
     :estimated-bytes (reduce + (map :length fetches))
     :need (mapv #(object-range-get pack-cid (:offset %) (:length %)) fetches)}))

(defn decode-range
  "Verify and decode one independently addressed block returned by Range GET."
  [descriptor bytes]
  (let [expected (ipld/link-cid (get descriptor "cid"))
        actual (ipld/cid bytes)]
    (when-not (= expected actual)
      (throw (ex-info "Materialized view block CID mismatch"
                      {:expected expected :actual actual})))
    (ipld/decode bytes)))

(defn finish-range-rows
  "Verify/decode RANGE-BYTES and return bounded physical rows, including
  tombstones. Delta-chain merge consumes this lower-level browser primitive."
  [plan range-bytes]
  (let [lower (:lower plan)
        upper (:upper plan)
        logical-ranges
        (mapcat
         (fn [fetch bytes]
           (when-not (= (:length fetch) (byte-count bytes))
             (throw (ex-info "Materialized view range length mismatch"
                             {:expected (:length fetch)
                              :actual (byte-count bytes)})))
           (map (fn [descriptor]
                  [descriptor
                   (slice-bytes bytes
                                (- (get descriptor "offset") (:offset fetch))
                                (get descriptor "length"))])
                (:descriptors fetch)))
         (:fetches plan) range-bytes)]
    (->> logical-ranges
         (mapcat (fn [[descriptor bytes]]
                   (get (decode-range descriptor bytes) "rows")))
         (filter (fn [row]
                   (let [key (get row "key")]
                     (and (or (nil? lower) (not (neg? (compare key lower))))
                          (or (nil? upper) (not (pos? (compare key upper))))))))
         vec)))

(defn finish-range-query
  "Finish one base-view query. This is the pure browser/Wasm execution kernel;
  storage and fetch remain host effects."
  [plan range-bytes]
  (let [limit (or (:limit plan) #?(:clj Long/MAX_VALUE :cljs js/Number.MAX_SAFE_INTEGER))]
    (->> (finish-range-rows plan range-bytes)
         (filter #(= "assert" (get % "op")))
         (take limit)
         (mapv #(get % "value")))))

(defn query-packed
  "In-memory oracle/benchmark helper using the same range plan and CID checks
  as a browser, but slicing a complete pack already supplied by the host."
  [bundle pack-bytes {:keys [lower upper limit]}]
  (let [plan (range-query-plan {:bundle bundle :lower lower :upper upper :limit limit})
        ranges (mapv #(slice-bytes pack-bytes (:offset %) (:length %))
                     (:fetches plan))]
    {:plan plan :values (finish-range-query plan ranges)}))

(defn range-query-plan-chain
  "Plan the same bounded query against BUNDLES ordered newest first. Fetching
  the small linked bundles is a host concern; all packed data reads stay
  explicit and bounded here."
  [bundles {:keys [lower upper limit]}]
  (let [plans (mapv #(range-query-plan {:bundle % :lower lower :upper upper}) bundles)]
    {:plans plans :lower lower :upper upper :limit limit
     :estimated-requests (reduce + (map :estimated-requests plans))
     :estimated-bytes (reduce + (map :estimated-bytes plans))
     :need (vec (mapcat :need plans))}))

(defn- newest-chain-rows [plans range-bytes-by-plan]
  (let [rows (mapcat (fn [plan range-bytes]
                       (finish-range-rows plan range-bytes))
                     plans range-bytes-by-plan)]
    (->> rows
         (reduce (fn [result row]
                   (let [key (get row "key")]
                     (if (contains? result key) result (assoc result key row))))
                 {})
         (sort-by key)
         (mapv val))))

(defn finish-range-query-chain
  "Newest-first MVCC merge for materialized-view delta packs. The first row
  for a key wins; a retract tombstone suppresses older assertions."
  [chain-plan range-bytes-by-plan]
  (let [limit (or (:limit chain-plan)
                  #?(:clj Long/MAX_VALUE :cljs js/Number.MAX_SAFE_INTEGER))]
    (->> (newest-chain-rows (:plans chain-plan) range-bytes-by-plan)
         (keep (fn [row]
                 (when (= "assert" (get row "op")) (get row "value"))))
         (take limit)
         vec)))

(defn query-packed-chain
  "In-memory oracle for a newest-first sequence of {:bundle :pack-bytes}."
  [generations query]
  (let [bundles (mapv :bundle generations)
        plan (range-query-plan-chain bundles query)
        range-groups
        (mapv (fn [generation generation-plan]
                (mapv #(slice-bytes (:pack-bytes generation)
                                    (:offset %) (:length %))
                      (:fetches generation-plan)))
              generations (:plans plan))]
    {:plan plan :values (finish-range-query-chain plan range-groups)}))

(defn compact-packed-chain
  "Compact complete newest-first packed generations into one deterministic
  base view. Intended for host/background compaction; memory-bounded remote
  streaming is a subsequent host executor concern."
  [{:keys [view-id epoch generations block-rows source-manifest plan-cid]}]
  (let [chain-plan (range-query-plan-chain (mapv :bundle generations) {})
        range-groups
        (mapv (fn [generation generation-plan]
                (mapv #(slice-bytes (:pack-bytes generation)
                                    (:offset %) (:length %))
                      (:fetches generation-plan)))
              generations (:plans chain-plan))
        entries (->> (newest-chain-rows (:plans chain-plan) range-groups)
                     (keep (fn [row]
                             (when (= "assert" (get row "op"))
                               {:key (get row "key") :value (get row "value")})))
                     vec)]
    (build-view {:view-id view-id :epoch epoch :entries entries
                 :block-rows (or block-rows 512) :sorted? true
                 :source-manifest source-manifest :plan-cid plan-cid})))
