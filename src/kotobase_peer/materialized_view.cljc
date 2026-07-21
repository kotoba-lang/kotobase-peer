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
  [{:keys [view-id epoch entries block-rows source-manifest plan-cid sorted?]
    :or {block-rows 512}}]
  (when-not (and (integer? epoch) (not (neg? epoch)))
    (throw (ex-info "View epoch must be a non-negative integer" {:epoch epoch})))
  (when-not (and (integer? block-rows) (pos? block-rows))
    (throw (ex-info "View block rows must be positive" {:block-rows block-rows})))
  (let [row-seq (map (fn [{:keys [key value]}]
                       (when-not (string? key)
                         (throw (ex-info "View key must be a string" {:key key})))
                       {"key" key "value" value})
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
                      "count" (count rows)
                      "pack-cid" (ipld/link pack-cid)
                      "pack-bytes" (byte-count pack-bytes)
                      "blocks" descriptors}
                      source-manifest (assoc "source-manifest" (ipld/link source-manifest))
                      plan-cid (assoc "plan-cid" (ipld/link plan-cid)))
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

(defn range-query-plan
  "Compile a bounded browser query into declarative object Range GET effects."
  [{:keys [bundle lower upper limit]}]
  (let [descriptors (select-blocks bundle lower upper)
        pack-cid (ipld/link-cid (get bundle "pack-cid"))]
    {:view-id (get bundle "view-id")
     :epoch (get bundle "epoch")
     :lower lower :upper upper :limit limit
     :descriptors descriptors
     :estimated-requests (count descriptors)
     :estimated-bytes (reduce + (map #(get % "length") descriptors))
     :need (mapv #(object-range-get pack-cid (get % "offset") (get % "length"))
                 descriptors)}))

(defn decode-range
  "Verify and decode one independently addressed block returned by Range GET."
  [descriptor bytes]
  (let [expected (ipld/link-cid (get descriptor "cid"))
        actual (ipld/cid bytes)]
    (when-not (= expected actual)
      (throw (ex-info "Materialized view block CID mismatch"
                      {:expected expected :actual actual})))
    (ipld/decode bytes)))

(defn finish-range-query
  "Finish a query from RANGE-BYTES in descriptor order. This function is the
  pure browser/Wasm execution kernel; storage and fetch remain host effects."
  [plan range-bytes]
  (let [lower (:lower plan)
        upper (:upper plan)
        limit (or (:limit plan) #?(:clj Long/MAX_VALUE :cljs js/Number.MAX_SAFE_INTEGER))]
    (->> (map vector (:descriptors plan) range-bytes)
         (mapcat (fn [[descriptor bytes]]
                   (get (decode-range descriptor bytes) "rows")))
         (filter (fn [row]
                   (let [key (get row "key")]
                     (and (or (nil? lower) (not (neg? (compare key lower))))
                          (or (nil? upper) (not (pos? (compare key upper))))))))
         (take limit)
         (mapv #(get % "value")))))

(defn query-packed
  "In-memory oracle/benchmark helper using the same range plan and CID checks
  as a browser, but slicing a complete pack already supplied by the host."
  [bundle pack-bytes {:keys [lower upper limit]}]
  (let [plan (range-query-plan {:bundle bundle :lower lower :upper upper :limit limit})
        ranges (mapv #(slice-bytes pack-bytes (get % "offset") (get % "length"))
                     (:descriptors plan))]
    {:plan plan :values (finish-range-query plan ranges)}))
