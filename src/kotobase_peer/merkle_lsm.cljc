(ns kotobase-peer.merkle-lsm
  "Pure M1 kernel for ADR-2607201600.

  Values in this namespace are immutable data.  Encoding a run or manifest
  returns the bytes/CID plus declarative effects; it never performs storage or
  head mutation itself."
  (:require [clojure.string :as str]
            [ipld.core :as ipld]))

(def format-version 1)
(def indexes #{:eavt :aevt :avet :vaet})
(def ^:private max-safe-integer 9007199254740991)
(def ^:private integer-width 16)

(defn- zero-pad [n]
  (let [s (str n)]
    (str (apply str (repeat (max 0 (- integer-width (count s))) "0")) s)))

(defn- sortable-integer [n]
  (when-not (<= (- max-safe-integer) n max-safe-integer)
    (throw (ex-info "Merkle-LSM integers must fit the portable safe-integer range"
                    {:value n :max max-safe-integer})))
  ;; Both halves sort numerically: negatives occupy prefix 0 with their
  ;; magnitude reversed, then non-negative values occupy prefix 1.
  (if (neg? n)
    (str "0" (zero-pad (+ max-safe-integer n)))
    (str "1" (zero-pad n))))

(defn- component-text [x]
  (cond
    (ipld/link? x) (str "l" (ipld/link-cid x))
    (string? x)    (str "s" x)
    (keyword? x)   (str "k" (namespace x) "/" (name x))
    (integer? x)   (str "i" (sortable-integer x))
    (number? x)    (str "n" x)
    (boolean? x)   (if x "b1" "b0")
    (nil? x)       "z"
    :else          (str "p" (pr-str x))))

(defn- frame [s]
  (str (count s) ":" s))

(defn canonical-key
  "Canonical, unambiguous physical key for one index entry. Components are
  length-framed so embedded separators cannot collide. Epoch is reversed,
  making the newest version sort first for an equal logical key."
  [index tenant components epoch]
  (when-not (indexes index)
    (throw (ex-info "Unknown Merkle-LSM index" {:index index})))
  (when-not (and (integer? epoch) (<= 0 epoch max-safe-integer))
    (throw (ex-info "Merkle-LSM epoch must be a non-negative integer"
                    {:epoch epoch})))
  (str/join "|"
            (concat [(name index) (frame (str tenant))]
                    (map (comp frame component-text) components)
                    [(zero-pad (- max-safe-integer epoch))])))

(defn block-put [cid bytes]
  {:effect/type :block/put :cid cid :bytes bytes})

(defn block-get [cid]
  {:effect/type :block/get :cid cid})

(defn head-read [db-id]
  {:effect/type :head/read :db-id db-id})

(defn head-cas [db-id expected next]
  {:effect/type :head/cas :db-id db-id :expected expected :next next})

(defn cache-get [cid]
  {:effect/type :cache/get :cid cid})

(defn cache-put [cid bytes]
  {:effect/type :cache/put :cid cid :bytes bytes})

(defn- encoded [node]
  (let [bytes (ipld/encode node)
        cid (ipld/cid bytes)]
    {:node node :bytes bytes :cid cid :effects [(block-put cid bytes)]}))

(defn build-run
  "Build a canonical immutable sorted run.

  ENTRY input is {:components [...] :epoch n :op :assert|:retract :value v}.
  The returned value includes :node/:bytes/:cid and a single BlockPut effect."
  [index tenant entries]
  (let [rows (->> entries
                  (map (fn [{:keys [components epoch op value]}]
                         (when-not (#{:assert :retract} op)
                           (throw (ex-info "Merkle-LSM entry op must be :assert or :retract"
                                           {:op op})))
                         {"key" (canonical-key index tenant components epoch)
                          "components" (vec components)
                          "epoch" epoch
                          "op" (name op)
                          "value" value}))
                  (sort-by #(get % "key"))
                  vec)
        keys' (mapv #(get % "key") rows)
        first-components (->> rows
                              (map #(component-text (first (get % "components"))))
                              sort vec)
        component-min (first first-components)
        component-max (peek first-components)
        node (cond-> {"format" "kotobase/merkle-run"
                      "version" format-version
                      "index" (name index)
                      "tenant" (str tenant)
                      "count" (count rows)
                      "min-key" (first keys')
                      "max-key" (peek keys')
                      "rows" rows}
               component-min (assoc "first-component-min" component-min
                                    "first-component-max" component-max))]
    (cond-> (assoc (encoded node)
                   :index index :tenant (str tenant) :count (count rows)
                   :min-key (first keys') :max-key (peek keys'))
      component-min (assoc :first-component-min component-min
                           :first-component-max component-max))))

(defn- datom-entry [index epoch {:keys [e a v op] :or {op :assert}}]
  (let [components (case index
                     :eavt [e a v]
                     :aevt [a e v]
                     :avet [a v e]
                     :vaet [v a e])]
    {:components components :epoch epoch :op op :value v}))

(declare partition-logical-groups)

(defn build-run-ranges
  "Build deterministic, logical-key-aligned runs of at most TARGET-ROWS.
  A single hot logical key may exceed the target rather than being split."
  [index tenant target-rows entries]
  (when-not (and (integer? target-rows) (pos? target-rows))
    (throw (ex-info "Run target rows must be a positive integer"
                    {:target-rows target-rows})))
  (->> entries
       (sort-by (fn [{:keys [components epoch]}]
                  (canonical-key index tenant components epoch)))
       (partition-by :components)
       (partition-logical-groups target-rows)
       (mapv #(build-run index tenant (mapcat identity %)))))

(defn build-index-runs
  "Flush one immutable datom batch into covering index runs. DATOMS are
  {:e :a :v :op}; :op defaults to :assert. VAET is intentionally sparse and
  contains only datoms whose value is a genuine IPLD Link."
  [tenant epoch datoms]
  (let [datoms (vec datoms)
        general (fn [index]
                  (build-run index tenant (map #(datom-entry index epoch %) datoms)))
        refs (filter #(ipld/link? (:v %)) datoms)]
    (cond-> {:eavt (general :eavt)
             :aevt (general :aevt)
             :avet (general :avet)}
      (seq refs) (assoc :vaet
                        (build-run :vaet tenant
                                   (map #(datom-entry :vaet epoch %) refs))))))

(defn build-index-run-ranges
  "Flush one datom batch into bounded L0 ranges for every covering index."
  [tenant epoch target-rows datoms]
  (let [datoms (vec datoms)
        general (fn [index]
                  (build-run-ranges index tenant target-rows
                                    (map #(datom-entry index epoch %) datoms)))
        refs (filter #(ipld/link? (:v %)) datoms)]
    (cond-> {:eavt (general :eavt)
             :aevt (general :aevt)
             :avet (general :avet)}
      (seq refs) (assoc :vaet
                        (build-run-ranges
                         :vaet tenant target-rows
                         (map #(datom-entry :vaet epoch %) refs))))))

(defn run-ref
  "Manifest-safe metadata for a run returned by build-run."
  [{:keys [cid count min-key max-key first-component-min first-component-max]}]
  (cond-> {"cid" (ipld/link cid)
           "count" count
           "min-key" min-key
           "max-key" max-key}
    first-component-min
    (assoc "first-component-min" first-component-min
           "first-component-max" first-component-max)))

(defn select-run-refs-by-first-component
  "Select refs whose first-component range can contain a string PREFIX.
  Missing metadata is retained for compatibility with older manifests."
  [refs prefix]
  (if (empty? prefix)
    (vec refs)
    (let [minimum (component-text (str prefix))
          maximum (str minimum "\uffff")]
      (->> refs
           (filter (fn [ref]
                     (let [lo (get ref "first-component-min")
                           hi (get ref "first-component-max")]
                       (or (nil? lo) (nil? hi)
                           (and (not (neg? (compare hi minimum)))
                                (not (pos? (compare lo maximum))))))))
           vec))))

(defn overlapping-run-ranges
  "Partition run refs into deterministic, disjoint key-range tasks. Refs in a
  task overlap transitively and must be compacted together. Missing range
  metadata disables splitting so older manifests remain correctness-safe."
  [refs]
  (let [refs (vec refs)]
    (cond
      (empty? refs) []
      (some #(or (nil? (get % "min-key")) (nil? (get % "max-key"))) refs)
      [refs]
      :else
      (reduce (fn [ranges ref]
                (let [start (get ref "min-key")
                      end (get ref "max-key")]
                  (if-let [{range-end :max-key range-refs :refs} (peek ranges)]
                    (if (<= (compare start range-end) 0)
                      (conj (pop ranges)
                            {:min-key (:min-key (peek ranges))
                             :max-key (if (pos? (compare end range-end)) end range-end)
                             :refs (conj range-refs ref)})
                      (conj ranges {:min-key start :max-key end :refs [ref]}))
                    [{:min-key start :max-key end :refs [ref]}])))
              []
              (sort-by (juxt #(get % "min-key") #(get % "max-key")) refs)))))

(defn- normalize-levels [levels]
  (into (sorted-map)
        (map (fn [[level runs]]
               [(name level) (mapv #(if (and (map? %) (:cid %))
                                      (run-ref %)
                                      %)
                                   runs)]))
        levels))

(defn build-range-directory
  "Build an immutable checkpoint directory over compacted run refs. PREVIOUS
  points at an uncompacted manifest tail, or is nil for a full checkpoint."
  [{:keys [db-id epoch indexes previous]}]
  (let [node (cond->
              {"format" "kotobase/range-directory"
               "version" format-version
               "db-id" (str db-id)
               "epoch" epoch
               "indexes" (into
                          (sorted-map)
                          (map (fn [[index refs]]
                                 [(name index)
                                  (mapv #(if (and (map? %) (:cid %))
                                           (run-ref %) %)
                                        refs)]))
                          indexes)}
               previous (assoc "previous" (ipld/link previous)))]
    (assoc (encoded node) :db-id (str db-id) :epoch epoch)))

(defn range-directory-refs [directory index]
  (get-in directory ["indexes" (name index)] []))

(defn build-manifest
  "Build VersionManifest v1. INDEXES maps :eavt/:aevt/:avet/:vaet to level
  maps such as {:l0 [run] :l1 [run-ref]}. PREVIOUS is nil or a manifest CID."
  [{:keys [db-id epoch safe-epoch indexes previous statistics]
    :or {safe-epoch 0 indexes {} statistics {}}}]
  (when-not (and (integer? epoch) (not (neg? epoch)))
    (throw (ex-info "Manifest epoch must be a non-negative integer" {:epoch epoch})))
  (when-not (and (integer? safe-epoch) (<= 0 safe-epoch epoch))
    (throw (ex-info "Manifest safe epoch must be within [0, epoch]"
                    {:epoch epoch :safe-epoch safe-epoch})))
  (let [unknown (seq (remove kotobase-peer.merkle-lsm/indexes (keys indexes)))]
    (when unknown
      (throw (ex-info "Manifest contains unknown indexes" {:indexes unknown}))))
  (let [node (cond->
              {"format" "kotobase/version-manifest"
               "version" format-version
               "db-id" (str db-id)
               "epoch" epoch
               "safe-epoch" safe-epoch
               "indexes" (into (sorted-map)
                               (map (fn [[index levels]]
                                      [(name index) (normalize-levels levels)]))
                               indexes)
               "statistics" statistics}
               previous (assoc "previous" (ipld/link previous)))]
    (assoc (encoded node) :db-id (str db-id) :epoch epoch :safe-epoch safe-epoch)))

(defn publication-plan
  "Return declarative effects that durably put all blocks before publishing
  MANIFEST with a single compare-and-swap."
  [db-id expected runs manifest]
  {:result {:db-id (str db-id) :epoch (:epoch manifest) :manifest (:cid manifest)}
   :effects (vec (concat (mapcat :effects runs)
                         (:effects manifest)
                         [(head-cas db-id expected (:cid manifest))]))})

(defn flush-plan
  "M2 shadow-write vertical slice: turn a datom batch into immutable L0 runs,
  a VersionManifest, and ordered BlockPut...HeadCAS effects. No effect is
  executed here."
  [{:keys [db-id tenant epoch safe-epoch previous expected datoms statistics
           target-run-rows]
    :or {safe-epoch 0 statistics {} target-run-rows 4096}}]
  (let [runs-by-index (build-index-run-ranges
                       (or tenant db-id) epoch target-run-rows datoms)
        manifest (build-manifest
                  {:db-id db-id :epoch epoch :safe-epoch safe-epoch
                   :previous previous
                   :statistics (assoc statistics "l0-target-run-rows" target-run-rows)
                   :indexes (into {}
                                  (map (fn [[index runs]] [index {:l0 runs}]))
                                  runs-by-index)})
        runs (->> runs-by-index
                  (sort-by (comp name key))
                  (mapcat val)
                  vec)]
    (assoc (publication-plan db-id expected runs manifest)
           :runs runs-by-index
           :manifest manifest)))

(defn- run-rows [run]
  (get (:node run) "rows"))

(defn- logical-id [row]
  (get row "components"))

(defn- merge-sorted-row-seqs
  "Lazily merge canonical run rows without materializing the input set."
  [row-seqs]
  (lazy-seq
   (let [active (keep-indexed (fn [i rows]
                                (when-let [rows (seq rows)] [i rows]))
                              row-seqs)]
     (when (seq active)
       (let [[i rows] (first (sort-by (juxt #(get (first (second %)) "key") first)
                                      active))]
         (cons (first rows)
               (merge-sorted-row-seqs (assoc row-seqs i (next rows)))))))))

(defn- partition-logical-groups [target-rows groups]
  (lazy-seq
   (when-let [groups (seq groups)]
     (loop [remaining groups batch [] row-count 0]
       (if-let [group (first remaining)]
         (let [next-count (+ row-count (count group))]
           (if (and (seq batch) (> next-count target-rows))
             (cons batch (partition-logical-groups target-rows remaining))
             (recur (next remaining) (conj batch group) next-count)))
         (list batch))))))

(defn- retained-entries [safe-epoch runs]
  (->> runs
       (mapv #(seq (run-rows %)))
       merge-sorted-row-seqs
       (partition-by logical-id)
       (mapcat (fn [versions]
                 ;; Physical keys put the newest epoch first for one logical id.
                 (let [{newer true older false}
                       (group-by #(> (get % "epoch") safe-epoch) versions)]
                   (concat newer (take 1 older)))))
       (map (fn [row]
              {:components (get row "components")
               :epoch (get row "epoch")
               :op (keyword (get row "op"))
               :value (get row "value")}))))

(defn visible-rows
  "MVCC multi-run merge. For each logical key, select the newest version at
  or before QUERY-EPOCH. Tombstones suppress the key. Returned rows remain in
  canonical physical-key order."
  [runs query-epoch]
  (->> runs
       (mapcat run-rows)
       (filter #(<= (get % "epoch") query-epoch))
       (group-by logical-id)
       vals
       (keep (fn [versions]
               (let [row (apply max-key #(get % "epoch") versions)]
                 (when (= "assert" (get row "op")) row))))
       (sort-by #(get % "key"))
       vec))

(defn compact-runs
  "Range-independent M1 compaction primitive. Retains every version newer
  than SAFE-EPOCH plus the newest version at/before SAFE-EPOCH for each
  logical key, then emits one deterministic run. This preserves all snapshots
  >= safe-epoch while pruning older shadowed versions."
  [index tenant safe-epoch runs]
  (build-run index tenant (retained-entries safe-epoch runs)))

(defn compact-runs-partitioned
  "Merge sorted RUNS with bounded working input and emit deterministic L1 runs
  of at most TARGET-ROWS. Logical keys are never split between output runs, so
  a hot key can exceed the target; all ordinary ranges remain bounded."
  [index tenant safe-epoch target-rows runs]
  (when-not (and (integer? target-rows) (pos? target-rows))
    (throw (ex-info "Compaction target rows must be a positive integer"
                    {:target-rows target-rows})))
  (->> (retained-entries safe-epoch runs)
       (partition-by :components)
       (partition-logical-groups target-rows)
       (mapv (fn [groups]
               (build-run index tenant (mapcat identity groups))))))

(defn query-plan
  "Pure first read step: pin the database by asking the host for its head."
  [db-id query]
  {:state {:phase :read-head :db-id (str db-id) :query query}
   :need [(head-read db-id)]})

(defn linked-cids
  "Return every IPLD Link reachable directly from decoded VALUE. Used by host
  GC walkers; traversal stays pure and independent of an object provider."
  [value]
  (cond
    (ipld/link? value) #{(ipld/link-cid value)}
    (map? value) (into #{} (mapcat linked-cids) (vals value))
    (sequential? value) (into #{} (mapcat linked-cids) value)
    :else #{}))
