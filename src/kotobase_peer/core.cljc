;; kotobase-peer.core — the kotobase peer library (Datomic's own term for
;; the transact/q/pull library an application embeds; this repo was named
;; kotobase-engine until ADR-2607050700 -- "kotobase" itself was already
;; taken by the client-side IStore port, kotoba-lang/kotobase), assembled
;; from the already-landed Wave 1-3 primitives (ADR-2607010930 Phase 6 +
;; ADR-2607022600): arrangement (hot 4-covering-index Arrangement + query
;; routing + per-commit snapshot -- formerly two repos, quad-store and kqe,
;; merged into one per ADR-2607050700), commit-dag (chain/verify). This is
;; the piece
;; that implements the `ai.gftd.apps.kotobase.datomic.*` surface
;; (transact/datoms/q/pull) end to end, backed by content-addressed,
;; verifiable persistence, in CLJC.
;;
;; ADR-2607032430 (log-structured write path): the original `commit!` snapshotted
;; a full hot db into 4 fresh prolly-trees on EVERY write -- O(graph). Confirmed
;; live 2026-07-03 as the root cause of a CF Worker CPU-limit collapse (error 1102)
;; during a 321-actor mass write. `commit!` is now a novelty-log APPEND (O(tx)):
;; commit-dag's opaque `state` carries `{"indexed" <snapshot Link>|nil "novelty"
;; [<tx-block Link> ...]}`; a write puts one small tx block and prepends its link,
;; touching neither a prolly-tree nor the graph itself. `fold!` compacts novelty
;; into a fresh `indexed` snapshot (the same O(graph) work the old `commit!` always
;; did, now amortized over however many writes accumulated first) and is content-
;; addressed -- deterministic, safe for any writer (server cron, a browser at idle)
;; to run redundantly. Reads go through `hot-datoms` (indexed snapshot, range-pruned,
;; + novelty merged in memory) instead of `hydrate-db` + `datoms` wherever a full hot
;; db value isn't actually needed.
;;
;; Composition decision (resolves the ADR-2607022600 "quad-store/commit! and
;; commit-dag are two unmerged implementations" gap -- quoting that ADR's own
;; wording; the repo it refers to is arrangement post-ADR-2607050700 rename):
;; arrangement/commit! is called
;; with `prev` always nil here — it is used ONLY to snapshot the 4 indexes into
;; content-addressed prolly-trees and return one CID for that snapshot. Chain
;; history, `:seq`, and tamper/gap verification are commit-dag's job, wrapping
;; the {indexed, novelty} state map as its opaque `state`. Neither library is
;; modified; this namespace is the seam between them.
(ns kotobase-peer.core
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])   ; both expose read-string over EDN
            [ipld.core :as ipld]
            [prolly-tree.core :as pt]
            [arrangement.core :as qs]
            [arrangement.query :as kqe]
            [commit-dag.core :as cd]
            [datom.core :as dc]))   ; canonical datom model (kotoba : kotobase = Clojure : Datomic)

(defn empty-db [] (qs/empty-db))

(defn- ->quad-value
  "Coerce one quad position: an `ipld.core/Link` passes through unchanged (so
   it survives to `assert-quad`'s `ref?` check as a genuine Link, ADR-2607050200);
   anything else stringifies as before (general typed-value support beyond
   Link -- int/bytes/bool/list/map -- remains a follow-up, ADR-2607023200 §6-5)."
  [v] (if (ipld/link? v) v (str v)))

(defn- ->quad
  "Coerce one tx-data item to a `{:s :p :o}` quad (`:s`/`:p` always coerced to
   strings; `:o` passes an `ipld.core/Link` through unchanged, see
   `->quad-value`). Accepts a `{:s :p :o}` map, `[:db/add e a v]`, or a bare
   `[e a v]` triple."
  [item]
  (cond
    (and (map? item) (contains? item :s) (contains? item :p) (contains? item :o))
    {:s (str (:s item)) :p (str (:p item)) :o (->quad-value (:o item))}

    (and (vector? item) (= 4 (count item)) (= :db/add (first item)))
    (let [[_ e a v] item] {:s (str e) :p (str a) :o (->quad-value v)})

    (and (sequential? item) (= 3 (count item)))
    (let [[e a v] item] {:s (str e) :p (str a) :o (->quad-value v)})

    :else
    (throw (ex-info "kotobase-peer: unrecognized tx-data item"
                     {:item item}))))

(defn transact
  "Apply `tx-data` (a seq of `{:s :p :o}` quads, `[:db/add e a v]`, or `[e a v]`
   triples) to `db`. Returns the new (immutable) db value. `ref?` is passed
   through to `arrangement.core/assert-quad` for reverse-reference indexing
   (default `ipld.core/link?`, ADR-2607050200 -- a value asserted as a Link is
   automatically reverse-indexed, see `refs`).

   A pure hot-db utility, independent of persistence strategy -- used
   internally by `fold!` (via `hydrate-db`'s reduce, not this fn directly) and
   available for anyone assembling/inspecting a hot db value directly (tests,
   migration/backfill tooling). The write path proper is `commit!`, below."
  ([db tx-data] (transact db tx-data ipld/link?))
  ([db tx-data ref?]
   (reduce (fn [db item] (qs/assert-quad db (->quad item) ref?))
           db tx-data)))

;; ── datafication via the canonical datom model (datom-clj) ───────────────────
;; kotoba : kotobase = Clojure : Datomic (ADR-2607032500). An entity tx-map is
;; turned into `[e a v]` datoms by `datom.core/eavt` — kotoba's shared datom
;; representation, the same `[e a v]` `kotoba.kgraph` (the language's in-mem view)
;; speaks and the same shape `transact`/`->quad` already accept. The kotobase
;; datom DATABASE thus consumes the language's datom model instead of a private
;; entity→quad reimplementation.

(defn tx-map->datoms
  "Datafy one Datomic-style entity tx-map `{:db/id e :ns/a v …}` → `[e a v]`
   datoms (`datom.core/eavt`)."
  [ent]
  (dc/eavt ent))

(defn entities->datoms
  "Flatten a seq of entity tx-maps into `[e a v]` datoms (`datom.core/eavt`)."
  [entities]
  (vec (mapcat dc/eavt entities)))

(defn transact-tx
  "Transact a seq of entity tx-maps into `db`: datafy through the canonical datom
   model (`entities->datoms`), then `transact`. The peer library's entity-transaction
   expressed in kotoba's datom representation."
  ([db entities] (transact-tx db entities ipld/link?))
  ([db entities ref?] (transact db (entities->datoms entities) ref?)))

(defn- v->edn
  "Serialize an arrangement value to the EDN string the kotobase wire uses -- a
   Link renders as `qs/link->edn`'s `[\"ipld/link\" cid]` form (readable, no
   custom reader needed), anything else pr-str's directly as before. Matches
   the shape the pre-existing DataScript-backed `kotobase.engine` (app-aozora)
   produced, for wire compatibility."
  [v] (pr-str (qs/link->edn v)))

(defn datoms
  "`datomic.datoms`-shaped rows `{:e :a :v_edn :added}`, AppView-scan compatible
   (same wire shape as the DataScript-backed `kotobase.engine` this replaces).

   1-arg → whole-db (:eavt full scan, unchanged). 2-arg honors an optional
   `{:index :components :limit}` SERVER-SIDE via arrangement's index accessors, so
   a filtered read (e.g. getBackup by entity, or by [attr value]) materialises
   ONLY the matching entity/attr instead of the whole graph — the root fix for
   the deployed wasm worker ignoring index/components_edn/limit and rehydrating
   the full graph per read (ADR-2607022330 addendum 2).

   `:index` ∈ #{:eavt :aevt :avet} (default :eavt); `:components` is an ordered
   prefix in that index's key order (:eavt=[e a v], :aevt/:avet=[a … ]); values
   are the opaque strings arrangement stores (the PDS's :db/id for e, `:ns/attr`
   for a, the stored value string for v). `:limit` caps the row count."
  ([db] (datoms db nil))
  ([db {:keys [index components limit]}]
   (let [[c0 c1] components
         triples
         (case (or index :eavt)
           ;; EAVT — key order [e a v]
           :eavt (cond
                   (and c0 c1) (for [v (get (qs/entity-attrs db c0) c1 #{})] [c0 c1 v])
                   c0          (for [[a vs] (qs/entity-attrs db c0), v vs]   [c0 a v])
                   :else       (for [[e pm] (:spo db), [a vs] pm, v vs]      [e a v]))
           ;; AEVT — key order [a e v]
           :aevt (cond
                   c0    (for [[e vs] (qs/by-predicate db c0), v vs]   [e c0 v])
                   :else (for [[a em] (:pso db), [e vs] em, v vs]      [e a v]))
           ;; AVET — key order [a v e]; [a v] is arrangement's point lookup
           :avet (cond
                   (and c0 c1) (for [e (qs/by-predicate-value db c0 c1)] [e c0 c1])
                   c0          (for [[e vs] (qs/by-predicate db c0), v vs] [e c0 v])
                   :else       (for [[a em] (:pso db), [e vs] em, v vs]    [e a v])))
         triples (cond->> triples limit (take limit))]
     (vec (for [[e a v] triples] {:e e :a a :v_edn (v->edn v) :added true})))))

(defn q
  "`datomic.q`-equivalent: `pattern` is `[s p o]` (nil = wildcard), routed to
   the matching index via `arrangement.query`. Returns a set of `{:s :p :o}`
   quads. NOTE: triple-pattern only — multi-clause join / recursive-rule
   fixpoint is not implemented (`arrangement.query`'s own documented scope),
   see README.

   `visible?` is REQUIRED, cascaded straight from `arrangement.query/query`
   (ADR-2607050500: Query is a first-class effect all the way up this
   stack, not just at arrangement's layer) -- pass `(constantly true)` to
   see everything, as an explicit choice."
  [db pattern visible?]
  (kqe/query db pattern visible?))

(defn pull
  "`datomic.pull`-equivalent for entity `s`: `{p #{o...}}` (arrangement has no
   cardinality tracking, so every attribute is multi-valued — a caller
   expecting single-valued Datomic pull semantics must pick e.g. `first`)."
  [db s]
  (qs/entity-attrs db s))

(defn refs
  "`{p #{s...}}`: every entity `s` that references `ref` via predicate `p`
   (VAET-style reverse lookup, ADR-2607050200). Only populated for quads
   whose value was asserted as an `ipld.core/Link` — see `transact`'s default
   `ref?`. `ref` is the same Link value the referencing quad's `:o` was."
  [db ref]
  (qs/refs-to db ref))

;; ── persistence: content-addressed, chained, verifiable ─────────────────────
;;
;; state shape (opaque to commit-dag; this peer library's own convention, encoded
;; with STRING keys per the codebase's IPLD-node convention):
;;   {"indexed" Link|nil    -- the last-folded arrangement snapshot CID
;;    "novelty" [Link ...]} -- tx-block CIDs appended since, oldest first
;;
;; Pre-D1 chains store a BARE snapshot Link as `state` (the original commit!,
;; now `snapshot!`) -- `normalize-state` reads that as the D1 empty-novelty
;; equivalent, so existing chains (incl. every actor already live on
;; pds.aozora.app before this landed) keep reading and writing correctly with
;; zero migration step.

(defn snapshot!
  "Full O(graph) commit: snapshot `db`'s 4 indexes into content-addressed
   prolly-trees (`arrangement.core/commit!`, always with `prev` nil) and
   append that snapshot CID (as `{\"indexed\" snap \"novelty\" []}`) onto the
   commit-dag chain rooted at `prev-chain-cid`. This is the SAME expensive
   rebuild the pre-D1 `commit!` did on every write; it now exists only as a
   primitive `fold!` calls internally, plus a one-shot cold-start entry point
   for callers that already have a fully materialized hot `db` (backfill /
   migration tooling, tests). Most write paths want `commit!`, below."
  [put! get-fn db prev-chain-cid]
  (let [snapshot-cid (qs/commit! put! db nil qs/current-schema-version)]
    (cd/commit! put! get-fn
                {"indexed" (ipld/link snapshot-cid) "novelty" []}
                prev-chain-cid)))

(defn- normalize-state [state]
  (cond
    (map? state)       state
    (ipld/link? state) {"indexed" state "novelty" []}
    (nil? state)       {"indexed" nil "novelty" []}
    :else (throw (ex-info "kotobase-peer: unrecognized commit state shape"
                          {:state state}))))

(defn- state-at
  "The normalized `{\"indexed\" ... \"novelty\" ...}` state at `chain-cid`
   (an O(1) head fetch since commit-dag/head, ADR-2607032430's other half).
   nil chain-cid (no prior commit) → the empty state."
  [get-fn chain-cid]
  (if (nil? chain-cid)
    {"indexed" nil "novelty" []}
    (normalize-state (:state (cd/head get-fn chain-cid)))))

(defn- indexed-cid [state] (some-> (get state "indexed") ipld/link-cid))
(defn- novelty-cids [state] (mapv ipld/link-cid (get state "novelty" [])))

(defn- quad->wire [{:keys [s p o]}] {"s" s "p" p "o" o})
(defn- wire->quad [{:strs [s p o]}] {:s s :p p :o o})

(defn- put-tx-block! [put! quads]
  (ipld/put-node! put! {"quads" (mapv quad->wire quads)}))

(defn- read-tx-block [get-fn tx-cid]
  (mapv wire->quad (get (ipld/get-node get-fn tx-cid) "quads")))

(defn commit!
  "THE write path (ADR-2607032430 D1): append `tx-data` as one novelty tx
   block. O(|tx-data|) — reads only the previous state's {indexed, novelty}
   LINKS (one O(1) head fetch via commit-dag, never the graph itself) and
   writes one new small tx block plus one new commit-dag entry. Touches no
   prolly-tree, rehydrates nothing. `tx-data` accepts the same shapes
   `transact` does (quad maps, `[:db/add e a v]`, bare `[e a v]` triples).

   This REPLACES the pre-D1 `commit!` (now `snapshot!`), which took an
   already-hydrated hot db and paid a full O(graph) rebuild on every call —
   confirmed live 2026-07-03 as the root cause of a CF Worker CPU-limit
   collapse (error 1102) partway through a 321-actor mass write. Call
   `fold!` periodically (see `should-fold?`) to bound novelty size and keep
   `hot-datoms` reads fast; correctness never depends on folding promptly."
  [put! get-fn tx-data prev-chain-cid]
  (let [quads (mapv ->quad tx-data)
        state (state-at get-fn prev-chain-cid)
        tx-cid (put-tx-block! put! quads)
        new-state (update state "novelty" (fnil conj []) (ipld/link tx-cid))]
    (cd/commit! put! get-fn new-state prev-chain-cid)))

(defn novelty-size
  "How many not-yet-folded tx blocks sit on `chain-cid`'s current state —
   the `fold!` trigger signal."
  [get-fn chain-cid]
  (count (get (state-at get-fn chain-cid) "novelty" [])))

(def default-fold-threshold
  "novelty length `should-fold?` flags for compaction at. Tunes the
   amortization: a fold costs O(graph_shard) once every `default-fold-
   threshold` writes, so write-heavy shards fold more often in absolute
   time but pay the SAME amortized cost per write. A shard's own write
   volume and latency budget should drive the real value in production;
   this is a reasonable engine-level default, not a tuned constant."
  64)

(defn should-fold?
  ([get-fn chain-cid] (should-fold? get-fn chain-cid default-fold-threshold))
  ([get-fn chain-cid threshold]
   (>= (novelty-size get-fn chain-cid) threshold)))

(defn chain
  "Full commit history rooted at `chain-cid`, oldest first. Each entry's
   `:state` is the `{indexed novelty}` map (or, for a pre-D1 commit, the
   bare snapshot Link `normalize-state` would expand — `chain`/`head` return
   the RAW state as commit-dag stored it; callers that need the normalized
   shape go through `state-at` / `hot-datoms` / `latest-snapshot-cid`)."
  [get-fn chain-cid]
  (cd/chain get-fn chain-cid))

(defn head
  "The commit-dag entry AT `chain-cid` (O(1) — see commit-dag/head). Returns
   the RAW `:state` (not normalize-state-expanded); see `chain`'s docstring."
  [get-fn chain-cid]
  (cd/head get-fn chain-cid))

(defn latest-snapshot-cid
  "The INDEXED arrangement snapshot CID as of `chain-cid`'s current state —
   the last fold point, NOT including any pending novelty. nil when the
   chain is empty OR when nothing has been folded yet (an all-novelty chain,
   the common case for a fresh, low-write-volume shard between folds — this
   is NOT the same as \"no data\"; use `hot-datoms`/`novelty-size` to check
   for pending unfolded writes, not this fn alone, before deciding a graph
   is empty)."
  [get-fn chain-cid]
  (when chain-cid (indexed-cid (state-at get-fn chain-cid))))

;; ── cold read: filtered datoms straight from a persisted snapshot ───────────
;; Closes the "nothing rehydrates a db from a snapshot CID" gap for the ONE case
;; that matters for scale: a FILTERED read. Instead of rebuilding the whole 4-index
;; db (the wasm worker's full-graph rehydrate that OOMs at 128MB), decode the
;; snapshot's index-roots, pick the ONE index tree the query needs, and prefix-seek
;; it by the components. arrangement persists each index as `(pr-str [k1 k2 v]) → true`
;; leaves (index-root), so a components prefix is a string prefix on that tree.

;; index → snapshot key + how its [k1 k2 v] maps back to a datom {:e :a :v}
(def ^:private index-spec
  {:eavt {:root "spo" :->eav (fn [[s p o]] [s p o])}   ; spo: [s p o]
   :aevt {:root "pso" :->eav (fn [[p s o]] [s p o])}   ; pso: [p s o]
   :avet {:root "pos" :->eav (fn [[p o s]] [s p o])}}) ; pos: [p o s]

(defn- components-prefix
  "String prefix into a `(pr-str [k1 k2 v])` leaf key for an ordered component
  vector. `[\"a\" \"b\"]` → `[\"a\" \"b\" ` (up to the space before the next
  slot), so it matches every key whose first components are exactly those."
  [components]
  (when (seq components)
    (let [s (pr-str (vec components))]            ; e.g. ["a" "b"]
      (str (subs s 0 (dec (count s))) " "))))     ; drop ] , add the slot separator

(defn cold-datoms
  "`datomic.datoms`-shaped rows read DIRECTLY from a persisted snapshot without
  rehydrating the whole db. `snapshot-cid` is an arrangement commit CID
  (`latest-snapshot-cid`). opts = `{:index :components :limit}` as in `datoms`.
  Touches only the chosen index tree's blocks along the components prefix path
  (range-pruned via `prolly-tree.core/scan-prefix`), never the other three
  indexes — so a keyed read stays small regardless of graph size. `nil`
  snapshot-cid → `[]` (nothing folded yet; see `hot-datoms` for the
  novelty-inclusive read). get-fn: `(get-fn cid) -> bytes`."
  [get-fn snapshot-cid {:keys [index components limit]}]
  (if (nil? snapshot-cid)
    []
    (let [{:keys [root ->eav]} (index-spec (or index :eavt))
          snap      (ipld/decode (get-fn snapshot-cid))
          root-cid  (some-> (get-in snap ["index-roots" root]) ipld/link-cid)
          entries   (if (nil? root-cid)
                      []
                      (pt/scan-prefix get-fn root-cid (or (components-prefix components) "")))
          rows      (for [[k _] entries
                          :let [[e a v] (->eav (mapv qs/edn->link (edn/read-string k)))]]
                      {:e e :a a :v_edn (v->edn v) :added true})
          rows      (cond->> rows limit (take limit))]
      (vec rows))))

(defn hydrate-db
  "Rebuild the full hot 4-index `db` from a persisted snapshot. Reads ONE
  index tree (spo) in full and re-asserts every quad, so the reconstructed
  db is ~1× graph size. This is `fold!`'s internal starting point (hydrate
  the last-indexed snapshot, then re-assert novelty on top) — an O(graph_shard)
  operation, now paid once per fold instead of once per write. get-fn:
  `(get-fn cid) -> bytes`; nil snapshot → empty db."
  [get-fn snapshot-cid]
  (if (nil? snapshot-cid)
    (qs/empty-db)
    (reduce (fn [db {:keys [e a v_edn]}]
              (qs/assert-quad db {:s e :p a :o (qs/edn->link (edn/read-string v_edn))}))
            (qs/empty-db)
            (cold-datoms get-fn snapshot-cid {:index :eavt}))))

(defn hot-datoms
  "THE scale-safe read path (ADR-2607032430 D1): `datoms`-shaped rows as of
  `chain-cid`'s current state, merging the indexed snapshot (`cold-datoms` —
  range-pruned, untouched by graph size) with any not-yet-folded novelty
  (small, bounded by the fold threshold). Novelty filtering reuses `datoms`'s
  own index/components logic (build a throwaway hot db from just the
  novelty quads, filter it the normal way) — exactly consistent with the
  snapshot path's semantics, no separate filter implementation to drift.
  Replaces `hydrate-db` + `datoms` for any caller that doesn't need a full
  hot db value. `nil` chain-cid → `[]`."
  ([get-fn chain-cid] (hot-datoms get-fn chain-cid nil))
  ([get-fn chain-cid opts]
   (if (nil? chain-cid)
     []
     (let [state (state-at get-fn chain-cid)
           snap-rows (cold-datoms get-fn (indexed-cid state) opts)
           novelty-quads (mapcat #(read-tx-block get-fn %) (novelty-cids state))
           novelty-rows (datoms (reduce qs/assert-quad (qs/empty-db) novelty-quads) opts)]
       (vec (concat snap-rows novelty-rows))))))

(defn fold!
  "Compact `chain-cid`'s novelty into a fresh indexed snapshot: hydrate the
  current `indexed` snapshot, re-assert every novelty quad on top (append
  order), `qs/commit!` that as the new `indexed`, and append ONE new
  commit-dag entry with an empty `novelty`. Cost: O(graph_shard) — the same
  full rebuild `snapshot!`/the pre-D1 `commit!` always paid, now amortized
  over however many `commit!` writes accumulated the folded novelty instead
  of being paid on every single one (`should-fold?`/`novelty-size` decide
  when to call this; correctness never depends on it running promptly).

  Deterministic: prolly-tree/arrangement are content-addressed, so folding
  the identical (indexed, novelty) pair — from ANY writer, a server cron or
  a browser at idle — always produces the same snapshot CID. Concurrent
  folds of the same state are safe, redundant, and cheap (re-`put!`ing
  already-stored bytes is a no-op at the block-store layer)."
  ([put! get-fn chain-cid] (fold! put! get-fn chain-cid ipld/link?))
  ([put! get-fn chain-cid ref?]
   (let [state (state-at get-fn chain-cid)
         novelty-quads (mapcat #(read-tx-block get-fn %) (novelty-cids state))
         db (reduce (fn [db q] (qs/assert-quad db q ref?))
                    (hydrate-db get-fn (indexed-cid state))
                    novelty-quads)
         new-snap-cid (qs/commit! put! db nil qs/current-schema-version)
         new-state {"indexed" (ipld/link new-snap-cid) "novelty" []}]
     (cd/commit! put! get-fn new-state chain-cid))))

(defn verify-chain
  "True iff the commit-dag chain rooted at `chain-cid` is untampered and its
   `:seq` values are gapless from 0. Does NOT verify the prolly-tree
   snapshots or tx blocks each commit's `:state` links to are themselves
   intact (that would require walking every index/tx-block — a follow-up)."
  [get-fn chain-cid]
  (cd/verify-chain get-fn chain-cid))
