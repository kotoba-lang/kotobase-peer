;; kotobase-engine.core — the kotobase XRPC-surface engine, assembled from the
;; already-landed Wave 1-3 primitives (ADR-2607010930 Phase 6 + ADR-2607022600):
;; quad-store (hot 4-index Arrangement + per-commit snapshot), kqe (triple-pattern
;; query routing), commit-dag (chain/verify). This is the piece that was still
;; missing: something that actually implements the `ai.gftd.apps.kotobase.datomic.*`
;; surface (transact/datoms/q/pull) end to end, backed by content-addressed,
;; verifiable persistence, in CLJC — as opposed to the wasm build of the deleted
;; Rust engine that kotobase.net's production backend (kotobase.aozora.app)
;; currently runs.
;;
;; Composition decision (resolves the ADR-2607022600 "quad-store/commit! and
;; commit-dag are two unmerged implementations" gap): quad-store/commit! is called
;; with `prev` always nil here — it is used ONLY to snapshot the 4 indexes into
;; content-addressed prolly-trees and return one CID for that snapshot. Chain
;; history, `:seq`, and tamper/gap verification are commit-dag's job, wrapping
;; that snapshot CID as its opaque `state`. Neither library is modified; this
;; namespace is the seam between them.
(ns kotobase-engine.core
  (:require [ipld.core :as ipld]
            [quad-store.core :as qs]
            [kqe.core :as kqe]
            [commit-dag.core :as cd]))

(defn empty-db [] (qs/empty-db))

(defn- ->quad
  "Coerce one tx-data item to a `{:s :p :o}` quad (all three coerced to strings,
   matching quad-store's opaque-string scope). Accepts a `{:s :p :o}` map,
   `[:db/add e a v]`, or a bare `[e a v]` triple."
  [item]
  (cond
    (and (map? item) (contains? item :s) (contains? item :p) (contains? item :o))
    {:s (str (:s item)) :p (str (:p item)) :o (str (:o item))}

    (and (vector? item) (= 4 (count item)) (= :db/add (first item)))
    (let [[_ e a v] item] {:s (str e) :p (str a) :o (str v)})

    (and (sequential? item) (= 3 (count item)))
    (let [[e a v] item] {:s (str e) :p (str a) :o (str v)})

    :else
    (throw (ex-info "kotobase-engine: unrecognized tx-data item"
                     {:item item}))))

(defn transact
  "Apply `tx-data` (a seq of `{:s :p :o}` quads, `[:db/add e a v]`, or `[e a v]`
   triples) to `db`. Returns the new (immutable) db value. `ref?` is passed
   through to `quad-store.core/assert-quad` for reverse-reference indexing."
  ([db tx-data] (transact db tx-data (constantly false)))
  ([db tx-data ref?]
   (reduce (fn [db item] (qs/assert-quad db (->quad item) ref?))
           db tx-data)))

(defn- v->edn
  "Serialize a quad-store value (always a string) to the EDN string the
   kotobase wire uses. Matches the shape the pre-existing DataScript-backed
   `kotobase.engine` (app-aozora) produced, for wire compatibility."
  [v] (pr-str v))

(defn datoms
  "`datomic.datoms`-shaped rows `{:e :a :v_edn :added}`, AppView-scan compatible
   (same wire shape as the DataScript-backed `kotobase.engine` this replaces).

   1-arg → whole-db (:eavt full scan, unchanged). 2-arg honors an optional
   `{:index :components :limit}` SERVER-SIDE via quad-store's index accessors, so
   a filtered read (e.g. getBackup by entity, or by [attr value]) materialises
   ONLY the matching entity/attr instead of the whole graph — the root fix for
   the deployed wasm worker ignoring index/components_edn/limit and rehydrating
   the full graph per read (ADR-2607022330 addendum 2).

   `:index` ∈ #{:eavt :aevt :avet} (default :eavt); `:components` is an ordered
   prefix in that index's key order (:eavt=[e a v], :aevt/:avet=[a … ]); values
   are the opaque strings quad-store stores (the PDS's :db/id for e, `:ns/attr`
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
           ;; AVET — key order [a v e]; [a v] is quad-store's point lookup
           :avet (cond
                   (and c0 c1) (for [e (qs/by-predicate-value db c0 c1)] [e c0 c1])
                   c0          (for [[e vs] (qs/by-predicate db c0), v vs] [e c0 v])
                   :else       (for [[a em] (:pso db), [e vs] em, v vs]    [e a v])))
         triples (cond->> triples limit (take limit))]
     (vec (for [[e a v] triples] {:e e :a a :v_edn (v->edn v) :added true})))))

(defn q
  "`datomic.q`-equivalent: `pattern` is `[s p o]` (nil = wildcard), routed to
   the matching index via kqe. Returns a set of `{:s :p :o}` quads. NOTE:
   triple-pattern only — multi-clause join / recursive-rule fixpoint is not
   implemented (kqe's own documented scope), see README."
  [db pattern]
  (kqe/query db pattern))

(defn pull
  "`datomic.pull`-equivalent for entity `s`: `{p #{o...}}` (quad-store has no
   cardinality tracking, so every attribute is multi-valued — a caller
   expecting single-valued Datomic pull semantics must pick e.g. `first`)."
  [db s]
  (qs/entity-attrs db s))

;; ── persistence: content-addressed, chained, verifiable ─────────────────────

(defn commit!
  "Snapshot `db`'s 4 indexes into content-addressed prolly-trees (via
   `quad-store.core/commit!`, always with `prev` nil — see namespace
   docstring) and append that snapshot CID onto the commit-dag chain rooted
   at `prev-chain-cid` (nil for the first commit). Returns the new chain
   commit CID. `put!`/`get-fn` follow the prolly-tree/quad-store/commit-dag
   convention: `(put! cid bytes)` / `(get-fn cid) -> bytes`."
  [put! get-fn db prev-chain-cid]
  (let [snapshot-cid (qs/commit! put! db nil)]
    ;; state is a REAL tag-42 link, so a generic walk (ipld.core/links /
    ;; kotoba-client.ipld-hydrate) reaches chain -> snapshot -> index trees
    ;; with no engine-specific schema knowledge.
    (cd/commit! put! get-fn (ipld/link snapshot-cid) prev-chain-cid)))

(defn chain
  "Full commit history rooted at `chain-cid`, oldest first. Each entry's
   `:state` is a quad-store snapshot CID (not a db value — see README's
   \"cold query\" gap: nothing here rehydrates a db from a snapshot CID yet)."
  [get-fn chain-cid]
  (cd/chain get-fn chain-cid))

(defn head
  "The most recent commit-dag entry in the chain rooted at `chain-cid`."
  [get-fn chain-cid]
  (cd/head get-fn chain-cid))

(defn latest-snapshot-cid
  "The quad-store snapshot CID (`:state`) of the most recent commit in the
   chain rooted at `chain-cid`, or nil if the chain is empty."
  [get-fn chain-cid]
  (some-> (head get-fn chain-cid) :state ipld/link-cid))

(defn verify-chain
  "True iff the commit-dag chain rooted at `chain-cid` is untampered and its
   `:seq` values are gapless from 0. Does NOT verify the prolly-tree
   snapshots each commit's `:state` points to are themselves intact (that
   would require walking every index — a follow-up, see README)."
  [get-fn chain-cid]
  (cd/verify-chain get-fn chain-cid))
