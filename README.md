# kotobase-peer

**The kotobase peer library — Datomic's own term for the transact/q/pull
library an application embeds — in real CLJC, verified on both JVM and
ClojureScript.** Composes the already-landed Wave 1–3 primitives
(`prolly-tree`, `arrangement`, `chain`) into `transact`/`datoms`/
`q`/`pull`, persisted as a content-addressed, verifiable commit chain whose
references are real tag-42 IPLD links end to end (chain prev, snapshot
state, index roots, tree children — one generic `ipld.core/links` walk
reaches them all) — the piece that was still missing before production
`kotobase.net` traffic can move off the wasm build of the deleted Rust
engine (`kotobase.aozora.app`). See
[ADR-2607022600](https://github.com/com-junkawasaki/root/blob/main/90-docs/adr/2607022600-kotoba-database-crates-cljc-migration-roadmap.md).

**Renamed from `kotobase-engine`** (ADR-2607050700): "kotobase" alone was
already taken by the client-side `IStore` port (see below), and "engine"
undersold what this actually is against the Datomic vocabulary the rest of
this substrate deliberately mirrors (`kotoba : kotobase = Clojure :
Datomic`, ADR-2607032500) — Datomic calls the library an application
embeds for `transact`/`q`/`pull` a **peer**. `arrangement` (this repo
depends on it) was `quad-store` + `kqe` until the same ADR merged them.
`chain` (also depended on) was `commit-dag` until a follow-up rename
(ADR-2607050800) — "chain" names the parent-linked structure itself,
matching that repo's own `chain`/`verify-chain`/`head` functions, without
colliding with the unrelated, already-existing `kotoba-lang/log`
(structured logging/telemetry) that "log" (Datomic's own term for this)
would have collided with.

**Portability**: `multiformats` and `dag-cbor` — the two foundational
primitives every layer of this stack sits on — used to be JVM-only despite
living in `.cljc`-named files (a documented, known gap every repo in this
chain carried). Both now have real `:cljs` branches (SHA-256 via
`@noble/hashes`, portable CBOR byte buffers), `prolly-tree` had its own
small hidden JVM-only call (`utf8-bytes`) fixed, and this repo's own source
was `.clj`, not `.cljc` — also fixed. The whole chain — `multiformats` →
`dag-cbor` → `prolly-tree` → `arrangement` → `chain` →
`kotobase-peer` — is verified end to end under real ClojureScript (real
`shadow-cljs`, not `nbb` — see ADR-2607022600 add.3), and JVM/cljs produce
**byte-identical CIDs** for the same content at every layer. This matters
specifically because content-addressing is only meaningful if two
platforms computing "the same" data agree on its address — that's
empirically checked, not assumed.

## Not `kotoba-lang/kotobase`, not `kotoba-lang/atproto`

Two adjacent, already-existing `kotoba-lang` repos have different jobs:

- **`kotoba-lang/kotobase`** is a *client-side* `IStore` port (`put`/`get`/
  `list`/`append`/`read`) that lets an app run standalone locally or persist
  to kotobase.net through an injected transport, without the app's code
  changing. It has no server logic and doesn't know what's on the other end
  of the wire.
- **`kotoba-lang/atproto`** is AT-Protocol *protocol* vocabulary (NSIDs, repo
  URI helpers, lexicon record shapes) shared by anything speaking AT
  Protocol — kotoba, app-aozora, future actors. It isn't kotobase-specific.
- **`kotoba-lang/kotobase-peer`** (this repo) is the *server-side*
  database peer library itself — what actually executes a `transact`/`q`/
  `pull`/`datoms` call and persists the result. It's what a Worker calling
  into `kotoba-lang/kotobase`'s client, or implementing the routes
  `kotoba-lang/atproto` describes, would run *behind* that wire.

## Use

```clojure
(require '[kotobase-peer.core :as eng]
         '[arrangement.core :as arr])

(def store (atom {}))
(def put!   (fn [cid bytes] (swap! store assoc cid bytes)))
(def get-fn (fn [cid] (get @store cid)))

;; write path: O(|tx-data|), independent of graph size (see D1 below)
(def c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}
                                    [:db/add "alice" "name" "Alice"]] nil))
(def c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0))

;; read path: scale-safe, merges the last-folded snapshot with pending novelty
;; hot-datoms/cold-datoms/datoms' visible? is required (Query is a
;; first-class effect, ADR-2607050500, same precedent as q's below) -- a
;; post-filter over each {:e :a :v_edn :added} row, applied before :limit
(eng/hot-datoms get-fn c1 (constantly true))     ;=> [{:e "alice" ...} {:e "bob" ...} ...]
(eng/hot-datoms get-fn c1 {:index :eavt :components ["alice"]} (constantly true))

;; compaction: fold accumulated novelty into a fresh indexed snapshot
(eng/should-fold? get-fn c1 32)  ;=> false (only 2 novelty commits so far)
(def folded (eng/fold! put! get-fn c1))
(eng/novelty-size get-fn folded) ;=> 0

(eng/chain get-fn folded)        ;=> commit history, oldest first
(eng/verify-chain get-fn folded) ;=> true

;; pure, hot-db primitives — for tests, backfill tooling, or anyone who wants
;; an in-memory db value to inspect before deciding whether/how to persist it
(def db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}]))
(eng/datoms db (constantly true))
;; q's visible? is required too (Query is a first-class effect, ADR-2607050500)
(eng/q db ["alice" nil nil] (constantly true))
(eng/pull db "alice")            ;=> {"role" #{"admin"}}
```

## D1: log-structured write path (ADR-2607032430)

Confirmed live 2026-07-03: the original `commit!` took an already-hydrated
hot `db` and rebuilt all 4 prolly-tree indexes from scratch on **every**
write — O(graph). At real mass-write volume (321 actors against a shared
graph) this collapsed a Cloudflare Worker's CPU budget (error 1102) well
before the graph itself was large by any absolute measure. The fix ports
Datomic's log/index separation (memtable/SSTable in LSM-tree terms) into
this content-addressed chain:

- `chain`'s opaque `state` is now `{"indexed" Link|nil "novelty"
  [Link ...]}` — `"indexed"` is the last-folded arrangement snapshot CID,
  `"novelty"` is the ordered list of small tx-block CIDs appended since.
- **`commit!`** appends one novelty tx block: O(|tx-data|), one `chain/
  head` fetch (O(1) since [chain#1](https://github.com/kotoba-lang/chain/pull/1), the pre-rename `commit-dag#1`), never touches a prolly-tree.
- **`hot-datoms`** merges `cold-datoms` on the indexed snapshot
  (range-pruned, untouched by graph size) with the bounded novelty tail
  (decoded and filtered in memory, reusing `datoms`'s own index logic) —
  the scale-safe read path. `hot-datoms`/`cold-datoms`/`datoms` all require
  an explicit `visible?` (same `q` precedent, ADR-2607050500), forwarded
  unchanged into both the snapshot and novelty halves so each row is
  filtered exactly once regardless of which half it came from.
- **`fold!`** compacts: hydrate the indexed snapshot, re-assert novelty on
  top, `snapshot!` the result, commit a fresh `{indexed novelty: []}`
  state. This is the SAME O(graph) work the old `commit!` always paid, now
  amortized over however many `commit!` writes accumulated first —
  triggered by `should-fold?`/`novelty-size`, not required for correctness.
  Content-addressed, so concurrent/redundant folds from any writer (a
  server cron, a browser at idle) converge to the same CID.
- **Backward compatible with zero migration**: a pre-D1 chain's `state` is
  a bare snapshot `Link` (no `{indexed novelty}` wrapper) — `hot-datoms`/
  `latest-snapshot-cid`/`commit!` all normalize that transparently as the
  D1 empty-novelty equivalent, so already-deployed chains keep working.

`snapshot!` is the pre-D1 `commit!`, kept as `fold!`'s internal primitive
and as a one-shot cold-start entry point for callers that already have a
fully materialized hot `db` (backfill/migration tooling, tests).

## Merkle-LSM migration (ADR-2607201600)

`kotobase-peer.merkle-lsm` contains the pure M1 kernel and M2 shadow-flush
vertical slice replacing full-snapshot folding:

The Worker object-store adapter supports R2 bindings and signed S3-compatible
GET/PUT requests. Mutable S3 heads require a provider that implements
conditional `PutObject`; enable that path explicitly with
`MERKLE_S3_CONDITIONAL_HEAD=true`. R2 remains the default CAS implementation.
Reachability GC walks IPLD links from the current head and only deletes objects
older than a caller-supplied grace period.

Run `clojure -M:merkle-bench 1000 100000 10000000` for the ADR scale sweep;
`MERKLE_BENCH_WRITERS` selects simulated concurrent flushers (default 32).
`npm run ci:local` is the canonical JVM/lint/CLJS/benchmark pre-push gate; CI
execution does not depend on GitHub Actions.

- canonical newest-first physical keys for EAVT/AEVT/AVET/VAET;
- deterministic immutable MerkleRun v1 blocks with min/max range metadata;
- sparse VAET runs containing only real IPLD Link values;
- VersionManifest v1 linking L0 runs by index, epoch, safe epoch, statistics,
  and the previous manifest;
- declarative BlockGet/Put, HeadRead/CAS, and CacheGet/Put effects; and
- `flush-plan`, which turns one datom batch into covering L0 runs, a manifest,
  and an ordered `BlockPut ... HeadCAS` publication plan without performing I/O;
- `visible-rows`, a snapshot-epoch MVCC multi-run merge; and
- `compact-runs`, which retains all versions newer than safe epoch plus the
  newest boundary version, preserving every snapshot at/above that epoch while
  pruning older shadowed versions.

This is currently a behavior-preserving shadow substrate: existing
`commit!`/`hot-datoms`/`fold!` remain the live path until read equivalence and
CLJ/CLJS CID determinism gates pass. New storage work must target the
Merkle-LSM surface; no new IStore dependency is permitted.

## Composition decision (resolves a known gap from ADR-2607022600)

`arrangement.core/commit!` and `chain.core/commit!` (`commit-dag.core/
commit!` before ADR-2607050800's rename) both implement a notion of
"commit," with different shapes (`{index-roots prev}` vs
`{state prev seq}`) and neither aware of the other. Rather than modifying
either upstream repo, `snapshot!` here calls `arrangement.core/commit!`
with `prev` **always nil** — using it purely to snapshot the 4 indexes
into content-addressed prolly-trees and return one CID — then wraps that
snapshot CID (inside the D1 `{indexed novelty}` state map) as chain's
opaque `state`, so chain history, `:seq`, and tamper/gap verification are
entirely chain's job. Neither library needed to change.

## Query

- `q` — single `[s p o]` triple pattern, routed via `arrangement.query`.
- `query` — Datomic-shaped `{:find [?var ...] :where [[e a v] ...]}`
  conjunctive multi-clause join, via `arrangement.datalog/q`
  (ADR-2607061200, stage 1 of a staged Datalog roadmap).
- `refs`/`refs-to` — `{predicate #{subjects}}` reverse-reference lookup for
  a given Link value (VAET-style point lookup, ADR-2607050200) — only
  populated for quads whose value was asserted as a real `ipld.core/Link`.
- `datoms`/`hot-datoms`/`cold-datoms`'s `:index` — one of `:eavt` / `:aevt`
  / `:avet` / `:vaet` (added 2026-07-08). `:vaet` is `refs`/`refs-to`'s own
  reverse-reference index (value → attribute → set-of-subjects), the same
  data reachable through the general `datoms` scan shape instead of the
  point-lookup accessor.
- `entid`/`ident` — Datomic's id↔keyword-ident resolution pair. `entid`
  passes a non-keyword id through unchanged (this substrate's entity ids
  are caller-chosen strings, not Datomic's auto-assigned longs) and
  resolves a keyword id via whatever entity has that `:db/ident` asserted
  on it (plain `transact`, no special engine support — schema is data,
  same posture `install-schema`/`schema-of` take). `ident` is the inverse:
  an entity's own asserted `:db/ident`, or nil. No uniqueness enforcement
  on `:db/ident` (documented limitation, not a bug) — see `entid`'s own
  docstring.

## What is NOT in this landing

- **Negation / aggregation / recursive-rule fixpoint / SPARQL BGP** —
  `query`'s conjunctive join (above) does not yet support `not` clauses,
  `:with`/aggregate functions in `:find`, or `:rules`; staged as tracked
  follow-ups (ADR-2607061200), not implemented here.
- **CACAO / capability auth** — a Worker embedding this peer library still
  needs to verify CACAO and check write capability itself
  (`kotoba-lang/cacao`) before calling `commit!`; this repo has no auth
  opinion of its own (`q`'s required `visible?` is the structural seam a
  caller uses to enforce one, not an opinion this repo holds).
- **BlockStore backends** (R2, B2, Kubo) — `put!`/`get-fn` are the same
  injection seam `prolly-tree`/`arrangement`/`chain` already use; a
  Worker wires them to real storage. No Memory-only assumption is baked in,
  but no real backend ships here either.
- **Snapshot-level tamper-evidence**: `verify-chain` checks the commit
  chain itself, not whether the prolly-tree/tx-block bytes a commit's
  `:state` points to are intact — that would mean walking every index.
- **Automatic/scheduled folding**: `should-fold?`/`fold!` are exposed as
  primitives; nothing in this repo decides *when* to call them. A Worker
  or cron wires the policy (e.g. fold after every write past a threshold,
  or on a timer) using its own storage/scheduling.

## Test

```bash
clojure -M:test              # JVM      -- 94 tests / 196 assertions
npm run test:cljs            # cljs     -- 85 tests / 180 assertions (real shadow-cljs build + node, not nbb)
```

Both 0 failures, 0 errors. Counts differ slightly because some assertions
are platform-specific (`#?(:clj ...)`/`#?(:cljs ...)` branches in the test
suite itself, e.g. JVM-vs-cljs error-shape checks) — not a coverage gap,
the underlying behavior is exercised on both platforms.

## License

Apache-2.0.
