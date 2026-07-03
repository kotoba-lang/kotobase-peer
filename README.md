# kotobase-engine

**The kotobase XRPC-surface engine, in real CLJC — verified on both JVM and
ClojureScript.** Composes the already-landed Wave 1–3 primitives
(`prolly-tree`, `quad-store`, `kqe`, `commit-dag`) into `transact`/`datoms`/
`q`/`pull`, persisted as a content-addressed, verifiable commit chain whose references
are real tag-42 IPLD links end to end (chain prev, snapshot state, index
roots, tree children — one generic `ipld.core/links` walk reaches them all) — the
piece that was still missing before production `kotobase.net` traffic can
move off the wasm build of the deleted Rust engine (`kotobase.aozora.app`).
See [ADR-2607022600](https://github.com/com-junkawasaki/root/blob/main/90-docs/adr/2607022600-kotoba-database-crates-cljc-migration-roadmap.md).

**Portability**: `multiformats` and `dag-cbor` — the two foundational
primitives every layer of this stack sits on — used to be JVM-only despite
living in `.cljc`-named files (a documented, known gap every repo in this
chain carried). Both now have real `:cljs` branches (SHA-256 via
`@noble/hashes`, portable CBOR byte buffers), `prolly-tree` had its own
small hidden JVM-only call (`utf8-bytes`) fixed, and this repo's own source
(`commit-dag`, `kotobase-engine`) was `.clj`, not `.cljc` — also fixed. The
whole chain — `multiformats` → `dag-cbor` → `prolly-tree` → `quad-store` →
`kqe` → `commit-dag` → `kotobase-engine` — is now verified end to end under
real ClojureScript (via `nbb`, not just reasoned about), and JVM/cljs
produce **byte-identical CIDs** for the same content at every layer. This
matters specifically because content-addressing is only meaningful if two
platforms computing "the same" data agree on its address — that's now
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
- **`kotoba-lang/kotobase-engine`** (this repo) is the *server-side*
  database engine itself — what actually executes a `transact`/`q`/`pull`/
  `datoms` call and persists the result. It's what a Worker calling into
  `kotoba-lang/kotobase`'s client, or implementing the routes
  `kotoba-lang/atproto` describes, would run *behind* that wire.

## Use

```clojure
(require '[kotobase-engine.core :as eng])

(def store (atom {}))
(def put!   (fn [cid bytes] (swap! store assoc cid bytes)))
(def get-fn (fn [cid] (get @store cid)))

;; write path: O(|tx-data|), independent of graph size (see D1 below)
(def c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}
                                    [:db/add "alice" "name" "Alice"]] nil))
(def c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0))

;; read path: scale-safe, merges the last-folded snapshot with pending novelty
(eng/hot-datoms get-fn c1)     ;=> [{:e "alice" ...} {:e "bob" ...} ...]
(eng/hot-datoms get-fn c1 {:index :eavt :components ["alice"]})

;; compaction: fold accumulated novelty into a fresh indexed snapshot
(eng/should-fold? get-fn c1 32)  ;=> false (only 2 novelty commits so far)
(def folded (eng/fold! put! get-fn c1))
(eng/novelty-size get-fn folded) ;=> 0

(eng/chain get-fn folded)        ;=> commit history, oldest first
(eng/verify-chain get-fn folded) ;=> true

;; pure, hot-db primitives — for tests, backfill tooling, or anyone who wants
;; an in-memory db value to inspect before deciding whether/how to persist it
(def db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}]))
(eng/datoms db)
(eng/q db ["alice" nil nil])
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

- `commit-dag`'s opaque `state` is now `{"indexed" Link|nil "novelty"
  [Link ...]}` — `"indexed"` is the last-folded quad-store snapshot CID,
  `"novelty"` is the ordered list of small tx-block CIDs appended since.
- **`commit!`** appends one novelty tx block: O(|tx-data|), one `commit-
  dag/head` fetch (O(1) since [commit-dag#1](https://github.com/kotoba-lang/commit-dag/pull/1)), never touches a prolly-tree.
- **`hot-datoms`** merges `cold-datoms` on the indexed snapshot
  (range-pruned, untouched by graph size) with the bounded novelty tail
  (decoded and filtered in memory, reusing `datoms`'s own index logic) —
  the scale-safe read path.
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

## Composition decision (resolves a known gap from ADR-2607022600)

`quad-store.core/commit!` and `commit-dag.core/commit!` both implement a
notion of "commit," with different shapes (`{index-roots prev}` vs
`{state prev seq}`) and neither aware of the other. Rather than modifying
either upstream repo, `snapshot!` here calls `quad-store.core/commit!` with
`prev` **always nil** — using it purely to snapshot the 4 indexes into
content-addressed prolly-trees and return one CID — then wraps that
snapshot CID (inside the D1 `{indexed novelty}` state map) as commit-dag's
opaque `state`, so chain history, `:seq`, and tamper/gap verification are
entirely commit-dag's job. Neither library needed to change.

## What is NOT in this landing

- **Multi-clause Datalog join / recursive-rule fixpoint / SPARQL BGP** —
  `kqe`'s own documented scope is triple-pattern only; unchanged here.
- **CACAO / capability auth** — a Worker embedding this engine still needs
  to verify CACAO and check write capability itself (`kotoba-lang/cacao`)
  before calling `commit!`; this repo has no auth opinion.
- **BlockStore backends** (R2, B2, Kubo) — `put!`/`get-fn` are the same
  injection seam `prolly-tree`/`quad-store`/`commit-dag` already use; a
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
clojure -M:test              # JVM
npm run test:cljs            # real shadow-cljs build + node, not nbb
```

```
Ran 17 tests containing 64 assertions.
0 failures, 0 errors.
```

## License

Apache-2.0.
