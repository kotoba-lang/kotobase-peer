# kotobase-engine

**The kotobase XRPC-surface engine, in CLJC.** Composes the already-landed
Wave 1–3 primitives (`prolly-tree`, `quad-store`, `kqe`, `commit-dag`) into
`transact`/`datoms`/`q`/`pull`, persisted as a content-addressed, verifiable
commit chain — the piece that was still missing before production
`kotobase.net` traffic can move off the wasm build of the deleted Rust
engine (`kotobase.aozora.app`). See
[ADR-2607022600](https://github.com/com-junkawasaki/root/blob/main/90-docs/adr/2607022600-kotoba-database-crates-cljc-migration-roadmap.md).

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

(def db (-> (eng/empty-db)
            (eng/transact [{:s "alice" :p "role" :o "admin"}
                            [:db/add "alice" "name" "Alice"]])))

(eng/datoms db)          ;=> [{:e "alice" :a "role" :v_edn "\"admin\"" :added true} ...]
(eng/q db ["alice" nil nil])
(eng/pull db "alice")    ;=> {"role" #{"admin"} "name" #{"Alice"}}

;; content-addressed, chained, verifiable persistence
(def store (atom {}))
(def put!   (fn [cid bytes] (swap! store assoc cid bytes)))
(def get-fn (fn [cid] (get @store cid)))

(def c0 (eng/commit! put! get-fn db nil))
(eng/chain get-fn c0)          ;=> ({:cid c0 :state <quad-store snapshot cid> :seq 0 ...})
(eng/verify-chain get-fn c0)   ;=> true
```

## Composition decision (resolves a known gap from ADR-2607022600)

`quad-store.core/commit!` and `commit-dag.core/commit!` both implement a
notion of "commit," with different shapes (`{index-roots prev}` vs
`{state prev seq}`) and neither aware of the other. Rather than modifying
either upstream repo, `commit!` here calls `quad-store.core/commit!` with
`prev` **always nil** — using it purely to snapshot the 4 indexes into
content-addressed prolly-trees and return one CID — then wraps that
snapshot CID as commit-dag's opaque `state`, so chain history, `:seq`, and
tamper/gap verification are entirely commit-dag's job. Neither library
needed to change.

## What is NOT in this landing

- **Cold query / hydrate-from-commit**: nothing here rebuilds a `db` value
  from a stored snapshot CID. `quad-store`'s `commit!` writes the 4 index
  prolly-trees but there's no public "walk the tree back into a db" path
  yet (would need to be added to `quad-store`, not reverse-engineered here
  against its private db shape). Every `transact`/`q`/`pull`/`datoms` call
  in this repo operates on the **hot**, in-memory `db` value only.
- **Multi-clause Datalog join / recursive-rule fixpoint / SPARQL BGP** —
  `kqe`'s own documented scope is triple-pattern only; unchanged here.
- **CACAO / capability auth** — a Worker embedding this engine still needs
  to verify CACAO and check write capability itself (`kotoba-lang/cacao`)
  before calling `transact`; this repo has no auth opinion.
- **BlockStore backends** (R2, B2, Kubo) — `put!`/`get-fn` are the same
  injection seam `prolly-tree`/`quad-store`/`commit-dag` already use; a
  Worker wires them to real storage. No Memory-only assumption is baked in,
  but no real backend ships here either.
- **Snapshot-level tamper-evidence**: `verify-chain` checks the commit
  chain itself, not whether the prolly-tree bytes a commit's `:state`
  points to are intact — that would mean walking every index.

## Test

```bash
clojure -M:test    # or: bb test
```

```
Ran 7 tests containing 17 assertions.
0 failures, 0 errors.
```

## License

Apache-2.0.
