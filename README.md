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
already used by the former client compatibility repository, and "engine"
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

## Repository boundary

Two adjacent, already-existing `kotoba-lang` repos have different jobs:

- **`kotoba-lang/kotobase`** provides client protocol compatibility. It is not
  a storage engine and is not a dependency of this peer.
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

Large immutable runs are stored as a small run root plus logical-key-aligned
data blocks (128 rows and 1 MiB encoded bytes by default). Both bounds are
applied to canonical encoded bytes, while one indivisible hot logical key may
exceed them and is marked `oversized-logical-key`. Descriptors carry their
verified encoded byte length. The block descriptors are copied into the
manifest run reference, so a bounded prefix page can skip blocks wholly before
its continuation without fetching the run root or replaying earlier data.
Continuation reads are demand-only: a block is fetched after its descriptor is
known to overlap the cursor and current page cutoff. This avoids speculative
successor GETs and keeps physical reads attributable to returned page work.
Required heads from independent runs are fetched in bounded waves, ordered by
logical minimum. `:block-get-concurrency` caps each wave (default 4), each run
contributes at most one block to a wave, and the cutoff is recomputed before
the next wave. `:block-get-max-wave-bytes` (default 4 MiB) also caps the sum of
descriptor bytes; legacy descriptors without byte metadata conservatively
occupy a wave alone. Successor blocks from one run are therefore never
prefetched in the same wave.
With an explicit `:remainder-max-bytes` budget, a page may return the decoded
current block as `:block-remainder`. A resumable host can persist that bounded
value beside the logical cursor and supply it to the next page, avoiding a
second R2 GET when the cursor remains inside the same physical block. The next
page verifies its CID and validates it against the current manifest descriptor
before use. A block that does not fit the budget is omitted and transparently
falls back to the ordinary immutable GET path.
Callers may pass an immutable `:head-cid`; every page then resolves that legacy
manifest or EpochPublication directly and never re-reads the mutable head. This
is the snapshot-pinned path used by resumable remote queries.
Legacy inline-row runs remain readable.  A single hot logical key is never
split merely to meet the row target and can therefore produce an oversized
block; that skew case remains an explicit compaction/query-planning boundary.

The Worker object-store adapter supports R2 bindings and signed S3-compatible
GET/PUT requests. Mutable S3 heads require a provider that implements
conditional `PutObject`; enable that path explicitly with
`MERKLE_S3_CONDITIONAL_HEAD=true`. R2 remains the default CAS implementation.
Reachability GC walks IPLD links from every R2 head because immutable blocks are
deduplicated in one shared prefix. It only considers objects older than a
caller-supplied grace period and supports a dry audit. Delete mode requires the
complete sorted head/root/ETag snapshot to remain unchanged and independently
recomputes reachability plus the sorted candidate inventory a second time.
Only byte-for-byte equal candidate key vectors are deleted; a head/root change
or candidate appearing/disappearing aborts the sweep. The grace period protects
fresh blocks written before a concurrent head publication. Publishers must
upload their immutable blocks before HeadCAS.
Before deletion, every candidate is copied sequentially to immutable
`gc-backups/objects/<content-cid>` storage and a DAG-CBOR inventory is written
under `gc-backups/inventories/<inventory-cid>`. The delete receipt includes that
inventory CID and deletion does not begin unless all objects and the inventory
have been stored. `restore-gc-inventory!` verifies the inventory CID, restricts
destinations to GC-managed block/scheduler namespaces, verifies every content
CID, and uses put-if-absent so it cannot overwrite newer mutable state. Repeated
restore is idempotent only when the existing bytes have the expected CID.
`MERKLE_GC_BACKUP_BUCKET` may bind a separate R2 bucket; otherwise backup uses
`MERKLE_BUCKET`. Backup retention is explicit—GC never automatically deletes
these inventories or their content-addressed objects.

Paged database restore externalizes exact reachability verification under
`scheduler/database-restore/<target>/<task>/verification/`. Each CID has an
ETag-CAS `pending`/`done` marker. One restore step lists at most 64 markers and
processes at most one CID-verified block or opaque materialized object; its
immutable checkpoint stores only the listing cursor and count. Child markers
are durable before their parent becomes done. A complete scan may publish the
head only when the unique marker count equals the immutable inventory entry
count. Active verification checkpoints are GC roots; inactive markers follow
the database-restore grace, double-snapshot fence, backup-before-delete, and
CID-verified restore path. The 10,000-entry synthetic gate kept 158 checkpoint
samples between 421 and 448 bytes; see
`bench/results/2026-07-23-external-restore-verification-10k.edn`. This removes
the resumable verifier's O(total entries) process memory. Database backup
inventory v3 also replaces the v2 root's O(page count) descriptor vector with
a CID-addressed 64-way directory tree. Restore by ordinal fetches one directory
node per level; verification fetches at most one node per namespace/CID target
per level. v1/v2 remain readable. A 1,048,832-entry synthetic shape used 4,097 data pages, 68 directory
nodes, height 3, a 728-byte top node, and a 20,812-byte largest directory node;
see `bench/results/2026-07-24-database-inventory-v3-tree.edn`. Backup-side graph
discovery remains a separate process-local scale gap.

The authenticated `/bench/orphan-gc` drill uses an isolated prefix and cleans
it in `finally`. The 2026-07-23 real-R2 run marked 2 heads and 4 live blocks,
confirmed the same one-candidate inventory twice, backed up and deleted the
orphan, restored it by inventory CID, and verified its content CID in 1,446 ms.
See `bench/results/2026-07-23-r2-gc-backup-restore.edn`; this is a correctness
drill rather than a production latency percentile.

`kotobase-peer.retention` defines the pure retention-root contract. Active
reader and replication roots are leases with an explicit millisecond expiry;
legal-hold, release, and backup roots are durable. The Worker persists mutable registry
records under `roots/<db-id>/<kind>/<encoded-id>` using R2 ETag CAS. Renewal and release
therefore cannot overwrite a concurrent owner; release writes an inactive CAS
tombstone instead of performing an unsafe delete. GC marks every active root,
reports the effective minimum safe epoch, and fences sweep when either a head
or registry ETag changes. `compact-head!` uses the same minimum epoch, so a
pinned snapshot is protected from both version pruning and physical block GC.
One explicit safe-epoch oracle filters all root kinds for both consumers and
keeps just-expired leases active for `MERKLE_RETENTION_CLOCK_SKEW_MS` (30
seconds by default), preventing a modest host-clock lead from pruning a live
reader. The oracle's safe epoch, active kind counts, and skew allowance are
reported by GC; compaction consumes the same decision rather than recomputing
an implicit latest-epoch boundary.

RangeDirectory v1 is a compaction boundary, not another manifest level. A
later window stops before that boundary, compacts every inherited ref for each
index it updates, and atomically replaces that index in the next directory;
untouched indexes remain inherited. CID deduplication and canonical ref order
make equivalent retries converge on one directory CID instead of accumulating
overlapping stale refs. Direct legacy VersionManifest chains remain readable
and become v1 directories on compaction. A directory with a future version,
wrong database, or epoch beyond its owning manifest fails closed until an
explicit format migrator is provided.

Window compaction streams every overlap component as a k-way merge over
physical run blocks. Each open run retains at most one input block, completed
blocks are released before the next GET, and each bounded output run is PUT
before the following output is built. Up to four disjoint range components run
in parallel; output refs retain canonical range order. A singleton
non-overlapping sorted run is promoted into the RangeDirectory by CID without
GET/re-encode/PUT; only actual overlap components are merged. The default fail-closed
limits are 64 overlapping runs, 2 MiB per input block, 16 MiB per component
and 64 MiB across a wave, 4,096
rows for a legacy inline run, and 65,536 versions for one indivisible logical
key. The resulting manifest records input GETs/bytes, peak buffered rows/bytes,
output PUTs/bytes, and output rows. These are algorithmic working-set metrics;
they do not replace Worker CPU/memory or R2 request measurements in the
1M/10M production gate.

Production compaction is scheduled with one deterministic task CID per
database head and compaction bounds. The R2 host claims a per-database lease
with ETag CAS, fences active contenders, renews only running leases, reclaims
expired attempts, and writes immutable token-scoped checkpoints. Scheduling
first reads a bounded manifest/L0 pressure window, so an already compacted
database is not compacted forever. Hosts use `run-compaction-batch!` for bounded
concurrency; HeadCAS remains the publication authority if a lease expires while
work is still running, and unreachable speculative output is collected by the
shared-prefix reachability GC. The authenticated `/bench/compaction-lease`
drill uses an isolated UUID prefix and removes every lease/checkpoint in
`finally`. A 2026-07-23 real-R2 run proved active-contender and stale-ETag
fencing, renewal, expiry reclaim at attempt 2, immutable checkpointing, and
terminal completion in 935 ms of measured R2 operations (p50 91 ms, p95 138
ms; remote Worker request 1613 ms).

Global `blocks/` and `objects/` collection also has a resumable bounded path:
`step-resumable-global-gc!` persists its mark set, traversal frontier,
candidate set, namespace cursor, and counters under a task-scoped R2 prefix.
Each invocation lists at most 64 objects and advances one of marking,
namespace scan, root-fenced backup-before-delete sweep, or mark cleanup. This
keeps data-namespace and reachable-CID working sets independent of database
size; the legacy whole-inventory `gc-unreachable!` remains available during
host migration. Every non-terminal step is first claimed with an expiring
ETag-CAS lease; active contenders return `:leased`, expired work is reclaimed,
and the lease is removed from the committed checkpoint after each bounded
effect.

Run `clojure -M:merkle-bench 1000 100000 10000000` for the ADR scale sweep;
`MERKLE_BENCH_WRITERS` selects simulated concurrent flushers (default 32).
For release evidence, prefer `npm run bench:merkle-scale`, which runs each
size in an isolated JVM, defaults to one writer and a 2 GiB heap, and enables
the resource gate. `KOTOBASE_SCALE_SIZES=100000,10000000`,
`KOTOBASE_SCALE_MAX_HEAP`, and `KOTOBASE_SCALE_WRITERS` define the sweep.
Every result includes flush p50/p95/p99, compaction and total wall time,
process CPU, sampled peak heap, object counts/bytes, and read/write
amplification. Missing metrics, lost rows, threshold breaches, or OOM make the
runner nonzero; a smaller successful tier cannot mask a failed larger tier.
Run `clojure -M:view-bench 100000 512` for the browser/no-local-disk serving
gate. It builds an immutable materialized-view pack, then executes deterministic
point and bounded-range queries through the same sparse-index selection, byte
range slicing, block-CID verification, and decode path used by a browser host.
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
  pruning older shadowed versions; and
- `kotobase-peer.materialized-view`, which packs independently addressed view
  blocks into a large immutable object and publishes a small query-bundle CID
  containing sparse min/max keys and byte offsets. A browser query binary-seeks
  that bundle and emits bounded `:object/range-get` effects; it never needs the
  full view or a persistent local database. The Worker adapter interprets these
  effects using native R2 or standard S3 HTTP Range requests without KV.

Materialized views are derived acceleration data, not a second authority. Their
bundle records the source manifest/epoch and optional plan CID; canonical datoms
remain authoritative and a view can be discarded and rebuilt. Physical packed
objects and logical block CIDs intentionally have different granularity, while
every returned range is verified against its logical block CID before decode.
`build-view-delta` appends an epoch pack linked to the previous bundle;
`query-packed-chain` applies newest-key-wins assertions/retraction tombstones,
and `compact-packed-chain` deterministically collapses a bounded chain back to
one base pack. Run `clojure -M:view-delta-bench 10000 1000 512` for this gate.

Resumable join hosts can encode a large positive-conjunctive Datalog frontier
with `datalog-materialization/build-frontier-work-chain`. It canonicalizes
symbol-keyed bindings into portable DAG-CBOR, recursively splits them by an
explicit byte budget, and links work nodes so a checkpoint needs only one CID.
New join waves can be prepended to an existing pending chain without rewriting
it. `decode-frontier-work` validates the format and restores ordinary Datalog
bindings. A single binding larger than the configured budget fails closed.

Adjacent selected blocks are coalesced into bounded (default 1 MiB) object
ranges. The response is split back into logical blocks and every CID is still
verified independently, reducing request amplification without weakening IPLD
integrity.
Each block descriptor also carries a deterministic CLJ/CLJS Bloom filter (10
bits/key, 7 hashes). Exact negative lookups can therefore complete from the
small query bundle with zero data-object requests. Missing/legacy filters and
false positives always fall back to a verified block read; range scans do not
apply an exact-key filter.

`wrangler.view-bench.jsonc` deploys a fixed-object verification gateway for the
browser gate. It exposes only single ranges up to 1 MiB and returns standard
HTTP `206`, `Content-Range`, `ETag`, CORS, and
`Cache-Control: public, max-age=31536000, immutable`. It does not use KV or the
Cloudflare Cache API; ordinary browser/CDN HTTP caches are optional host
acceleration. Run `npm run test:r2-gateway` for its contract test.

`npm run build:view-e2e` compiles the CLJS query executor served at `/e2e`.
That page downloads and verifies the query-bundle CID, decodes DAG-CBOR, plans
from sparse metadata and Bloom filters, fetches one bounded R2 byte range,
verifies the selected block CID, decodes its rows, and renders the result. The
2026-07-21 real-browser R2 gate returned `tenant-a/000000500` in one 6,348-byte
range request: 207.3 ms on the first navigation and 27.3–29.5 ms total across
five warm reloads. The measured receipt is in
`bench/results/2026-07-21-browser-full-query-e2e.edn`.

The load gate extends that executor to repeated point and 200-row range queries
plus five batches of eight concurrent cold point queries. Protected fixture
routes require an `E2E_BEARER_TOKEN` Worker secret; the browser accepts the
capability only through the URL fragment and sends it as `Authorization`, so it
is neither committed nor sent in the page URL. Tenant responses are
`private, immutable` and vary on authorization. See
`bench/results/2026-07-21-browser-r2-load-auth.edn` for the real R2 receipt.

Encrypted views keep plaintext logical block CIDs separate from ciphertext
storage CIDs. Each bundle descriptor carries its key ID, algorithm, nonce,
plaintext length, and ciphertext range. The browser retrieves a DEK only
through the authorized, no-store host endpoint, decrypts each selected block
with WebCrypto AES-256-GCM, then resumes the pure kernel and verifies the
plaintext IPLD CID before decode. Rotating a key therefore replaces the
ciphertext pack and bundle while preserving logical block identity. The real
encrypted R2/browser result is recorded in
`bench/results/2026-07-22-browser-r2-encrypted-view.edn`.

Base state and derived state can now be published through one immutable
`kotobase/epoch-publication` root. `atomic-publication/build-plan` removes the
constituent base/view HeadCAS effects, requires the statistics and every view
bundle to share the base epoch, requires each bundle to pin the exact base
manifest CID, then emits all BlockPut/ObjectPut effects followed by exactly one
root HeadCAS. The Worker host refuses malformed plans and never touches the
head after an immutable write failure. Direct VersionManifest heads remain
explicitly readable during migration. The real two-contender R2 drill produced
one winner and a root whose base/statistics/bundle/pack artifacts were all
present; see `bench/results/2026-07-23-r2-atomic-derived-publication.edn`.
Object-store entity readers and compaction now resolve either head format.
Prefix/exact entity readers merge every selected EAVT run at the resolved base
epoch before grouping, so newer assertions win and tombstones cannot leak stale
datoms into host-side refresh state.
Compaction rewrites only the physical base manifest, deterministically rebases
each current query bundle's `source-manifest`, preserves statistics/view packs,
puts those immutable blocks first, and finally CASes a replacement publication
root. A legacy direct VersionManifest remains a supported migration input.

`datalog-materialization/refresh-plan` closes the refresh-consistency path for
registered query views. Positive conjunctive queries use changed datoms as
clause anchors and re-check only affected result tuples, including alternative
join derivations and parameter inputs. Negation, aggregation, functions, and
rules use a correctness-preserving recompute path. Every registered view must
be supplied exactly once with an older pinned bundle; its delta bundle, scoped
statistics, and base flush are then published at one epoch through the same
single-HeadCAS `EpochPublication` plan.

Rotation uses a keyring resolved from the immutable bundle descriptors rather
than a client-side current-key constant. During a rollout the host may expose
both old and new key IDs; clients fetch only IDs referenced by their pinned
bundle. After the new pack and bundle are published and verified, the old key
can be revoked without interrupting new queries. The v1→v2 dual-key and revoke
drill is recorded in `bench/results/2026-07-22-browser-r2-key-rotation.edn`.

The browser join gate uses an encrypted edge view as its outer bounded scan,
deduplicates the resulting foreign keys, and turns a contiguous lookup batch
into one inner sparse-index range. For 20 post→author results this reduces the
naive index nested loop from 21 requests/148,786 bytes to 2 requests/14,912
bytes while retaining AES-GCM and plaintext CID verification. See
`bench/results/2026-07-22-browser-r2-batched-join.edn`.

`batch-point-query-plan` generalizes the inner join step for non-contiguous
keys. It resolves each exact key through sparse metadata and Bloom filters,
deduplicates shared logical blocks, coalesces only physically adjacent selected
blocks, and filters decoded overfetch back to the requested key set. The plan
is pure CLJ/CLJS effect data and is reusable by browser and Datalog hosts. Its
real encrypted R2 gate is in
`bench/results/2026-07-22-browser-r2-noncontiguous-batch.edn`.

`plan-clause-order` adds a portable greedy join-order kernel: it starts with
the lowest estimated cardinality, then prefers clauses connected to already
bound variables before comparing their estimated rows. The three-clause
post-author browser gate therefore executes `edges → authors → posts`, returns
20 joined rows with 3 bounded requests/21,276 bytes, and keeps AES-GCM plus
plaintext CID verification. See
`bench/results/2026-07-22-browser-r2-cost-join.edn`.

The hot Datalog compiler now calls the same clause-order kernel for queries
whose `:where` consists entirely of positive triple patterns. It estimates
each clause through the caller's required `visible?` decision, then passes the
reordered query to `arrangement.datalog/q`; this keeps authorization filtering
in the planning path as well as execution. Binding-sensitive negation,
function, rule, and `or` queries deliberately retain source order. The
compiler plan is inspectable through `datalog-query-plan`.

Query bundles may carry deterministic `query-statistics` entries scoped by an
explicit visibility identifier. A query uses them only when its
`:statistics-scope` matches; legacy bundles, missing patterns, and mismatched
scopes fall back to the visibility-filtered scan estimator. On a 10,000-entity
JVM fixture this removes planning scans (p50 59.786 ms → 0.078 ms). The real
encrypted R2/browser receipt is
`bench/results/2026-07-22-browser-r2-bundle-statistics.edn`.

`refresh-query-statistics` advances those counts from normalized effective
assert/retract deltas and rejects epoch regression, unknown operations, and
cardinality underflow. Statistics carry their source epoch; callers may set
`:query-epoch` and `:max-statistics-age`, with stale or future metadata falling
back to visible scans. Refreshing 10,000 deltas across three clauses measured
p50 24.87 ms on the JVM. The epoch-pinned R2/browser receipt is
`bench/results/2026-07-22-browser-r2-statistics-refresh.edn`.

`transact-effective` is the pure boundary between raw transaction requests and
those statistics: duplicate asserts and missing/double retracts produce no
delta, while entity retracts expand the entity's currently asserted triples in
deterministic order. `transact-with-statistics` applies that normalized result
and refreshes the scoped statistics at the same epoch. A 10,000-op fixture with
9,998 no-ops produced two effective deltas in p50 10.06 ms. See
`bench/results/2026-07-22-effective-statistics-delta.edn`.

`commit-serialized-effective!` carries normalization through the durable CAS
publication boundary. It hydrates and CID-verifies the actual winning head,
persists only effective assert/retract deltas, and re-runs normalization after a
lost race. A semantic no-op creates no block, chain entry, or CAS operation.
This correctness-first path currently performs a full head
hydration; already-normalized writers can retain the O(tx)
`commit-serialized!` append path. See
`bench/results/2026-07-22-persisted-effective-cas.edn`.

The persisted path now uses `hydrate-transaction-slice`: one range-pruned EAVT
prefix read per distinct transaction subject plus one replay of the bounded
unfolded novelty set. Unrelated snapshot rows are never hydrated, while
retract/reassert and retractEntity semantics remain identical to a full replay.
On cljs, `:async-get-fn` routes these prefixes directly through the asynchronous
prolly-tree scanner. See
`bench/results/2026-07-22-persisted-effective-prefix.edn`.

New novelty queue nodes also carry keyed blinded subject tokens. Effective
writes supply the same `blind-fn` used by the indexed arrangement, so a slice
walk can reject unrelated tx blocks before fetching or decrypting their
ciphertext. Nodes written through the legacy `commit!` arity have no tokens and
remain readable through a conservative fallback. With 30 distinct unfolded
writes, effective commits averaged 19.5 total block gets instead of fetching
every novelty ciphertext; the remaining linear cost is the queue-node walk.
See `bench/results/2026-07-22-novelty-subject-index.edn`.

The subject directory is now grouped into immutable 16-entry metadata
segments. The head points only to the newest segment; full segments link to the
previous segment, while appends rewrite at most one bounded partial segment.
At the default fold threshold this caps directory reads at four blocks instead
of 64 queue nodes. Thirty accumulating writes averaged 7.37 total block gets,
down from 19.5 with the per-node directory. See
`bench/results/2026-07-22-novelty-segment-index.edn`.

Bounded partial folds now preserve both queue-node subject tokens and rebuild
the remaining segment directory; folding no longer silently drops the pruning
optimization. `fold-serialized-if-needed!` combines the fold threshold,
bounded `max-novelty`, and HeadCAS publication into one scheduler primitive.
Below threshold it writes no blocks and performs no CAS; after contention it
re-evaluates the actual winning head before deciding or folding.

CLJS serialized writes now await Promise-returning CAS implementations, and
`object-store.worker/compare-and-exchange-head!` adapts R2/S3 ETags to the
engine contract (return next on success, actual winner on loss). A real R2
remote-preview gate wrote one immutable 256-byte block per operation and
published 32 updates with zero lost writes at concurrency 1, 8, and 32. The
single-head contention result is intentionally not hidden: p50 was 293 ms,
941 ms, and 6,776 ms respectively, demonstrating that production throughput
must shard heads or batch through a transactor. See
`bench/results/2026-07-22-r2-head-cas-write.edn`.

`kotobase-peer.transactor/plan-head-batches` is the provider-neutral hot-head
boundary. It preserves request order within each actor/tenant head, coalesces
bounded request/datom groups into one canonical transaction, and never batches
across heads. `execution-waves` runs at most one batch per head while allowing
independent heads in parallel; `commit-serialized-batch!` publishes one planned
batch with one HeadCAS and returns acknowledgements for every logical request.
Request IDs correlate acknowledgements; they are not silently treated as a
second storage model or an exactly-once ledger. Production ingress must durably
deduplicate retries before planning (or normalize them through the effective
datom path).
The R2 host drill is Bearer-protected, uses a unique prefix, and cleans every
head/block in `finally`. For 32 logical writes, the previous concurrent
single-head baseline needed 508 CAS attempts and 10,717 ms. Eight actor/tenant
heads with four writes per batch needed 8 attempts, lost no writes, and took
515 ms (62.14 logical writes/s): 63.5x fewer CAS attempts and 20.8x lower wall
time. See `bench/results/2026-07-23-r2-batched-sharded-write.edn`.

This is currently a behavior-preserving shadow substrate: existing
`commit!`/`hot-datoms`/`fold!` remain the live path until read equivalence and
CLJ/CLJS CID determinism gates pass. New storage work must target the
Merkle-LSM datom/IPLD surface; compatibility storage adapters are not permitted.

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
clojure -M:test              # JVM      -- 183 tests / 479 assertions
npm run test:cljs            # cljs     -- 173 tests / 454 assertions (real shadow-cljs build + node, not nbb)
```

Both 0 failures, 0 errors. Counts differ slightly because some assertions
are platform-specific (`#?(:clj ...)`/`#?(:cljs ...)` branches in the test
suite itself, e.g. JVM-vs-cljs error-shape checks) — not a coverage gap,
the underlying behavior is exercised on both platforms.

## License

Apache-2.0.
