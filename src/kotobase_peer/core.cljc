;; kotobase-peer.core — the kotobase peer library (Datomic's own term for
;; the transact/q/pull library an application embeds; this repo was named
;; kotobase-engine until ADR-2607050700 -- "kotobase" itself was already
;; taken by the client-side IStore port, kotoba-lang/kotobase), assembled
;; from the already-landed Wave 1-3 primitives (ADR-2607010930 Phase 6 +
;; ADR-2607022600): arrangement (hot 4-covering-index Arrangement + query
;; routing + per-commit snapshot -- formerly two repos, quad-store and kqe,
;; merged into one per ADR-2607050700), chain (append-only chain/verify,
;; renamed from commit-dag per ADR-2607050800). This is the piece
;; that implements the `ai.gftd.apps.kotobase.datomic.*` surface
;; (transact/datoms/q/pull) end to end, backed by content-addressed,
;; verifiable persistence, in CLJC.
;;
;; ADR-2607032430 (log-structured write path): the original `commit!` snapshotted
;; a full hot db into 4 fresh prolly-trees on EVERY write -- O(graph). Confirmed
;; live 2026-07-03 as the root cause of a CF Worker CPU-limit collapse (error 1102)
;; during a 321-actor mass write. `commit!` is now a novelty-log APPEND (O(tx)):
;; chain's opaque `state` carries `{"indexed" <snapshot Link>|nil "novelty"
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
;; wording verbatim; "quad-store" in the quote is arrangement post-ADR-2607050700
;; rename, "commit-dag" in the quote is chain post-ADR-2607050800 rename):
;; arrangement/commit! is called
;; with `prev` always nil here — it is used ONLY to snapshot the 4 indexes into
;; content-addressed prolly-trees and return one CID for that snapshot. Chain
;; history, `:seq`, and tamper/gap verification are chain's job, wrapping
;; the {indexed, novelty} state map as its opaque `state`. Neither library is
;; modified; this namespace is the seam between them.
(ns kotobase-peer.core
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])   ; both expose read-string over EDN
            [clojure.string :as str]
            [ipld.core :as ipld]
            [prolly-tree.core :as pt]
            [arrangement.core :as qs]
            [arrangement.query :as kqe]
            [arrangement.datalog :as datalog]
            [chain.core :as cd]
            [datom.core :as dc]))   ; canonical datom model (kotoba : kotobase = Clojure : Datomic)

;; ── platform split for the crypto-touching functions below (ADR-2607051000
;; Worker addendum): `blind-fn`/`encrypt-fn`/`decrypt-fn` are synchronous on
;; JVM, `js/Promise`-returning on cljs (Web Crypto's `crypto.subtle` has no
;; synchronous AEAD/HMAC primitive — see `arrangement.core`'s identical
;; platform-contract note, which this repo's crypto-touching fns inherit).
;; `put!`/`get-fn` stay synchronous, unchanged, on both platforms — only the
;; crypto step is async, resolved before any (still-synchronous) tree/chain
;; write. Every fn below that touches `blind-fn`/`encrypt-fn`/`decrypt-fn`
;; therefore returns its result directly on JVM, a `js/Promise` of it on
;; cljs — this file's own sibling of `arrangement.core`'s split.
#?(:cljs
   (defn- pmap-async
     "cljs only: map `f` (returns a `js/Promise`) over `coll` concurrently,
     return one `js/Promise` of the resolved results, order-preserved."
     [f coll]
     (-> (js/Promise.all (into-array (map f coll)))
         (.then (fn [arr] (vec arr))))))

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

;; ── schema: Datomic-style "schema is just datoms too" ───────────────────────
;; (one of the "3 pillars" this landing adds.) An attribute is a normal
;; entity, using its own NAME as its entity id (Datomic's `:db/ident`
;; pattern) -- `db/valueType`/`db/cardinality`/`db/unique` facts ABOUT it are
;; ordinary datoms, queryable like anything else, not a side-channel config
;; the db doesn't know about. `bootstrap-attrs` is the one fixed exception:
;; describing an attribute's schema requires asserting `db/valueType`/etc.
;; facts, which are themselves attributes -- Datomic solves this bootstrap
;; problem with a small built-in meta-schema those specific attributes are
;; exempt from needing a schema entry for; this landing does the same.

(def bootstrap-attrs
  "Meta-schema attributes describing OTHER attributes -- these never need a
  schema entry of their own. Declaring them about some attribute name IS
  how that attribute gets its first schema entry; a database with zero
  schema installed can still bootstrap its first one. `db/tupleTypes`
  (ADR-2607061200 value-type follow-up) declares a `:tuple` attribute's
  fixed per-position value types."
  #{"db/valueType" "db/cardinality" "db/unique" "db/doc" "db/tupleTypes"})

(defn- raw-triple
  "Like `->quad`, but the `:o`/`v` position is NOT coerced (Link-passthrough
  or stringified) -- the caller's ORIGINAL value, for `transact-with-
  schema`'s `:db/valueType` check to inspect before `->quad`'s
  stringification would erase e.g. an int's actual type. Returns `[s p o]`,
  not yet a quad map; storage still goes through the normal `->quad`
  coercion afterward (schema validates the value's type at the API
  boundary, it does not change the on-disk string-based representation
  every other value already uses)."
  [item]
  (cond
    (and (map? item) (contains? item :s) (contains? item :p) (contains? item :o))
    [(str (:s item)) (str (:p item)) (:o item)]

    (and (vector? item) (= 4 (count item)) (= :db/add (first item)))
    (let [[_ e a v] item] [(str e) (str a) v])

    (and (sequential? item) (= 3 (count item)))
    (let [[e a v] item] [(str e) (str a) v])

    :else
    (throw (ex-info "kotobase-peer: unrecognized tx-data item" {:item item}))))

(defn- schema-value->wire
  "One `install-schema` declaration value -> its wire string. A keyword
  stores by `name` (`:string` -> \"string\"); `db/tupleTypes`'s vector of
  per-position types stores comma-joined (`[:string :long]` -> \"string,
  long\") rather than a stringified EDN vector -- simpler to parse back
  (`str/split`) than round-tripping through `edn/read-string`, and this
  substrate's other schema fields are already single plain strings."
  [v]
  (cond
    (vector? v) (str/join "," (map #(if (keyword? %) (name %) (str %)) v))
    (keyword? v) (name v)
    :else (str v)))

(defn install-schema
  "Assert schema declarations as ordinary datoms (see the ns section
  comment above): `schema` is `{attr-name {:db/valueType t :db/cardinality
  c :db/unique u :db/doc \"...\" :db/tupleTypes [t1 t2 ...]}}` (each key
  optional; values may be keywords -- `:string`, `:one`, `:identity`,
  etc. -- or plain strings; `:db/tupleTypes` is a vector of per-position
  value-types, only meaningful when `:db/valueType` is `:tuple`).
  `db/doc` is purely informational, never validated by `transact-with-
  schema`."
  ([db schema] (install-schema db schema ipld/link?))
  ([db schema ref?]
   (transact db
             (for [[attr decl] schema
                   [k v] decl
                   :let [k-name (subs (str k) 1)]        ; :db/valueType -> "db/valueType"
                   :when (contains? bootstrap-attrs k-name)]
               [attr k-name (schema-value->wire v)])
             ref?)))

(defn schema-of
  "Derive `{attr-name {:value-type _ :cardinality _ :unique _ :tuple-
  types _}}` from whatever `db/valueType`/`db/cardinality`/`db/unique`/
  `db/tupleTypes` datoms already exist in `db` (see `install-schema`) --
  schema is just data the db already has, not a side-channel a caller
  must separately track. `:cardinality` defaults to `\"many\"`
  (arrangement's own native cardinality-agnostic behavior, ADR-2607061200
  pillar note) when an attribute has SOME schema entry but no explicit
  `db/cardinality`. `:tuple-types` is a vector of value-type strings
  (parsed back from `db/tupleTypes`'s comma-joined wire string), or `nil`."
  [db]
  (into {}
        (keep (fn [[ent attrs]]
                (let [vt (first (get attrs "db/valueType"))
                      card (first (get attrs "db/cardinality"))
                      uniq (first (get attrs "db/unique"))
                      tuple-types-str (first (get attrs "db/tupleTypes"))]
                  (when (or vt card uniq tuple-types-str)
                    [ent {:value-type vt :cardinality (or card "many") :unique uniq
                          :tuple-types (some-> tuple-types-str (str/split #","))}]))))
        (:spo db)))

(defn- validate-value-type!
  "`value-type` -> the check run against the ORIGINAL value `v` (before
  `->quad`'s string coercion -- see `raw-triple`). `tuple-types` (a
  vector of per-position value-type strings, from `db/tupleTypes`) is
  only consulted when `value-type` is `\"tuple\"`.

  `\"bigdec\"`/`\"bigint\"` are deliberately NOT included: cljs/JS has no
  true arbitrary-precision decimal/integer type, so a check that's
  meaningful on JVM (`decimal?`) would ALWAYS fail on cljs/nbb -- a
  cross-platform correctness mismatch this codebase avoids elsewhere
  (see e.g. `avg`'s forced double division in `arrangement.datalog`),
  not an oversight."
  [attr value-type v tuple-types]
  (case value-type
    "string"  (when-not (string? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a string" {:attr attr :value v})))
    "long"    (when-not (integer? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a long" {:attr attr :value v})))
    "double"  (when-not (number? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a double" {:attr attr :value v})))
    "boolean" (when-not (boolean? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a boolean" {:attr attr :value v})))
    "ref"     (when-not (ipld/link? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a ref (ipld/Link)" {:attr attr :value v})))
    "uuid"    (when-not (uuid? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a uuid" {:attr attr :value v})))
    "instant" (when-not (inst? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected an instant" {:attr attr :value v})))
    "keyword" (when-not (keyword? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a keyword" {:attr attr :value v})))
    "symbol"  (when-not (symbol? v)
                (throw (ex-info "kotobase-peer: schema violation -- expected a symbol" {:attr attr :value v})))
    "bytes"   (when-not #?(:clj (bytes? v) :cljs (instance? js/Uint8Array v))
                (throw (ex-info "kotobase-peer: schema violation -- expected bytes" {:attr attr :value v})))
    "tuple"   (do
                (when-not (vector? v)
                  (throw (ex-info "kotobase-peer: schema violation -- expected a tuple (vector)" {:attr attr :value v})))
                (when-not (= (count tuple-types) (count v))
                  (throw (ex-info "kotobase-peer: schema violation -- tuple arity mismatch"
                                  {:attr attr :expected (count tuple-types) :got (count v)})))
                (dorun (map (fn [t elem] (validate-value-type! attr t elem nil)) tuple-types v)))
    nil))

(defn- normalize-inline-schema
  "Caller-passed inline schema declarations (`install-schema`'s own
  `{:db/valueType t :db/cardinality c :db/unique u :db/tupleTypes
  [t1 ...]}` shape) -> the SAME `{:value-type _ :cardinality _ :unique _
  :tuple-types _}` shape `schema-of` derives from already-installed
  datoms, so `transact-with-schema`'s `(merge (schema-of db) schema)`
  actually reconciles the two instead of one shape silently shadowing
  the other with mismatched keys.

  This fixes a real bug: an inline `{:db/valueType :long}` passed
  directly to `transact-with-schema` (never pre-installed via
  `install-schema`) was previously a SILENT no-op for value-type
  validation -- `effective-schema`'s entry for that attr ended up in the
  caller's raw `:db/valueType`-keyed shape, and `(get-in ... [p
  :value-type])` looked for a key (`:value-type`) that was never there,
  returning `nil` and skipping the check entirely."
  [schema]
  (into {}
        (map (fn [[attr decl]]
               [attr {:value-type (some-> (:db/valueType decl) name)
                      :cardinality (or (some-> (:db/cardinality decl) name) "many")
                      :unique (some-> (:db/unique decl) name)
                      :tuple-types (some->> (:db/tupleTypes decl)
                                            (mapv #(if (keyword? %) (name %) (str %))))}]))
        schema))

(defn transact-with-schema
  "Like `transact`, but OPT-IN schema enforcement (plain `transact` is
  completely unchanged -- every existing caller keeps working exactly as
  before): validates every tx-data item against `schema` (merged with
  whatever `schema-of` already derives from `db`, so a caller can declare
  brand-new attributes in the very same call that writes their first
  data, Datomic's own \"schema is just a transaction too\" idiom) BEFORE
  applying any of them.
    - an attribute with no schema entry (and not `bootstrap-attrs`, which
      describe schema itself and need none) throws -- Datomic's own
      \"attribute must be installed before use\" discipline.
    - a declared `:value-type` mismatch (checked against the ORIGINAL
      value, before `->quad`'s string coercion -- see `raw-triple`)
      throws.
    - a declared `:cardinality \"one\"` attribute automatically retracts
      any PRIOR value(s) for that `(s, p)` pair before asserting the new
      one -- this fn's own job, NOT `arrangement.core/assert-quad`'s
      (arrangement stays cardinality-agnostic per its own ns docstring;
      cardinality is a POLICY layered on top here, the same way
      `visible?`/`blind-fn`/`encrypt-fn` are layered rather than baked
      into arrangement).
    - a declared `:unique \"identity\"`/`\"value\"` attribute throws if
      the `(attr, value)` pair already belongs to a DIFFERENT entity.
  Applied in tx-data order against the RUNNING db, so a later item can
  rely on an earlier item's cardinality-one retraction having already
  happened."
  ([db tx-data schema] (transact-with-schema db tx-data ipld/link? schema))
  ([db tx-data ref? schema]
   (let [effective-schema (merge (schema-of db) (normalize-inline-schema schema))]
     (reduce
      (fn [db item]
        (let [[s p o] (raw-triple item)]
          (when-not (or (contains? bootstrap-attrs p) (contains? effective-schema p))
            (throw (ex-info "kotobase-peer: unknown attribute -- no schema declared (see install-schema)"
                            {:attr p})))
          (when-let [vt (get-in effective-schema [p :value-type])]
            (validate-value-type! p vt o (get-in effective-schema [p :tuple-types])))
          (when (= "identity" (get-in effective-schema [p :unique]))
            (let [quad-o (->quad-value o)
                  existing (qs/by-predicate-value db p quad-o)]
              (when (and (seq existing) (not= existing #{s}))
                (throw (ex-info "kotobase-peer: unique attribute violation -- value already belongs to a different entity"
                                {:attr p :value quad-o :existing existing})))))
          (let [db (if (= "one" (get-in effective-schema [p :cardinality] "many"))
                     (reduce (fn [db old-o] (qs/retract-quad db {:s s :p p :o old-o} ref?))
                             db
                             (disj (get (qs/entity-attrs db s) p #{}) (->quad-value o)))
                     db)]
            (qs/assert-quad db {:s s :p p :o (->quad-value o)} ref?))))
      db
      tx-data))))

(defn transact-with-report
  "Like `transact`, but returns a Datomic-shaped tx-report --
   `{:db-before :db-after :tx-data}` -- instead of the bare new db value.
   No `:tempids` (see `commit-with-report!`'s docstring for why)."
  ([db tx-data] (transact-with-report db tx-data ipld/link?))
  ([db tx-data ref?]
   {:db-before db :db-after (transact db tx-data ref?) :tx-data (mapv ->quad tx-data)}))

(defn with
  "Datomic's `with`: apply `tx-data` to `db` SPECULATIVELY and return the
   tx-report -- a pure function, db value in, `{:db-before :db-after
   :tx-data}` out, nothing persisted anywhere (no `put!`/`chain` touched
   at all). This substrate's `transact`/`transact-with-report` already
   ARE exactly this -- a hot db value in, a new hot db value out, no
   persistence -- so `with` is simply that mechanism under Datomic's own
   name, for a caller specifically looking for it."
  ([db tx-data] (transact-with-report db tx-data))
  ([db tx-data ref?] (transact-with-report db tx-data ref?)))

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

   `visible?` is REQUIRED (ADR-2607050500: Query is a first-class effect all
   the way up this stack, `q`'s own precedent -- no permissive default). It is
   applied as a post-filter over each candidate ROW, i.e. `(visible? {:e :a
   :v_edn :added})` -- the same `datomic.datoms`-shaped map this fn returns,
   NOT `arrangement.query`'s `{:s :p :o}` quad shape (`:e`/`:a` name the same
   positions as `:s`/`:p`, but `:v_edn` is already the wire-encoded EDN string,
   not the raw decoded value, and `:added` is present too) -- a caller
   filtering by entity/attribute (the common case: can this actor see this
   entity, is this attribute public) needs no decoding to do so; a caller
   that must decide on the raw value can `clojure.edn/read-string` `:v_edn`
   itself. Pass `(constantly true)` to see everything, as an explicit choice.

   2-arg (`[db visible?]`) → whole-db (:eavt full scan, unchanged). 3-arg
   additionally honors an optional `{:index :components :limit}` SERVER-SIDE
   via arrangement's index accessors, so a filtered read (e.g. getBackup by
   entity, or by [attr value]) materialises ONLY the matching entity/attr
   instead of the whole graph — the root fix for the deployed wasm worker
   ignoring index/components_edn/limit and rehydrating the full graph per read
   (ADR-2607022330 addendum 2).

   `:index` ∈ #{:eavt :aevt :avet} (default :eavt); `:components` is an ordered
   prefix in that index's key order (:eavt=[e a v], :aevt/:avet=[a … ]); values
   are the opaque strings arrangement stores (the PDS's :db/id for e, `:ns/attr`
   for a, the stored value string for v). `visible?` is applied BEFORE `:limit`
   is taken, so `:limit` caps the row count the caller actually receives (all
   of them visible), not the count of raw candidates scanned."
  ([db visible?] (datoms db nil visible?))
  ([db {:keys [index components limit]} visible?]
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
         rows (for [[e a v] triples] {:e e :a a :v_edn (v->edn v) :added true})
         rows (filter visible? rows)
         rows (cond->> rows limit (take limit))]
     (vec rows))))

(defn q
  "`datomic.q`-equivalent: `pattern` is `[s p o]` (nil = wildcard), routed to
   the matching index via `arrangement.query`. Returns a set of `{:s :p :o}`
   quads. NOTE: triple-pattern only, no join across clauses -- see `query`
   below for that.

   `visible?` is REQUIRED, cascaded straight from `arrangement.query/query`
   (ADR-2607050500: Query is a first-class effect all the way up this
   stack, not just at arrangement's layer) -- pass `(constantly true)` to
   see everything, as an explicit choice."
  [db pattern visible?]
  (kqe/query db pattern visible?))

(defn query
  "`datomic.api/q`-equivalent: `{:find [?var ...] :in [?param ...] :where
   [[e a v] ...] :rules [...]}` conjunctive multi-clause join over
   `arrangement.datalog/q` (ADR-2607061200 staged Datalog roadmap + query-
   language follow-up). `:where` clauses may be `(not [e a v])`,
   `(rule-name ?arg ...)`, `[(fn-sym arg...)]` / `[(fn-sym arg...)
   result-var]` (a whitelisted predicate/function call), or
   `(or clause ...)` / `(or-join [?shared ...] clause ...)`. `:find`
   elements may be `(count ?v)`/`(count-distinct ?v)`/`(sum ?v)`/
   `(avg ?v)`/`(min ?v)`/`(max ?v)` alongside plain variables, and
   `:rules` may define recursive relations (`[[(rule-name ?param ...)
   clause ...] ...]`, evaluated to a least fixpoint via semi-naive
   iteration). `inputs` (optional 4th arg, positional, matching `:in`'s
   order) supplies extra query parameters. See `arrangement.datalog`'s
   ns docstring for the full grammar and for why negation/recursion/
   `or` are all safe against `visible?` (enforced in arrangement, this
   fn is a straight passthrough). Returns a set of `:find`-ordered
   vectors. `visible?` is REQUIRED, same convention as `q` above."
  ([db find+where visible?] (datalog/q db find+where visible?))
  ([db find+where visible? inputs] (datalog/q db find+where visible? inputs)))

;; ── pull: a Datomic-shaped pull-pattern language over entity-attrs/refs-to ──
;; (one of the "3 pillars" this landing adds -- ADR-2607061200's own note
;; that pull's flat `{p #{o...}}` was a deliberately minimal Stage-0 landing).

(defn- pull-wildcard? [attr] (= attr '*))
(defn- pull-map-spec? [attr] (map? attr))
(defn- reverse-attr? [a] (str/starts-with? a "_"))
(defn- reverse-attr-name [a] (subs a 1))

(defn- pull1
  "Pull entity `s` per pull-pattern `pattern` (a vector of pull-attrs, see
  `pull`'s docstring). `seen` is the set of entity ids already being
  expanded on this recursion path -- an entity already on the path is
  returned as `{}` rather than re-expanded, so a cyclic graph can never
  loop forever, REGARDLESS of whether the caller wrote an explicit `n`/
  `'...` recursion limit or not (unconditional cycle safety, not just a
  courtesy for well-behaved callers)."
  [db s pattern seen]
  (let [attrs (qs/entity-attrs db s)         ; {p #{o...}}
        seen' (conj seen s)
        expand (fn [ref sub-pattern]
                 (if (contains? seen' ref) {} (pull1 db ref sub-pattern seen')))]
    (reduce
     (fn [acc attr]
       (cond
         (pull-wildcard? attr) (merge acc attrs)

         (and (string? attr) (reverse-attr? attr))
         (assoc acc attr (get (qs/refs-to db s) (reverse-attr-name attr) #{}))

         (string? attr)
         (assoc acc attr (get attrs attr #{}))

         (pull-map-spec? attr)
         (let [[a spec] (first attr)
               reverse? (reverse-attr? a)
               refs (if reverse? (get (qs/refs-to db s) (reverse-attr-name a) #{}) (get attrs a #{}))]
           (assoc acc a
                  (cond
                    (vector? spec)
                    (into #{} (map #(expand % spec)) refs)

                    (or (integer? spec) (= spec '...))
                    (if (and (integer? spec) (<= spec 0))
                      refs
                      (into #{} (map #(expand % ['* {a (if (integer? spec) (dec spec) '...)}])) refs))

                    :else acc)))

         :else acc))
     {}
     pattern)))

(defn pull
  "`datomic.pull`-equivalent for entity `s`. 2-arg form is unchanged: flat
   `{p #{o...}}` (arrangement has no cardinality tracking, so every
   attribute is multi-valued -- a caller expecting single-valued Datomic
   pull semantics must pick e.g. `first`).

   3-arg form takes a Datomic-shaped pull PATTERN, a vector of pull-attrs:
     - a plain attr string        -> that attr's value set, flat (as above)
     - `'*`                        -> every attr the entity has (wildcard)
     - `\"_attr\"` (leading `_`)   -> reverse nav: every entity referencing
                                      THIS one via `attr` (`refs`/VAET-style),
                                      flat
     - `{attr [sub-pattern ...]}`  -> nested pull: treat attr's value(s) (or,
                                      for `\"_attr\"`, its referrers) as
                                      entity ids and pull each with
                                      sub-pattern
     - `{attr n}` / `{attr '...}`  -> recursive wildcard pull through attr,
                                      `n` levels deep or unlimited -- cycle-
                                      safe either way (see `pull1`), so a
                                      graph cycle can't loop forever even
                                      with `'...`."
  ([db s] (qs/entity-attrs db s))
  ([db s pattern] (pull1 db s pattern #{})))

;; ── entity: lazy navigational entity API (ADR-2607061200 follow-up) ────────
;; Datomic's `entity` returns an object that behaves like a Clojure map but
;; transparently navigates ref-valued attributes into further entity
;; objects on access. Reimplementing that fully (a custom type overriding
;; `get`/keyword-invoke to be lazy) would need per-platform protocol
;; dispatch this codebase has ALREADY hit real portability limits on --
;; nbb/SCI cannot dispatch a custom deftype's implementation of a built-in
;; protocol at all (confirmed empirically, `io-ipld`/`org-ietf-cbor`'s own
;; deftype->defrecord history), and a `defrecord`'s map/field behavior is
;; fixed, not overridable to be lazy. So `entity` here is DELIBERATELY a
;; plain map (identical to `pull`'s 2-arg flat form, under Datomic's own
;; name) plus a separate, explicit `entity-attr` for the one thing that
;; makes `entity` more than `pull` -- navigating INTO a ref value's own
;; entity on demand, without needing a pull pattern known up front.

(defn entity
  "Datomic's `entity`: `pull`'s flat 2-arg form (`{p #{o...}}`) under
   Datomic's own name, for a caller specifically looking for it. See the
   ns section comment above for why this is a plain map, not a custom
   lazy-navigating type -- use `entity-attr` to navigate a ref-valued
   attribute's values into their own nested entities on demand."
  [db s]
  (qs/entity-attrs db s))

(defn entity-attr
  "Navigate FROM an already-`entity`/`pull`-ed map at `attr` INTO each of
   its values as their OWN `entity` -- the explicit lazy-navigation step
   `entity` itself doesn't take eagerly (see ns section comment). Returns
   `#{{...} ...}`, one nested entity map per value; a value that was
   never itself asserted as an entity's own subject simply resolves to
   `{}` (harmless -- same convention `pull`'s nested-ref form uses)."
  [db entity-map attr]
  (into #{} (map #(entity db %)) (get entity-map attr #{})))

(defn refs
  "`{p #{s...}}`: every entity `s` that references `ref` via predicate `p`
   (VAET-style reverse lookup, ADR-2607050200). Only populated for quads
   whose value was asserted as an `ipld.core/Link` — see `transact`'s default
   `ref?`. `ref` is the same Link value the referencing quad's `:o` was."
  [db ref]
  (qs/refs-to db ref))

;; ── persistence: content-addressed, chained, verifiable ─────────────────────
;;
;; state shape (opaque to chain; this peer library's own convention, encoded
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
   chain rooted at `prev-chain-cid`. This is the SAME expensive
   rebuild the pre-D1 `commit!` did on every write; it now exists only as a
   primitive `fold!` calls internally, plus a one-shot cold-start entry point
   for callers that already have a fully materialized hot `db` (backfill /
   migration tooling, tests). Most write paths want `commit!`, below.

   `blind-fn`/`encrypt-fn` (ADR-2607051000, accepted 2026-07-06) are REQUIRED
   and threaded straight to `arrangement.core/commit!` -- see its docstring
   for their contract, including the sync-JVM/Promise-cljs platform split
   this fn inherits (`arrangement.core/commit!` is itself a `js/Promise` on
   cljs; `cd/commit!` below is unaffected/synchronous on both platforms)."
  [put! get-fn db prev-chain-cid blind-fn encrypt-fn]
  #?(:clj
     (let [snapshot-cid (qs/commit! put! db nil qs/current-schema-version blind-fn encrypt-fn)]
       (cd/commit! put! get-fn
                   {"indexed" (ipld/link snapshot-cid) "novelty" []}
                   prev-chain-cid))
     :cljs
     (-> (qs/commit! put! db nil qs/current-schema-version blind-fn encrypt-fn)
         (.then (fn [snapshot-cid]
                  (cd/commit! put! get-fn
                              {"indexed" (ipld/link snapshot-cid) "novelty" []}
                              prev-chain-cid))))))

(defn- normalize-state [state]
  (cond
    (ipld/link? state) {"indexed" state "novelty" []}
    (map? state)       state
    (nil? state)       {"indexed" nil "novelty" []}
    :else (throw (ex-info "kotobase-peer: unrecognized commit state shape"
                          {:state state}))))

(defn- state-at
  "The normalized `{\"indexed\" ... \"novelty\" ...}` state at `chain-cid`
   (an O(1) head fetch since chain/head, ADR-2607032430's other half).
   nil chain-cid (no prior commit) → the empty state."
  [get-fn chain-cid]
  (if (nil? chain-cid)
    {"indexed" nil "novelty" []}
    (normalize-state (:state (cd/head get-fn chain-cid)))))

(defn- indexed-cid [state] (some-> (get state "indexed") ipld/link-cid))
(defn- novelty-cids [state] (mapv ipld/link-cid (get state "novelty" [])))

(defn- quad->wire [{:keys [s p o]}] {"s" s "p" p "o" o})
(defn- wire->quad [{:strs [s p o]}] {:s s :p p :o o})

(defn- put-tx-block!
  "ADR-2607051000 (accepted 2026-07-06): the novelty tx block's whole quad
  payload is encrypted as one opaque ciphertext blob (`encrypt-fn`, REQUIRED,
  no silent default) -- `read-tx-block` is whole-block reads only, never a
  keyed/prefix seek, so there's no blind-index concern here, just AEAD.
  Synchronous on JVM, a `js/Promise` of the block CID on cljs (see the
  platform-split note above)."
  [put! quads encrypt-fn]
  #?(:clj (ipld/put-node! put! {"ct" (encrypt-fn (ipld/encode {"quads" (mapv quad->wire quads)}))})
     :cljs (-> (encrypt-fn (ipld/encode {"quads" (mapv quad->wire quads)}))
               (.then (fn [ct] (ipld/put-node! put! {"ct" ct}))))))

(defn- read-tx-block
  "Inverse of `put-tx-block!`: decrypt the block's `\"ct\"` ciphertext
  (`decrypt-fn`, REQUIRED) back to the dag-cbor-encoded `{\"quads\" [...]}`
  node, then decode it as before. Synchronous on JVM, a `js/Promise` of the
  quads on cljs."
  [get-fn tx-cid decrypt-fn]
  #?(:clj (let [{:strs [ct]} (ipld/get-node get-fn tx-cid)]
            (mapv wire->quad (get (ipld/decode (decrypt-fn ct)) "quads")))
     :cljs (let [{:strs [ct]} (ipld/get-node get-fn tx-cid)]
             (-> (decrypt-fn ct)
                 (.then (fn [pt] (mapv wire->quad (get (ipld/decode pt) "quads"))))))))

(defn commit!
  "THE write path (ADR-2607032430 D1): append `tx-data` as one novelty tx
   block. O(|tx-data|) — reads only the previous state's {indexed, novelty}
   LINKS (one O(1) head fetch via chain, never the graph itself) and
   writes one new small tx block plus one new chain entry. Touches no
   prolly-tree, rehydrates nothing. `tx-data` accepts the same shapes
   `transact` does (quad maps, `[:db/add e a v]`, bare `[e a v]` triples).

   This REPLACES the pre-D1 `commit!` (now `snapshot!`), which took an
   already-hydrated hot db and paid a full O(graph) rebuild on every call —
   confirmed live 2026-07-03 as the root cause of a CF Worker CPU-limit
   collapse (error 1102) partway through a 321-actor mass write. Call
   `fold!` periodically (see `should-fold?`) to bound novelty size and keep
   `hot-datoms` reads fast; correctness never depends on folding promptly.

   `encrypt-fn` (ADR-2607051000, accepted 2026-07-06) is REQUIRED and
   threaded straight to `put-tx-block!` -- see its docstring for the
   contract, including the sync-JVM/Promise-cljs platform split (`put-tx-
   block!` is itself a `js/Promise` of the tx CID on cljs)."
  [put! get-fn tx-data prev-chain-cid encrypt-fn]
  (let [quads (mapv ->quad tx-data)
        state (state-at get-fn prev-chain-cid)]
    #?(:clj
       (let [tx-cid (put-tx-block! put! quads encrypt-fn)
             new-state (update state "novelty" (fnil conj []) (ipld/link tx-cid))]
         (cd/commit! put! get-fn new-state prev-chain-cid))
       :cljs
       (-> (put-tx-block! put! quads encrypt-fn)
           (.then (fn [tx-cid]
                    (let [new-state (update state "novelty" (fnil conj []) (ipld/link tx-cid))]
                      (cd/commit! put! get-fn new-state prev-chain-cid))))))))

(defn commit-with-report!
  "Like `commit!`, but returns a Datomic-shaped tx-report --
   `{:chain-cid-before prev-chain-cid :chain-cid-after new-chain-cid
   :tx-data quads}` -- instead of a bare chain-cid. Datomic's own
   `{:db-before :db-after :tx-data :tempids}`, adapted: this substrate's
   \"db identity\" for a chain-append write IS the chain-cid (content-
   addressed), not a hot db value, so `:chain-cid-before`/`-after` take
   `:db-before`/`-after`'s role. No `:tempids` -- this substrate has no
   temporary-id resolution step (callers pass real entity ids directly),
   so it's omitted rather than faked. Synchronous on JVM; a `js/Promise`
   of the report on cljs."
  [put! get-fn tx-data prev-chain-cid encrypt-fn]
  (let [quads (mapv ->quad tx-data)]
    #?(:clj
       {:chain-cid-before prev-chain-cid
        :chain-cid-after (commit! put! get-fn tx-data prev-chain-cid encrypt-fn)
        :tx-data quads}
       :cljs
       (-> (commit! put! get-fn tx-data prev-chain-cid encrypt-fn)
           (.then (fn [new-cid]
                    {:chain-cid-before prev-chain-cid :chain-cid-after new-cid :tx-data quads}))))))

;; ── write ACID: commit-serialized! ──────────────────────────────────────────
;; (one of the "3 pillars" this landing adds.) `chain.core/commit!`'s `:seq`
;; computation is a plain read-then-write with no atomicity (confirmed by
;; direct inspection): two concurrent writers can both derive the same
;; `:seq` from the same `prev-cid` and silently FORK the chain -- there is no
;; Transactor-equivalent single-writer serialization anywhere in this stack
;; today. Fixing that PROPERLY requires an atomic primitive no storage
;; backend already deployed here (R2, plain in-memory test stores) is
;; guaranteed to support -- so this is deliberately an OPT-IN, ADDITIVE new
;; function, not a change to `commit!`'s existing contract: a caller without
;; (or not wanting) a CAS-capable head-store keeps calling plain `commit!`
;; exactly as before, completely unaffected. `pds.aozora.app`'s already-
;; deployed chains and storage contract need zero changes for this to exist.

(def default-max-cas-retries
  "How many times `commit-serialized!` retries after losing a `cas!` race
  before giving up and throwing -- a defensive cap (persistent, unbounded
  write contention on one head-key is a caller/deployment problem, not
  something to retry forever for), not a tuned constant."
  10)

(defn commit-serialized!
  "Like `commit!`, but coordinates through a caller-provided `cas!` --
   `(cas! head-key expected-chain-cid new-chain-cid) -> actual-chain-cid`,
   an ATOMIC compare-and-swap on `head-key` (a caller-chosen MUTABLE
   pointer, e.g. a D1/DynamoDB row keyed by actor id -- NOT a chain-cid;
   chain-cids are content-addressed and never change meaning). Contract:
   if `head-key`'s CURRENT value equals `expected-chain-cid`, atomically
   swap it to `new-chain-cid` and return `new-chain-cid`; otherwise leave
   it untouched and return whatever its ACTUAL current value is (a
   standard compare-and-exchange shape -- the caller can always tell
   success from failure by comparing the return value to `new-chain-cid`,
   and a failure directly hands back what to retry against, no separate
   re-read needed) -- instead of blindly trusting a caller-passed
   `prev-chain-cid`.

   Re-derives `tx-data`'s tx-block against whatever the ACTUAL current
   head turns out to be on every attempt, retrying (up to `max-retries`)
   on a lost race, throwing if it never wins. True single-writer-per-
   `head-key` serialization -- the ACID guarantee `commit!` alone cannot
   make.

   `head-key` identifies the mutable pointer. `expected-chain-cid` is the
   chain-cid the CALLER currently believes is the head (`nil` means \"I
   believe this chain doesn't exist yet\"). `cas!` is assumed synchronous
   on BOTH platforms (matching `put!`/`get-fn`'s own existing convention
   in this file -- only the crypto step is ever async here); only
   `commit!`'s own crypto step needs promise-chaining on cljs."
  ([put! get-fn cas! head-key expected-chain-cid tx-data encrypt-fn]
   (commit-serialized! put! get-fn cas! head-key expected-chain-cid tx-data encrypt-fn default-max-cas-retries))
  ([put! get-fn cas! head-key expected-chain-cid tx-data encrypt-fn max-retries]
   #?(:clj
      (loop [current-cid expected-chain-cid, attempts 0]
        (when (> attempts max-retries)
          (throw (ex-info "kotobase-peer: commit-serialized! exceeded max-cas-retries -- persistent write contention on head-key"
                          {:head-key head-key :attempts attempts})))
        (let [new-cid (commit! put! get-fn tx-data current-cid encrypt-fn)
              actual (cas! head-key current-cid new-cid)]
          (if (= actual new-cid)
            new-cid
            (recur actual (inc attempts)))))
      :cljs
      (letfn [(attempt [current-cid attempts]
                (if (> attempts max-retries)
                  (js/Promise.reject (ex-info "kotobase-peer: commit-serialized! exceeded max-cas-retries -- persistent write contention on head-key"
                                              {:head-key head-key :attempts attempts}))
                  (-> (commit! put! get-fn tx-data current-cid encrypt-fn)
                      (.then (fn [new-cid]
                               (let [actual (cas! head-key current-cid new-cid)]
                                 (if (= actual new-cid)
                                   new-cid
                                   (attempt actual (inc attempts)))))))))]
        (attempt expected-chain-cid 0)))))

(defn commit-serialized-with-report!
  "Like `commit-serialized!`, but returns a Datomic-shaped tx-report (see
   `commit-with-report!`) -- `{:chain-cid-before :chain-cid-after
   :tx-data}`, where `:chain-cid-before` is whichever chain-cid actually
   won the CAS race and got committed against (NOT necessarily the
   caller's original `expected-chain-cid`, if a retry happened)."
  ([put! get-fn cas! head-key expected-chain-cid tx-data encrypt-fn]
   (commit-serialized-with-report! put! get-fn cas! head-key expected-chain-cid tx-data encrypt-fn default-max-cas-retries))
  ([put! get-fn cas! head-key expected-chain-cid tx-data encrypt-fn max-retries]
   (let [quads (mapv ->quad tx-data)]
     #?(:clj
        (loop [current-cid expected-chain-cid, attempts 0]
          (when (> attempts max-retries)
            (throw (ex-info "kotobase-peer: commit-serialized-with-report! exceeded max-cas-retries -- persistent write contention on head-key"
                            {:head-key head-key :attempts attempts})))
          (let [new-cid (commit! put! get-fn tx-data current-cid encrypt-fn)
                actual (cas! head-key current-cid new-cid)]
            (if (= actual new-cid)
              {:chain-cid-before current-cid :chain-cid-after new-cid :tx-data quads}
              (recur actual (inc attempts)))))
        :cljs
        (letfn [(attempt [current-cid attempts]
                  (if (> attempts max-retries)
                    (js/Promise.reject (ex-info "kotobase-peer: commit-serialized-with-report! exceeded max-cas-retries -- persistent write contention on head-key"
                                                {:head-key head-key :attempts attempts}))
                    (-> (commit! put! get-fn tx-data current-cid encrypt-fn)
                        (.then (fn [new-cid]
                                 (let [actual (cas! head-key current-cid new-cid)]
                                   (if (= actual new-cid)
                                     {:chain-cid-before current-cid :chain-cid-after new-cid :tx-data quads}
                                     (attempt actual (inc attempts)))))))))]
          (attempt expected-chain-cid 0))))))

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
   the RAW state as chain stored it; callers that need the normalized
   shape go through `state-at` / `hot-datoms` / `latest-snapshot-cid`)."
  [get-fn chain-cid]
  (cd/chain get-fn chain-cid))

(defn head
  "The chain entry AT `chain-cid` (O(1) — see chain/head). Returns
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

;; ── time-travel: as-of / hydrate-chain ──────────────────────────────────────
;; (one of the "3 pillars" this landing adds.) Keyed by commit `:seq`, NOT
;; wall-clock time (design decision: `chain.core`'s commit envelope has no
;; timestamp field, and adding one would change the CID of every future
;; commit for chains ALREADY deployed live -- pds.aozora.app actors keep
;; reading/writing with zero migration step, exactly as `normalize-state`'s
;; own comment already promises elsewhere in this file). The raw capability
;; already existed (`chain` walks the full history, `:state` included, for
;; ANY commit along the way) -- `as-of`/`hydrate-chain` just make it
;; ergonomic, without requiring a caller to hand-roll the walk.

(defn as-of
  "The chain-cid at-or-before `seq` (Datomic's `:as-of`, keyed by commit
   :seq number here -- see ns section comment). Given `:seq`'s gaplessness
   from 0 (`verify-chain`), this finds an exact match unless `seq` is
   beyond the chain's own tip, in which case it clamps to the tip
   (Datomic's own \"as-of the future just means now\" behavior). `nil` if
   `chain-cid` itself is `nil` (no prior commit at all yet).

   The result composes directly with every existing chain-cid-accepting
   fn -- `(hot-datoms get-fn (as-of get-fn chain-cid 5) ...)` reads
   \"hot-datoms as of seq 5\", no separate as-of-flavored sibling of each
   read fn needed."
  [get-fn chain-cid seq]
  (when chain-cid
    (let [entries (chain get-fn chain-cid)]
      (:cid (or (some #(when (= seq (:seq %)) %) entries)
                (last entries))))))

;; ── cold read: filtered datoms straight from a persisted snapshot ───────────
;; Closes the "nothing rehydrates a db from a snapshot CID" gap for the ONE case
;; that matters for scale: a FILTERED read. Instead of rebuilding the whole 4-index
;; db (the wasm worker's full-graph rehydrate that OOMs at 128MB), decode the
;; snapshot's index-roots, pick the ONE index tree the query needs, and prefix-seek
;; it by the components. arrangement persists each index as
;; `(pr-str [blind(k1) blind(k2) blind(v)]) → encrypt([k1 k2 v])` leaves
;; (index-root, ADR-2607051000 accepted 2026-07-06), so a components prefix
;; is a blinded string prefix on that tree, and a matched leaf's value must
;; be decrypted to recover the real triple (the key is one-way, unrecoverable).

;; index → snapshot key + how its [k1 k2 v] maps back to a datom {:e :a :v}
(def ^:private index-spec
  {:eavt {:root "spo" :->eav (fn [[s p o]] [s p o])}   ; spo: [s p o]
   :aevt {:root "pso" :->eav (fn [[p s o]] [s p o])}   ; pso: [p s o]
   :avet {:root "pos" :->eav (fn [[p o s]] [s p o])}}) ; pos: [p o s]

(defn- components-prefix
  "String prefix into a `(pr-str [k1 k2 v])` leaf key for an ordered component
  vector. `[\"a\" \"b\"]` → `[\"a\" \"b\" ` (up to the space before the next
  slot), so it matches every key whose first components are exactly those.

  ADR-2607051000 (accepted 2026-07-06): each component is run through
  `blind-fn` (REQUIRED, the SAME keyed MAC `arrangement.core/index-root` used
  to build the key) before printing, so a caller who already knows the
  plaintext component independently re-derives the identical blinded prefix
  bytes to seek on -- unchanged from before in every other respect (still no
  `link->edn` call here, matching this fn's pre-existing behavior for
  Link-valued components, an orthogonal, pre-existing gap this ADR doesn't
  touch). Synchronous on JVM (returns the prefix string, or nil for empty
  `components`); a `js/Promise` of the same on cljs."
  [components blind-fn]
  #?(:clj
     (when (seq components)
       (let [s (pr-str (mapv blind-fn components))]   ; e.g. ["blinded-a" "blinded-b"]
         (str (subs s 0 (dec (count s))) " ")))       ; drop ] , add the slot separator
     :cljs
     (if (seq components)
       (-> (pmap-async blind-fn components)
           (.then (fn [blinded]
                    (let [s (pr-str blinded)]
                      (str (subs s 0 (dec (count s))) " ")))))
       (js/Promise.resolve nil))))

(defn cold-datoms
  "`datomic.datoms`-shaped rows read DIRECTLY from a persisted snapshot without
  rehydrating the whole db. `snapshot-cid` is an arrangement commit CID
  (`latest-snapshot-cid`). opts = `{:index :components :limit}` as in `datoms`.
  Touches only the chosen index tree's blocks along the components prefix path
  (range-pruned via `prolly-tree.core/scan-prefix`), never the other three
  indexes — so a keyed read stays small regardless of graph size. `nil`
  snapshot-cid → `[]` (nothing folded yet; see `hot-datoms` for the
  novelty-inclusive read). get-fn: `(get-fn cid) -> bytes`.

  `visible?` is REQUIRED, same contract as `datoms`'s (ADR-2607050500): a
  post-filter over each candidate `{:e :a :v_edn :added}` row, applied BEFORE
  `:limit` is taken (so `:limit` caps what the caller actually receives).
  Pass `(constantly true)` to see everything, as an explicit choice. See
  `hydrate-db`, below, for the one internal caller that deliberately must NOT
  narrow this.

  `blind-fn`/`decrypt-fn` (ADR-2607051000, accepted 2026-07-06) are REQUIRED:
  `blind-fn` re-derives the seek prefix from known `:components` (see
  `components-prefix`); `decrypt-fn` opens each matched leaf's ENCRYPTED
  VALUE to recover the real `[k1 k2 v]` triple -- the key is a one-way blind
  token and can no longer be read back for the row's actual content (the
  design correction ADR-2607061800/2607061900's addendum made to
  ADR-2607051000's original, incorrect \"no separate value encryption
  needed\" claim). Synchronous on JVM; a `js/Promise` of the rows on cljs
  (since `components-prefix`/`decrypt-fn` are themselves Promise-returning
  there — `get-fn`/`pt/scan-prefix` stay synchronous on both platforms)."
  [get-fn snapshot-cid {:keys [index components limit]} visible? blind-fn decrypt-fn]
  #?(:clj
     (if (nil? snapshot-cid)
       []
       (let [{:keys [root ->eav]} (index-spec (or index :eavt))
             snap      (ipld/decode (get-fn snapshot-cid))
             root-cid  (some-> (get-in snap ["index-roots" root]) ipld/link-cid)
             entries   (if (nil? root-cid)
                         []
                         (pt/scan-prefix get-fn root-cid (or (components-prefix components blind-fn) "")))
             rows      (for [[_ ciphertext] entries
                             :let [[k1 k2 v3] (ipld/decode (decrypt-fn ciphertext))
                                   [e a v]    (->eav (mapv qs/edn->link [k1 k2 v3]))]]
                         {:e e :a a :v_edn (v->edn v) :added true})
             rows      (filter visible? rows)
             rows      (cond->> rows limit (take limit))]
         (vec rows)))
     :cljs
     (if (nil? snapshot-cid)
       (js/Promise.resolve [])
       (let [{:keys [root ->eav]} (index-spec (or index :eavt))
             snap (ipld/decode (get-fn snapshot-cid))
             root-cid (some-> (get-in snap ["index-roots" root]) ipld/link-cid)]
         (-> (components-prefix components blind-fn)
             (.then (fn [prefix]
                      (let [entries (if (nil? root-cid)
                                      []
                                      (pt/scan-prefix get-fn root-cid (or prefix "")))]
                        (pmap-async (fn [[_ ciphertext]]
                                      (.then (decrypt-fn ciphertext) ipld/decode))
                                    entries))))
             (.then (fn [triples]
                      (let [rows (for [[k1 k2 v3] triples
                                       :let [[e a v] (->eav (mapv qs/edn->link [k1 k2 v3]))]]
                                   {:e e :a a :v_edn (v->edn v) :added true})
                            rows (filter visible? rows)
                            rows (cond->> rows limit (take limit))]
                        (vec rows)))))))))

(defn hydrate-db
  "Rebuild the full hot 4-index `db` from a persisted snapshot. Reads ONE
  index tree (spo) in full and re-asserts every quad, so the reconstructed
  db is ~1× graph size. This is `fold!`'s internal starting point (hydrate
  the last-indexed snapshot, then re-assert novelty on top) — an O(graph_shard)
  operation, now paid once per fold instead of once per write. get-fn:
  `(get-fn cid) -> bytes`; nil snapshot → empty db.

  Deliberately calls `cold-datoms` with `(constantly true)`, NOT a caller-
  supplied `visible?`: this fn reconstructs the COMPLETE hot db for an
  internal structural purpose (`fold!`'s re-assert-novelty-then-resnapshot
  step), not a scoped read for an external caller. Any narrower `visible?`
  here would silently drop rows out of the rebuilt db — and since `fold!`
  then commits that db as the new persisted snapshot, a narrowed hydrate
  would bake permanent, silent data loss into the graph. Visibility belongs
  at the read surface (`datoms`/`cold-datoms`/`hot-datoms` called directly
  for a caller-facing read), never at this internal rehydration step.

  `blind-fn`/`decrypt-fn` (ADR-2607051000, accepted 2026-07-06) are REQUIRED
  and threaded straight to `cold-datoms` -- see its docstring for their
  contract, including the sync-JVM/Promise-cljs platform split (`cold-
  datoms` is itself a `js/Promise` of the rows on cljs)."
  [get-fn snapshot-cid blind-fn decrypt-fn]
  (letfn [(build [rows]
            (reduce (fn [db {:keys [e a v_edn]}]
                      (qs/assert-quad db {:s e :p a :o (qs/edn->link (edn/read-string v_edn))}))
                    (qs/empty-db)
                    rows))]
    #?(:clj
       (if (nil? snapshot-cid)
         (qs/empty-db)
         (build (cold-datoms get-fn snapshot-cid {:index :eavt} (constantly true) blind-fn decrypt-fn)))
       :cljs
       (if (nil? snapshot-cid)
         (js/Promise.resolve (qs/empty-db))
         (-> (cold-datoms get-fn snapshot-cid {:index :eavt} (constantly true) blind-fn decrypt-fn)
             (.then build))))))

(defn hydrate-chain
  "Rebuild the full hot db AS OF `chain-cid` — the persisted `indexed`
   snapshot PLUS any not-yet-folded `novelty` re-asserted on top, in append
   order. Unlike `hydrate-db` (indexed snapshot only, `fold!`'s internal
   building block), this matches exactly what `fold!` would commit if
   called at this exact point: `(hydrate-chain get-fn (as-of get-fn
   chain-cid seq) blind-fn decrypt-fn)` is a true database VALUE as of
   that point in history, safe to `q`/`query`/`pull` against like any
   other hot db.

   `blind-fn`/`decrypt-fn` (ADR-2607051000) REQUIRED, same contract as
   `hydrate-db`/`hot-datoms`. Synchronous on JVM; a `js/Promise` of the db
   on cljs."
  [get-fn chain-cid blind-fn decrypt-fn]
  (let [state (state-at get-fn chain-cid)]
    #?(:clj
       (let [base (hydrate-db get-fn (indexed-cid state) blind-fn decrypt-fn)
             novelty-quads (mapcat #(read-tx-block get-fn % decrypt-fn) (novelty-cids state))]
         (reduce qs/assert-quad base novelty-quads))
       :cljs
       (-> (js/Promise.all
            #js [(hydrate-db get-fn (indexed-cid state) blind-fn decrypt-fn)
                 (pmap-async (fn [cid] (read-tx-block get-fn cid decrypt-fn)) (novelty-cids state))])
           (.then (fn [results]
                    (let [[base novelty-quads-per-cid] (vec results)
                          novelty-quads (apply concat novelty-quads-per-cid)]
                      (reduce qs/assert-quad base novelty-quads))))))))

(defn- newly-added-tx-cids
  "Walk `chain-cid`'s full history: for each commit whose OWN novelty list
   grew by exactly one entry relative to the PREVIOUS commit (a plain
   `commit!` append, not a `fold!` compaction/reset) AND whose `:seq` is
   greater than `after-seq`, collect that commit's newest novelty tx-cid.
   A `fold!` commit itself contributes nothing (folding restructures
   already-seen data into an indexed snapshot, it doesn't add new user
   data) -- but any ORIGINAL `commit!` this walk already passed still
   counts regardless of a LATER fold, since this reads each commit's
   HISTORICAL state as it was at that point in the chain, not just the
   tip's current shape. Shared walk `since`/`history` both build on."
  [get-fn chain-cid after-seq]
  (loop [prev-count 0, remaining (chain get-fn chain-cid), acc []]
    (if (empty? remaining)
      acc
      (let [{:keys [seq state]} (first remaining)
            novelty (get (normalize-state state) "novelty" [])
            cur-count (count novelty)]
        (recur cur-count
               (rest remaining)
               (if (and (> seq after-seq) (= cur-count (inc prev-count)))
                 (conj acc (ipld/link-cid (last novelty)))
                 acc))))))

(defn since
  "A hot db containing ONLY the quads from commits with `:seq` greater
   than `since-seq` -- Datomic's `since`, a view of what changed AFTER a
   point (NOT merged with state as-of that point; for the merged
   \"everything up to and including now\" view, use `hydrate-chain` on
   the chain-cid itself). See `newly-added-tx-cids` for exactly which
   commits this includes. `decrypt-fn` (ADR-2607051000) REQUIRED.
   Synchronous on JVM; a `js/Promise` of the db on cljs."
  [get-fn chain-cid since-seq decrypt-fn]
  (let [tx-cids (newly-added-tx-cids get-fn chain-cid since-seq)]
    #?(:clj
       (reduce qs/assert-quad (qs/empty-db) (mapcat #(read-tx-block get-fn % decrypt-fn) tx-cids))
       :cljs
       (-> (pmap-async (fn [cid] (read-tx-block get-fn cid decrypt-fn)) tx-cids)
           (.then (fn [quads-per-cid] (reduce qs/assert-quad (qs/empty-db) (apply concat quads-per-cid))))))))

(defn- spo->quads [db]
  (for [[e attrs] (:spo db), [a vs] attrs, v vs] {:s e :p a :o v}))

(defn history
  "A hot db containing every quad EVER asserted via any commit rooted at
   `chain-cid`, across its WHOLE history from genesis to tip -- Datomic's
   `history`, scoped HONESTLY to what this substrate can actually track:
   assertions only, no retraction visibility. `commit!`'s novelty tx-
   block format has no add/retract flag at all -- two separate `commit!`
   calls asserting different values for the same `(s, p)` pair BOTH
   remain visible via `hot-datoms`/`as-of` too today (`arrangement.core`
   has no cardinality tracking of its own, ADR-2607061200 pillar note),
   so `history` does NOT yet distinguish itself from the current view by
   \"catching\" a retraction the current view has lost -- that would
   require a schema-aware, retraction-EMITTING variant of `commit!`
   itself (`transact-with-schema`'s cardinality `:one` retraction only
   ever happens in-memory, on a hot db value that's never persisted
   through `commit!`'s tx-block format), which is a natural follow-up,
   not yet built. What `history` DOES give today: a complete, literal
   audit trail across `fold!` compactions -- data folded away from
   `hot-datoms`' novelty view remains fully reconstructable here.

   Built from `(since get-fn chain-cid -1 decrypt-fn)` (every novelty
   tx-block ever appended, across the whole chain -- `-1` because every
   real commit's `:seq` is >= 0) UNION the tip's `indexed` snapshot
   (`hydrate-db`, which already subsumes everything folded before it).
   Overlap between the two (a commit whose novelty was later folded
   appears in both) is harmless: `arrangement.core`'s indices are sets,
   re-asserting an identical fact is a no-op.

   `blind-fn`/`decrypt-fn` (ADR-2607051000) REQUIRED, same contract as
   `hydrate-db`/`since`. Synchronous on JVM; a `js/Promise` of the db on
   cljs."
  [get-fn chain-cid blind-fn decrypt-fn]
  #?(:clj
     (let [novelty-db (since get-fn chain-cid -1 decrypt-fn)
           snap-db (hydrate-db get-fn (latest-snapshot-cid get-fn chain-cid) blind-fn decrypt-fn)]
       (reduce qs/assert-quad snap-db (spo->quads novelty-db)))
     :cljs
     (-> (js/Promise.all
          #js [(since get-fn chain-cid -1 decrypt-fn)
               (hydrate-db get-fn (latest-snapshot-cid get-fn chain-cid) blind-fn decrypt-fn)])
         (.then (fn [results]
                  (let [[novelty-db snap-db] (vec results)]
                    (reduce qs/assert-quad snap-db (spo->quads novelty-db))))))))

(defn hot-datoms
  "THE scale-safe read path (ADR-2607032430 D1): `datoms`-shaped rows as of
  `chain-cid`'s current state, merging the indexed snapshot (`cold-datoms` —
  range-pruned, untouched by graph size) with any not-yet-folded novelty
  (small, bounded by the fold threshold). Novelty filtering reuses `datoms`'s
  own index/components logic (build a throwaway hot db from just the
  novelty quads, filter it the normal way) — exactly consistent with the
  snapshot path's semantics, no separate filter implementation to drift.
  Replaces `hydrate-db` + `datoms` for any caller that doesn't need a full
  hot db value. `nil` chain-cid → `[]`.

  `visible?` is REQUIRED (ADR-2607050500, same contract as `datoms`/
  `cold-datoms`: a post-filter over each `{:e :a :v_edn :added}` row).
  Forwarded UNCHANGED to both `cold-datoms` (for the snapshot half) and
  `datoms` (for the novelty half) — each half is filtered exactly once, by
  its own producer, before the two row seqs are concatenated, so nothing is
  filtered twice and neither path (snapshot or novelty) is missed. Pass
  `(constantly true)` to see everything, as an explicit choice.

  `blind-fn`/`decrypt-fn` (ADR-2607051000, accepted 2026-07-06) are REQUIRED:
  `blind-fn`/`decrypt-fn` go to `cold-datoms` (the snapshot half);
  `decrypt-fn` also goes to `read-tx-block` (the novelty half, whole-block
  decrypt, no blinding needed there). Synchronous on JVM; a `js/Promise` of
  the rows on cljs (the snapshot and novelty halves resolve concurrently
  there, via `js/Promise.all`, since they're independent)."
  ([get-fn chain-cid visible? blind-fn decrypt-fn]
   (hot-datoms get-fn chain-cid nil visible? blind-fn decrypt-fn))
  ([get-fn chain-cid opts visible? blind-fn decrypt-fn]
   #?(:clj
      (if (nil? chain-cid)
        []
        (let [state (state-at get-fn chain-cid)
              snap-rows (cold-datoms get-fn (indexed-cid state) opts visible? blind-fn decrypt-fn)
              novelty-quads (mapcat #(read-tx-block get-fn % decrypt-fn) (novelty-cids state))
              novelty-rows (datoms (reduce qs/assert-quad (qs/empty-db) novelty-quads) opts visible?)]
          (vec (concat snap-rows novelty-rows))))
      :cljs
      (if (nil? chain-cid)
        (js/Promise.resolve [])
        (let [state (state-at get-fn chain-cid)]
          (-> (js/Promise.all
               #js [(cold-datoms get-fn (indexed-cid state) opts visible? blind-fn decrypt-fn)
                    (pmap-async (fn [cid] (read-tx-block get-fn cid decrypt-fn)) (novelty-cids state))])
              ;; `vec`, NOT `js->clj`: every element `js/Promise.all` resolves here is
              ;; already realized ClojureScript data (a Link is an `ipld.core/Link`
              ;; defrecord, which is iterable) -- `js->clj` walks anything iterable and
              ;; would shred a Link back down into a bare `([:cid ...])` seq, silently
              ;; losing its type (confirmed regression, ADR-2607051000 follow-up).
              (.then (fn [results]
                       (let [[snap-rows novelty-quads-per-cid] (vec results)
                             novelty-quads (apply concat novelty-quads-per-cid)
                             novelty-rows (datoms (reduce qs/assert-quad (qs/empty-db) novelty-quads) opts visible?)]
                         (vec (concat snap-rows novelty-rows)))))))))))

(defn fold!
  "Compact `chain-cid`'s novelty into a fresh indexed snapshot: hydrate the
  current `indexed` snapshot, re-assert every novelty quad on top (append
  order), `qs/commit!` that as the new `indexed`, and append ONE new
  chain entry with an empty `novelty`. Cost: O(graph_shard) — the same
  full rebuild `snapshot!`/the pre-D1 `commit!` always paid, now amortized
  over however many `commit!` writes accumulated the folded novelty instead
  of being paid on every single one (`should-fold?`/`novelty-size` decide
  when to call this; correctness never depends on it running promptly).

  Deterministic: prolly-tree/arrangement are content-addressed, so folding
  the identical (indexed, novelty) pair — from ANY writer, a server cron or
  a browser at idle — always produces the same snapshot CID. Concurrent
  folds of the same state are safe, redundant, and cheap (re-`put!`ing
  already-stored bytes is a no-op at the block-store layer). NOTE
  (ADR-2607051000, accepted 2026-07-06): this determinism now additionally
  depends on `encrypt-fn` deriving its nonce deterministically from the
  plaintext rather than randomly -- a random-nonce `encrypt-fn` would make
  even identical (indexed, novelty) pairs fold to DIFFERENT snapshot CIDs,
  silently losing the \"cheap redundant fold\" property this paragraph
  otherwise promises. Callers should supply a deterministic `encrypt-fn`
  (e.g. nonce = HMAC(nonce-key, plaintext)) if this property matters to them.

  `blind-fn`/`encrypt-fn`/`decrypt-fn` are REQUIRED and threaded to
  `hydrate-db` (blind-fn, decrypt-fn), `read-tx-block` (decrypt-fn), and
  `arrangement.core/commit!` (blind-fn, encrypt-fn). Synchronous on JVM; a
  `js/Promise` of the new chain CID on cljs (the novelty-quads read and the
  snapshot hydrate resolve concurrently there, via `js/Promise.all`, since
  they're independent)."
  ([put! get-fn chain-cid blind-fn encrypt-fn decrypt-fn]
   (fold! put! get-fn chain-cid ipld/link? blind-fn encrypt-fn decrypt-fn))
  ([put! get-fn chain-cid ref? blind-fn encrypt-fn decrypt-fn]
   (let [state (state-at get-fn chain-cid)]
     #?(:clj
        (let [novelty-quads (mapcat #(read-tx-block get-fn % decrypt-fn) (novelty-cids state))
              db (reduce (fn [db q] (qs/assert-quad db q ref?))
                         (hydrate-db get-fn (indexed-cid state) blind-fn decrypt-fn)
                         novelty-quads)
              new-snap-cid (qs/commit! put! db nil qs/current-schema-version blind-fn encrypt-fn)
              new-state {"indexed" (ipld/link new-snap-cid) "novelty" []}]
          (cd/commit! put! get-fn new-state chain-cid))
        :cljs
        (-> (js/Promise.all
             #js [(pmap-async (fn [cid] (read-tx-block get-fn cid decrypt-fn)) (novelty-cids state))
                  (hydrate-db get-fn (indexed-cid state) blind-fn decrypt-fn)])
            (.then (fn [results]
                     (let [[novelty-quads-per-cid hydrated-db] (vec results)
                           novelty-quads (apply concat novelty-quads-per-cid)
                           db (reduce (fn [db q] (qs/assert-quad db q ref?)) hydrated-db novelty-quads)]
                       (qs/commit! put! db nil qs/current-schema-version blind-fn encrypt-fn))))
            (.then (fn [new-snap-cid]
                     (let [new-state {"indexed" (ipld/link new-snap-cid) "novelty" []}]
                       (cd/commit! put! get-fn new-state chain-cid)))))))))

(defn verify-chain
  "True iff the chain rooted at `chain-cid` is untampered and its
   `:seq` values are gapless from 0. Does NOT verify the prolly-tree
   snapshots or tx blocks each commit's `:state` links to are themselves
   intact (that would require walking every index/tx-block — a follow-up)."
  [get-fn chain-cid]
  (cd/verify-chain get-fn chain-cid))
