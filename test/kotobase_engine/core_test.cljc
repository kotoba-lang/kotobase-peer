(ns kotobase-engine.core-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [clojure.set :as set]
            [ipld.core :as ipld]
            [commit-dag.core :as cd]
            [quad-store.core :as qs]
            [kotobase-engine.core :as eng]))

(defn- mem-store []
  (let [store (atom {})]
    {:put! (fn [cid bytes] (swap! store assoc cid bytes))
     :get-fn (fn [cid] (get @store cid))
     :store store}))

(deftest transact-and-datoms-roundtrip
  (testing "quad maps, [:db/add e a v], and bare [e a v] triples all normalize the same way"
    (let [db (-> (eng/empty-db)
                 (eng/transact [{:s "alice" :p "role" :o "admin"}
                                 [:db/add "alice" "name" "Alice"]
                                 ["bob" "role" "user"]]))
          rows (eng/datoms db)]
      (is (= 3 (count rows)))
      (is (every? #(and (:e %) (:a %) (:v_edn %) (:added %)) rows))
      (is (= #{"\"admin\"" "\"Alice\"" "\"user\""} (set (map :v_edn rows)))))))

(deftest datoms-honors-index-components-limit
  ;; Models the getBackup shape: several entities, fetch ONE by an index prefix
  ;; instead of scanning the whole graph (ADR-2607022330 addendum 2).
  (let [db (eng/transact (eng/empty-db)
                          [["keybackup/did:key:zAlice" ":aozora.keyBackup/did" "did:key:zAlice"]
                           ["keybackup/did:key:zAlice" ":aozora.keyBackup/blob" "{blobA}"]
                           ["keybackup/did:key:zBob"   ":aozora.keyBackup/did" "did:key:zBob"]
                           ["keybackup/did:key:zBob"   ":aozora.keyBackup/blob" "{blobB}"]
                           ["acct/alice" ":atproto.account/handle" "alice.aozora.app"]])]
    (testing "no opts → whole-db scan (unchanged)"
      (is (= 5 (count (eng/datoms db))))
      (is (= 5 (count (eng/datoms db nil)))))
    (testing ":eavt + [e] returns ONLY that entity's datoms (the getBackup query)"
      (let [rows (eng/datoms db {:index :eavt :components ["keybackup/did:key:zAlice"]})]
        (is (= 2 (count rows)))
        (is (every? #(= "keybackup/did:key:zAlice" (:e %)) rows))
        (is (= #{":aozora.keyBackup/did" ":aozora.keyBackup/blob"} (set (map :a rows))))))
    (testing ":eavt + [e a] narrows to one attribute"
      (is (= [{:e "keybackup/did:key:zAlice" :a ":aozora.keyBackup/blob"
               :v_edn "\"{blobA}\"" :added true}]
             (eng/datoms db {:index :eavt
                             :components ["keybackup/did:key:zAlice" ":aozora.keyBackup/blob"]}))))
    (testing ":avet + [attr value] point-looks-up the subject(s)"
      (let [rows (eng/datoms db {:index :avet
                                 :components [":aozora.keyBackup/did" "did:key:zBob"]})]
        (is (= [{:e "keybackup/did:key:zBob" :a ":aozora.keyBackup/did"
                 :v_edn "\"did:key:zBob\"" :added true}] rows))))
    (testing ":avet + [attr] returns all datoms for that attribute"
      (is (= 2 (count (eng/datoms db {:index :avet :components [":aozora.keyBackup/did"]})))))
    (testing ":aevt + [attr] scans one attribute"
      (is (= 2 (count (eng/datoms db {:index :aevt :components [":aozora.keyBackup/blob"]})))))
    (testing ":limit caps rows"
      (is (= 1 (count (eng/datoms db {:index :avet :components [":aozora.keyBackup/did"] :limit 1})))))
    (testing "missing entity/attr → empty, not a full scan"
      (is (= [] (eng/datoms db {:index :eavt :components ["keybackup/nope"]})))
      (is (= [] (eng/datoms db {:index :avet :components [":aozora.keyBackup/did" "did:key:zNope"]}))))))

(deftest cold-datoms-reads-filtered-from-snapshot
  ;; The scale fix: read a filtered set DIRECTLY from a persisted snapshot,
  ;; never rehydrating the whole db (ADR-2607022330 addendum 2, #13).
  (let [{:keys [put! get-fn]} (mem-store)
        db (eng/transact (eng/empty-db)
                          [["keybackup/zAlice" ":aozora.keyBackup/did" "did:key:zAlice"]
                           ["keybackup/zAlice" ":aozora.keyBackup/blob" "{blobA}"]
                           ["keybackup/zBob"   ":aozora.keyBackup/did" "did:key:zBob"]
                           ["acct/alice" ":atproto.account/handle" "alice.aozora.app"]])
        chain-cid (eng/snapshot! put! get-fn db nil)
        snap      (eng/latest-snapshot-cid get-fn chain-cid)]
    (testing "cold :eavt [e] equals the hot filter (the getBackup query)"
      (is (= (set (eng/datoms db {:index :eavt :components ["keybackup/zAlice"]}))
             (set (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/zAlice"]}))))
      (is (= 2 (count (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/zAlice"]})))))
    (testing "cold :avet [attr value] point lookup returns one datom"
      (is (= [{:e "keybackup/zBob" :a ":aozora.keyBackup/did"
               :v_edn "\"did:key:zBob\"" :added true}]
             (eng/cold-datoms get-fn snap {:index :avet
                                           :components [":aozora.keyBackup/did" "did:key:zBob"]}))))
    (testing "cold :avet [attr] returns all subjects for the attribute"
      (is (= 2 (count (eng/cold-datoms get-fn snap {:index :avet
                                                    :components [":aozora.keyBackup/did"]})))))
    (testing ":limit caps cold rows"
      (is (= 1 (count (eng/cold-datoms get-fn snap {:index :avet
                                                    :components [":aozora.keyBackup/did"] :limit 1})))))
    (testing "missing entity/value → empty"
      (is (= [] (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/nope"]})))
      (is (= [] (eng/cold-datoms get-fn snap {:index :avet
                                              :components [":aozora.keyBackup/did" "did:key:zNope"]}))))))

(deftest hydrate-db-roundtrips-the-whole-db
  ;; write path: persist → hydrate → the reconstructed db equals the original,
  ;; and a follow-on transact+commit chains cleanly (#14 worker's transact uses this).
  (let [{:keys [put! get-fn]} (mem-store)
        db0 (eng/transact (eng/empty-db)
                          [["alice" "role" "admin"] ["alice" "name" "Alice"]
                           ["bob" "role" "user"]])
        c0  (eng/snapshot! put! get-fn db0 nil)
        s0  (eng/latest-snapshot-cid get-fn c0)
        db1 (eng/hydrate-db get-fn s0)]
    (is (= (set (eng/datoms db0)) (set (eng/datoms db1))) "hydrated db == original")
    (testing "hydrate → assert → commit is a clean incremental write"
      (let [db2 (eng/transact db1 [["carol" "role" "guest"]])
            c1  (eng/snapshot! put! get-fn db2 c0)
            s1  (eng/latest-snapshot-cid get-fn c1)]
        (is (= 3 (count (eng/cold-datoms get-fn s1 {:index :aevt :components ["role"]}))))
        (is (= [{:e "carol" :a "role" :v_edn "\"guest\"" :added true}]
               (eng/cold-datoms get-fn s1 {:index :avet :components ["role" "guest"]})))))
    (testing "nil snapshot → empty db"
      (is (= [] (eng/datoms (eng/hydrate-db get-fn nil)))))))

(deftest q-routes-through-kqe
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "role" :o "admin"}
                           {:s "bob" :p "role" :o "user"}])
        everything (constantly true)]
    (is (= #{{:s "alice" :p "role" :o "admin"}} (eng/q db ["alice" nil nil] everything)))
    (is (= #{{:s "alice" :p "role" :o "admin"}} (eng/q db [nil "role" "admin"] everything)))
    (is (= 2 (count (eng/q db [nil nil nil] everything))))))

(deftest q-visible-is-required
  ;; a non-empty db: an omitted visible? must actually be invoked as a
  ;; predicate (and fail) for this to be a meaningful cross-platform check --
  ;; on an empty db, cljs's `filter` never calls the missing predicate at
  ;; all, so the arity mismatch would silently NOT surface.
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])]
    (is (thrown? #?(:clj clojure.lang.ArityException :cljs js/Error)
                 (eng/q db [nil nil nil]))
        "q cascades kqe's required visibility decision -- no permissive default")))

(deftest pull-returns-entity-attrs
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}
                                          {:s "alice" :p "name" :o "Alice"}])]
    (is (= {"role" #{"admin"} "name" #{"Alice"}} (eng/pull db "alice")))))

(deftest transact-is-immutable
  (let [db0 (eng/empty-db)
        db1 (eng/transact db0 [{:s "alice" :p "role" :o "admin"}])]
    (is (= 0 (count (eng/datoms db0))))
    (is (= 1 (count (eng/datoms db1))))))

(deftest commit-snapshots-are-content-addressed-and-deterministic
  (testing "committing the identical db content twice, from two independent
            chains/stores, yields the same snapshot CID -- quad-store's own
            content-addressing guarantee, exercised end to end through this
            namespace's snapshot! composition"
    (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
          {:keys [put! get-fn]} (mem-store)
          {put2! :put! get-fn2 :get-fn} (mem-store)
          a (:state (eng/head get-fn (eng/snapshot! put! get-fn db nil)))
          b (:state (eng/head get-fn2 (eng/snapshot! put2! get-fn2 db nil)))]
      (is (= a b)))))

(deftest commit-dag-chain-tracks-multiple-snapshots
  (let [{:keys [put! get-fn]} (mem-store)
        db1 (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
        db2 (eng/transact db1 [{:s "bob" :p "role" :o "user"}])
        c0 (eng/snapshot! put! get-fn db1 nil)
        c1 (eng/snapshot! put! get-fn db2 c0)
        history (eng/chain get-fn c1)]
    (is (= 2 (count history)))
    (is (= [0 1] (map :seq history)))
    (is (not= (:state (first history)) (:state (second history)))
        "different db content -> different snapshot CID")
    (is (= (eng/latest-snapshot-cid get-fn c1)
           (ipld/link-cid (get (:state (last history)) "indexed")))
        "chain state is {\"indexed\" Link \"novelty\" []} wrapping the snapshot CID")
    (is (true? (eng/verify-chain get-fn c1)))))

(deftest verify-chain-catches-a-store-that-lies
  (let [{:keys [put! get-fn store]} (mem-store)
        db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
        c0 (eng/snapshot! put! get-fn db nil)
        db2 (eng/transact db [{:s "bob" :p "role" :o "user"}])
        c1 (eng/snapshot! put! get-fn db2 c0)
        other-cid (eng/snapshot! put! get-fn (eng/transact (eng/empty-db)
                                                            [{:s "mallory" :p "role" :o "evil"}])
                                  nil)]
    (is (true? (eng/verify-chain get-fn c1)))
    ;; splice a DIFFERENT (but validly dag-cbor-encoded) commit's bytes under
    ;; c0's own cid key -- a dishonest/corrupted store, without changing c0
    ;; itself. verify-chain must catch this via CID re-derivation, not throw.
    (swap! store assoc c0 (get @store other-cid))
    (is (false? (eng/verify-chain get-fn c1)))))

;; ── D1: novelty-log write path (ADR-2607032430) ─────────────────────────────

(deftest commit-bang-appends-novelty-without-touching-snapshot
  (testing "commit! on a fresh chain writes ONLY novelty -- nothing folded yet"
    (let [{:keys [put! get-fn]} (mem-store)
          c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil)]
      (is (= 1 (eng/novelty-size get-fn c0)))
      (is (nil? (eng/latest-snapshot-cid get-fn c0))
          "nothing folded yet -- an all-novelty chain has no indexed snapshot")
      (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}]
             (eng/hot-datoms get-fn c0))
          "hot-datoms still sees the data via novelty merge"))))

(deftest commit-bang-multiple-writes-accumulate-novelty
  (let [{:keys [put! get-fn]} (mem-store)
        c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil)
        c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0)
        c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] c1)]
    (is (= 3 (eng/novelty-size get-fn c2)))
    (is (= [0 1 2] (map :seq (eng/chain get-fn c2))))
    (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
             {:e "bob" :a "role" :v_edn "\"user\"" :added true}
             {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
           (set (eng/hot-datoms get-fn c2)))
        "no loss/dup across three sequential novelty-append commits")))

(deftest hot-datoms-merges-indexed-and-novelty
  (let [{:keys [put! get-fn]} (mem-store)
        c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil)
        c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0)
        folded (eng/fold! put! get-fn c1)
        c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] folded)]
    (is (some? (eng/latest-snapshot-cid get-fn c2)) "folded snapshot carries forward")
    (is (= 1 (eng/novelty-size get-fn c2)) "only the post-fold write is novelty")
    (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
             {:e "bob" :a "role" :v_edn "\"user\"" :added true}
             {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
           (set (eng/hot-datoms get-fn c2)))
        "reads see both the folded-in history AND the fresh novelty")
    (testing "filtered hot-datoms honors opts across the snapshot/novelty split"
      (is (= [{:e "carol" :a "role" :v_edn "\"guest\"" :added true}]
             (eng/hot-datoms get-fn c2 {:index :eavt :components ["carol"]})))
      (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}]
             (eng/hot-datoms get-fn c2 {:index :eavt :components ["alice"]}))))))

(deftest fold-bang-compacts-novelty-and-preserves-data
  (let [{:keys [put! get-fn]} (mem-store)
        c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil)
        c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0)
        before (set (eng/hot-datoms get-fn c1))
        folded (eng/fold! put! get-fn c1)]
    (is (= 0 (eng/novelty-size get-fn folded)) "fold resets the tail")
    (is (some? (eng/latest-snapshot-cid get-fn folded)))
    (is (= before (set (eng/hot-datoms get-fn folded)))
        "folding never loses or duplicates data")
    (is (= before (set (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil)))
        "the fold really did index the data -- a cold-only read (no novelty) sees it")))

(deftest fold-bang-is-deterministic-across-independent-stores
  (testing "folding the identical (indexed, novelty) history from two independent
            stores yields the same snapshot CID -- content-addressing holds
            through the fold, so concurrent/redundant folds converge safely"
    (let [mk-fold (fn []
                    (let [{:keys [put! get-fn]} (mem-store)
                          c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil)
                          c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0)]
                      (eng/latest-snapshot-cid get-fn (eng/fold! put! get-fn c1))))]
      (is (= (mk-fold) (mk-fold))))))

(deftest should-fold-flags-at-threshold
  (let [{:keys [put! get-fn]} (mem-store)
        c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil)
        c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0)]
    (is (false? (eng/should-fold? get-fn c1 3)))
    (is (true? (eng/should-fold? get-fn c1 2)))
    (is (false? (eng/should-fold? get-fn c1)) "default threshold is well above 2")))

(deftest normalize-state-reads-pre-d1-bare-link-chains
  (testing "a chain committed by the pre-D1 code (state = a bare snapshot Link,
            no {indexed novelty} wrapper) still reads correctly under the new
            code -- zero migration step for already-deployed actors"
    (let [{:keys [put! get-fn]} (mem-store)
          db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
          snap-cid (qs/commit! put! db nil qs/current-schema-version)
          pre-d1-chain (cd/commit! put! get-fn (ipld/link snap-cid) nil)]
      (is (= snap-cid (eng/latest-snapshot-cid get-fn pre-d1-chain)))
      (is (= 0 (eng/novelty-size get-fn pre-d1-chain)))
      (is (= (set (eng/datoms db)) (set (eng/hot-datoms get-fn pre-d1-chain))))
      (testing "committing new novelty on top of a pre-D1 chain works (mixed-era chain)"
        (let [c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] pre-d1-chain)]
          (is (= 1 (eng/novelty-size get-fn c1)))
          (is (= (conj (set (eng/datoms db))
                       {:e "bob" :a "role" :v_edn "\"user\"" :added true})
                 (set (eng/hot-datoms get-fn c1)))))))))

;; ── canonical datom model (datom-clj) — ADR-2607032500 ───────────────────────
(deftest datafy-via-canonical-datom-model
  (testing "entities->datoms uses datom.core/eavt: :db/id → e, other pairs → [e a v]"
    (is (= [["e1" :ns/a "v1"] ["e1" :ns/b "v2"]]
           (eng/entities->datoms [{:db/id "e1" :ns/a "v1" :ns/b "v2"}]))))
  (testing "transact-tx (entity tx-maps) ≡ transact (equivalent [e a v] triples)"
    (let [ents [{:db/id "alice" :atproto.account/did "alice" :atproto.account/handle "a.app"}
                {:db/id "keybackup/alice" :aozora.keyBackup/did "alice"}]
          via-tx  (eng/transact-tx (eng/empty-db) ents)
          via-raw (eng/transact (eng/empty-db)
                                [["alice" :atproto.account/did "alice"]
                                 ["alice" :atproto.account/handle "a.app"]
                                 ["keybackup/alice" :aozora.keyBackup/did "alice"]])]
      (is (= (set (eng/datoms via-raw)) (set (eng/datoms via-tx)))
          "the DB engine's entity datafication == kotoba's shared [e a v] model"))))

;; ── ref? naturalizes to ipld/link? (ADR-2607050200, ADR-2607023200 §6-4) ─────

(def ^:private bob-link
  (ipld/link "bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m"))

(deftest transact-auto-indexes-link-values-as-refs
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "knows" :o bob-link}
                           {:s "alice" :p "name" :o "Alice"}])]
    (testing "a Link-valued object is reverse-indexed by default -- no explicit ref? needed"
      (is (= {"knows" #{"alice"}} (eng/refs db bob-link))))
    (testing "a plain string is never mistaken for a ref"
      (is (= {} (eng/refs db "Alice"))))
    (testing "the Link survives into datoms' wire encoding, readable, no custom reader"
      (is (= #{["knows" "[\"ipld/link\" \"bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m\"]"]
               ["name" "\"Alice\""]}
             (set (map (juxt :a :v_edn) (eng/datoms db))))))))

(deftest link-ref-survives-fold-and-cold-read
  (testing "a Link-valued datom, folded into a persisted snapshot and read back
            cold, is still a real Link -- and refs/refs-to still finds it on the
            rehydrated hot db (the full novelty -> fold -> cold-read round trip)"
    (let [{:keys [put! get-fn]} (mem-store)
          c0 (eng/commit! put! get-fn [{:s "alice" :p "knows" :o bob-link}] nil)
          folded (eng/fold! put! get-fn c0)
          snap (eng/latest-snapshot-cid get-fn folded)]
      (testing "cold-datoms reconstructs the Link (not the raw edn-safe vector)"
        (let [row (first (eng/cold-datoms get-fn snap {:index :eavt :components ["alice"]}))]
          (is (= "[\"ipld/link\" \"bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m\"]"
                 (:v_edn row)))))
      (testing "hydrate-db reconstructs a real Link, so refs-to finds it again"
        (let [db (eng/hydrate-db get-fn snap)]
          (is (= {"knows" #{"alice"}} (qs/refs-to db bob-link)))))
      (testing "hot-datoms (novelty path, via dag-cbor) agrees with the cold path"
        (is (= (set (eng/hot-datoms get-fn c0))
               (set (eng/cold-datoms get-fn snap nil))))))))
