(ns kotobase-peer.core-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [clojure.set :as set]
            [ipld.core :as ipld]
            [chain.core :as cd]
            [arrangement.core :as qs]
            [kotobase-peer.core :as eng])
  #?(:clj (:import [javax.crypto Cipher Mac]
                   [javax.crypto.spec SecretKeySpec GCMParameterSpec]
                   [java.util Base64])))

(defn- mem-store []
  (let [store (atom {})]
    {:put! (fn [cid bytes] (swap! store assoc cid bytes))
     :get-fn (fn [cid] (get @store cid))
     :store store}))

;; ── ADR-2607051000 test crypto (accepted 2026-07-06) ────────────────────────
;; Real AES-256-GCM + HMAC-SHA256, not a mock -- JVM (javax.crypto) only for
;; now. Worker/`crypto.subtle` support is an explicit, separately-tracked
;; follow-up (ADR-2607061900's sequencing decision), so every test that calls
;; `eng/snapshot!`/`eng/commit!`/`eng/cold-datoms`/`eng/hydrate-db`/
;; `eng/hot-datoms`/`eng/fold!` (all of which now REQUIRE `blind-fn`/
;; `encrypt-fn`/`decrypt-fn`, no silent default) is wrapped `#?(:clj ...)`
;; below; the `:cljs` build simply doesn't compile them yet, same as
;; `kotoba-lang/arrangement`'s own test suite handles this identically.
;;
;; The nonce is HMAC-derived (deterministic per plaintext), not random -- see
;; `arrangement.core-test`'s identical helper for why: `snapshot!`/`fold!`'s
;; content-addressing idempotency depends on it.
#?(:clj
   (do
     (def ^:private test-dek
       (SecretKeySpec. (byte-array (range 1 33)) "AES"))
     (def ^:private test-blind-key
       (SecretKeySpec. (byte-array (range 33 65)) "HmacSHA256"))
     (def ^:private test-nonce-key
       (SecretKeySpec. (byte-array (range 65 97)) "HmacSHA256"))

     (defn test-encrypt-fn [^bytes plaintext]
       (let [mac (Mac/getInstance "HmacSHA256")
             _ (.init mac test-nonce-key)
             nonce (byte-array (take 12 (.doFinal mac plaintext)))
             cipher (Cipher/getInstance "AES/GCM/NoPadding")]
         (.init cipher Cipher/ENCRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
         (byte-array (concat nonce (.doFinal cipher plaintext)))))

     (defn test-decrypt-fn [^bytes blob]
       (let [nonce (byte-array (take 12 blob))
             ct (byte-array (drop 12 blob))
             cipher (Cipher/getInstance "AES/GCM/NoPadding")]
         (.init cipher Cipher/DECRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
         (.doFinal cipher ct)))

     (defn test-blind-fn [component]
       (let [mac (Mac/getInstance "HmacSHA256")]
         (.init mac test-blind-key)
         (let [digest (.doFinal mac (.getBytes (pr-str component) "UTF-8"))]
           (.encodeToString (Base64/getEncoder) digest))))))

(deftest transact-and-datoms-roundtrip
  (testing "quad maps, [:db/add e a v], and bare [e a v] triples all normalize the same way"
    (let [db (-> (eng/empty-db)
                 (eng/transact [{:s "alice" :p "role" :o "admin"}
                                 [:db/add "alice" "name" "Alice"]
                                 ["bob" "role" "user"]]))
          rows (eng/datoms db (constantly true))]
      (is (= 3 (count rows)))
      (is (every? #(and (:e %) (:a %) (:v_edn %) (:added %)) rows))
      (is (= #{"\"admin\"" "\"Alice\"" "\"user\""} (set (map :v_edn rows)))))))

(deftest datoms-visible-is-required
  ;; mirrors q-visible-is-required: a non-empty db, so an omitted visible?
  ;; must actually be invoked as a predicate (and fail on arity) for this to
  ;; be a meaningful cross-platform check.
  (let [db (eng/transact (eng/empty-db) [["alice" "role" "admin"]])]
    (is (thrown? #?(:clj clojure.lang.ArityException :cljs js/Error)
                 (eng/datoms db))
        "datoms requires an explicit visibility decision -- no permissive default")))

(deftest datoms-visible-filters-rows
  (let [db (eng/transact (eng/empty-db)
                          [["alice" "role" "admin"] ["bob" "role" "user"]])
        alice-only (fn [{:keys [e]}] (= "alice" e))]
    (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}]
           (eng/datoms db alice-only))
        "visible? actually excludes rows, not just threaded through")))

(deftest datoms-honors-index-components-limit
  ;; Models the getBackup shape: several entities, fetch ONE by an index prefix
  ;; instead of scanning the whole graph (ADR-2607022330 addendum 2).
  (let [db (eng/transact (eng/empty-db)
                          [["keybackup/did:key:zAlice" ":aozora.keyBackup/did" "did:key:zAlice"]
                           ["keybackup/did:key:zAlice" ":aozora.keyBackup/blob" "{blobA}"]
                           ["keybackup/did:key:zBob"   ":aozora.keyBackup/did" "did:key:zBob"]
                           ["keybackup/did:key:zBob"   ":aozora.keyBackup/blob" "{blobB}"]
                           ["acct/alice" ":atproto.account/handle" "alice.aozora.app"]])
        everything (constantly true)]
    (testing "no opts → whole-db scan (unchanged)"
      (is (= 5 (count (eng/datoms db everything))))
      (is (= 5 (count (eng/datoms db nil everything)))))
    (testing ":eavt + [e] returns ONLY that entity's datoms (the getBackup query)"
      (let [rows (eng/datoms db {:index :eavt :components ["keybackup/did:key:zAlice"]} everything)]
        (is (= 2 (count rows)))
        (is (every? #(= "keybackup/did:key:zAlice" (:e %)) rows))
        (is (= #{":aozora.keyBackup/did" ":aozora.keyBackup/blob"} (set (map :a rows))))))
    (testing ":eavt + [e a] narrows to one attribute"
      (is (= [{:e "keybackup/did:key:zAlice" :a ":aozora.keyBackup/blob"
               :v_edn "\"{blobA}\"" :added true}]
             (eng/datoms db {:index :eavt
                             :components ["keybackup/did:key:zAlice" ":aozora.keyBackup/blob"]}
                         everything))))
    (testing ":avet + [attr value] point-looks-up the subject(s)"
      (let [rows (eng/datoms db {:index :avet
                                 :components [":aozora.keyBackup/did" "did:key:zBob"]}
                             everything)]
        (is (= [{:e "keybackup/did:key:zBob" :a ":aozora.keyBackup/did"
                 :v_edn "\"did:key:zBob\"" :added true}] rows))))
    (testing ":avet + [attr] returns all datoms for that attribute"
      (is (= 2 (count (eng/datoms db {:index :avet :components [":aozora.keyBackup/did"]} everything)))))
    (testing ":aevt + [attr] scans one attribute"
      (is (= 2 (count (eng/datoms db {:index :aevt :components [":aozora.keyBackup/blob"]} everything)))))
    (testing ":limit caps rows"
      (is (= 1 (count (eng/datoms db {:index :avet :components [":aozora.keyBackup/did"] :limit 1} everything)))))
    (testing "missing entity/attr → empty, not a full scan"
      (is (= [] (eng/datoms db {:index :eavt :components ["keybackup/nope"]} everything)))
      (is (= [] (eng/datoms db {:index :avet :components [":aozora.keyBackup/did" "did:key:zNope"]} everything))))))

#?(:clj
   (deftest cold-datoms-reads-filtered-from-snapshot
     ;; The scale fix: read a filtered set DIRECTLY from a persisted snapshot,
     ;; never rehydrating the whole db (ADR-2607022330 addendum 2, #13).
     (let [{:keys [put! get-fn]} (mem-store)
           db (eng/transact (eng/empty-db)
                             [["keybackup/zAlice" ":aozora.keyBackup/did" "did:key:zAlice"]
                              ["keybackup/zAlice" ":aozora.keyBackup/blob" "{blobA}"]
                              ["keybackup/zBob"   ":aozora.keyBackup/did" "did:key:zBob"]
                              ["acct/alice" ":atproto.account/handle" "alice.aozora.app"]])
           chain-cid (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
           snap      (eng/latest-snapshot-cid get-fn chain-cid)
           everything (constantly true)]
       (testing "cold :eavt [e] equals the hot filter (the getBackup query)"
         (is (= (set (eng/datoms db {:index :eavt :components ["keybackup/zAlice"]} everything))
                (set (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/zAlice"]} everything
                                       test-blind-fn test-decrypt-fn))))
         (is (= 2 (count (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/zAlice"]} everything
                                          test-blind-fn test-decrypt-fn)))))
       (testing "cold :avet [attr value] point lookup returns one datom"
         (is (= [{:e "keybackup/zBob" :a ":aozora.keyBackup/did"
                  :v_edn "\"did:key:zBob\"" :added true}]
                (eng/cold-datoms get-fn snap {:index :avet
                                              :components [":aozora.keyBackup/did" "did:key:zBob"]}
                                 everything test-blind-fn test-decrypt-fn))))
       (testing "cold :avet [attr] returns all subjects for the attribute"
         (is (= 2 (count (eng/cold-datoms get-fn snap {:index :avet
                                                       :components [":aozora.keyBackup/did"]}
                                          everything test-blind-fn test-decrypt-fn)))))
       (testing ":limit caps cold rows"
         (is (= 1 (count (eng/cold-datoms get-fn snap {:index :avet
                                                       :components [":aozora.keyBackup/did"] :limit 1}
                                          everything test-blind-fn test-decrypt-fn)))))
       (testing "missing entity/value → empty"
         (is (= [] (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/nope"]} everything
                                    test-blind-fn test-decrypt-fn)))
         (is (= [] (eng/cold-datoms get-fn snap {:index :avet
                                                 :components [":aozora.keyBackup/did" "did:key:zNope"]}
                                    everything test-blind-fn test-decrypt-fn)))))))

#?(:clj
   (deftest cold-datoms-visible-is-required
     ;; cold-datoms is single-arity (like q, unlike datoms/hot-datoms) -- a
     ;; non-empty snapshot is needed for a meaningful cross-platform check, same
     ;; reasoning as q-visible-is-required's comment.
     (let [{:keys [put! get-fn]} (mem-store)
           db (eng/transact (eng/empty-db) [["alice" "role" "admin"]])
           chain-cid (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
           snap (eng/latest-snapshot-cid get-fn chain-cid)]
       (is (thrown? clojure.lang.ArityException
                    (eng/cold-datoms get-fn snap nil))
           "cold-datoms requires an explicit visibility decision -- no permissive default"))))

#?(:clj
   (deftest cold-datoms-visible-filters-rows
     (let [{:keys [put! get-fn]} (mem-store)
           db (eng/transact (eng/empty-db)
                             [["alice" "role" "admin"] ["bob" "role" "user"]])
           chain-cid (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
           snap (eng/latest-snapshot-cid get-fn chain-cid)
           alice-only (fn [{:keys [e]}] (= "alice" e))]
       (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}]
              (eng/cold-datoms get-fn snap nil alice-only test-blind-fn test-decrypt-fn))
           "visible? actually excludes rows read cold from the snapshot"))))

#?(:clj
   (deftest hydrate-db-roundtrips-the-whole-db
     ;; write path: persist → hydrate → the reconstructed db equals the original,
     ;; and a follow-on transact+commit chains cleanly (#14 worker's transact uses this).
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           db0 (eng/transact (eng/empty-db)
                             [["alice" "role" "admin"] ["alice" "name" "Alice"]
                              ["bob" "role" "user"]])
           c0  (eng/snapshot! put! get-fn db0 nil test-blind-fn test-encrypt-fn)
           s0  (eng/latest-snapshot-cid get-fn c0)
           db1 (eng/hydrate-db get-fn s0 test-blind-fn test-decrypt-fn)]
       (is (= (set (eng/datoms db0 everything)) (set (eng/datoms db1 everything))) "hydrated db == original")
       (testing "hydrate → assert → commit is a clean incremental write"
         (let [db2 (eng/transact db1 [["carol" "role" "guest"]])
               c1  (eng/snapshot! put! get-fn db2 c0 test-blind-fn test-encrypt-fn)
               s1  (eng/latest-snapshot-cid get-fn c1)]
           (is (= 3 (count (eng/cold-datoms get-fn s1 {:index :aevt :components ["role"]} everything
                                            test-blind-fn test-decrypt-fn))))
           (is (= [{:e "carol" :a "role" :v_edn "\"guest\"" :added true}]
                  (eng/cold-datoms get-fn s1 {:index :avet :components ["role" "guest"]} everything
                                   test-blind-fn test-decrypt-fn)))))
       (testing "nil snapshot → empty db"
         (is (= [] (eng/datoms (eng/hydrate-db get-fn nil test-blind-fn test-decrypt-fn) everything)))))))

(deftest q-routes-through-arrangement-query
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
        "q cascades arrangement.query's required visibility decision -- no permissive default")))

(deftest query-joins-across-clauses
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "role" :o "admin"}
                           {:s "alice" :p "name" :o "Alice"}
                           {:s "bob" :p "role" :o "user"}
                           {:s "bob" :p "name" :o "Bob"}])
        everything (constantly true)]
    (is (= #{["Alice"]}
           (eng/query db {:find '[?name]
                          :where '[[?s "role" "admin"]
                                   [?s "name" ?name]]}
                      everything)))))

(deftest query-visible-is-required
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])]
    (is (thrown? #?(:clj clojure.lang.ArityException :cljs js/Error)
                 (eng/query db {:find '[?s] :where '[[?s "role" "admin"]]})))))

(deftest pull-returns-entity-attrs
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}
                                          {:s "alice" :p "name" :o "Alice"}])]
    (is (= {"role" #{"admin"} "name" #{"Alice"}} (eng/pull db "alice")))))

(deftest transact-is-immutable
  (let [db0 (eng/empty-db)
        db1 (eng/transact db0 [{:s "alice" :p "role" :o "admin"}])
        everything (constantly true)]
    (is (= 0 (count (eng/datoms db0 everything))))
    (is (= 1 (count (eng/datoms db1 everything))))))

#?(:clj
   (deftest commit-snapshots-are-content-addressed-and-deterministic
     (testing "committing the identical db content twice, from two independent
               chains/stores, yields the same snapshot CID -- arrangement's own
               content-addressing guarantee (preserved through encryption because
               test-encrypt-fn's nonce is content-derived, not random), exercised
               end to end through this namespace's snapshot! composition"
       (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
             {:keys [put! get-fn]} (mem-store)
             {put2! :put! get-fn2 :get-fn} (mem-store)
             a (:state (eng/head get-fn (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)))
             b (:state (eng/head get-fn2 (eng/snapshot! put2! get-fn2 db nil test-blind-fn test-encrypt-fn)))]
         (is (= a b))))))

#?(:clj
   (deftest chain-tracks-multiple-snapshots
     (let [{:keys [put! get-fn]} (mem-store)
           db1 (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
           db2 (eng/transact db1 [{:s "bob" :p "role" :o "user"}])
           c0 (eng/snapshot! put! get-fn db1 nil test-blind-fn test-encrypt-fn)
           c1 (eng/snapshot! put! get-fn db2 c0 test-blind-fn test-encrypt-fn)
           history (eng/chain get-fn c1)]
       (is (= 2 (count history)))
       (is (= [0 1] (map :seq history)))
       (is (not= (:state (first history)) (:state (second history)))
           "different db content -> different snapshot CID")
       (is (= (eng/latest-snapshot-cid get-fn c1)
              (ipld/link-cid (get (:state (last history)) "indexed")))
           "chain state is {\"indexed\" Link \"novelty\" []} wrapping the snapshot CID")
       (is (true? (eng/verify-chain get-fn c1))))))

#?(:clj
   (deftest verify-chain-catches-a-store-that-lies
     (let [{:keys [put! get-fn store]} (mem-store)
           db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
           c0 (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
           db2 (eng/transact db [{:s "bob" :p "role" :o "user"}])
           c1 (eng/snapshot! put! get-fn db2 c0 test-blind-fn test-encrypt-fn)
           other-cid (eng/snapshot! put! get-fn (eng/transact (eng/empty-db)
                                                               [{:s "mallory" :p "role" :o "evil"}])
                                     nil test-blind-fn test-encrypt-fn)]
       (is (true? (eng/verify-chain get-fn c1)))
       ;; splice a DIFFERENT (but validly dag-cbor-encoded) commit's bytes under
       ;; c0's own cid key -- a dishonest/corrupted store, without changing c0
       ;; itself. verify-chain must catch this via CID re-derivation, not throw.
       (swap! store assoc c0 (get @store other-cid))
       (is (false? (eng/verify-chain get-fn c1))))))

;; ── D1: novelty-log write path (ADR-2607032430) ─────────────────────────────

#?(:clj
   (deftest commit-bang-appends-novelty-without-touching-snapshot
     (testing "commit! on a fresh chain writes ONLY novelty -- nothing folded yet"
       (let [{:keys [put! get-fn]} (mem-store)
             c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)]
         (is (= 1 (eng/novelty-size get-fn c0)))
         (is (nil? (eng/latest-snapshot-cid get-fn c0))
             "nothing folded yet -- an all-novelty chain has no indexed snapshot")
         (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}]
                (eng/hot-datoms get-fn c0 (constantly true) test-blind-fn test-decrypt-fn))
             "hot-datoms still sees the data via novelty merge")))))

#?(:clj
   (deftest commit-bang-multiple-writes-accumulate-novelty
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] c1 test-encrypt-fn)]
       (is (= 3 (eng/novelty-size get-fn c2)))
       (is (= [0 1 2] (map :seq (eng/chain get-fn c2))))
       (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
              (set (eng/hot-datoms get-fn c2 (constantly true) test-blind-fn test-decrypt-fn)))
           "no loss/dup across three sequential novelty-append commits"))))

#?(:clj
   (deftest hot-datoms-merges-indexed-and-novelty
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)
           folded (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] folded test-encrypt-fn)]
       (is (some? (eng/latest-snapshot-cid get-fn c2)) "folded snapshot carries forward")
       (is (= 1 (eng/novelty-size get-fn c2)) "only the post-fold write is novelty")
       (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
              (set (eng/hot-datoms get-fn c2 everything test-blind-fn test-decrypt-fn)))
           "reads see both the folded-in history AND the fresh novelty")
       (testing "filtered hot-datoms honors opts across the snapshot/novelty split"
         (is (= [{:e "carol" :a "role" :v_edn "\"guest\"" :added true}]
                (eng/hot-datoms get-fn c2 {:index :eavt :components ["carol"]} everything
                                test-blind-fn test-decrypt-fn)))
         (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}]
                (eng/hot-datoms get-fn c2 {:index :eavt :components ["alice"]} everything
                                test-blind-fn test-decrypt-fn)))))))

#?(:clj
   (deftest hot-datoms-visible-is-required
     ;; multi-arity (2-arg / 3-arg), so an arity mismatch throws regardless of
     ;; the db's content (unlike q/cold-datoms' single-arity case) -- kept
     ;; non-empty anyway for consistency with the rest of this suite.
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)]
       (is (thrown? clojure.lang.ArityException
                    (eng/hot-datoms get-fn c0))
           "hot-datoms requires an explicit visibility decision -- no permissive default"))))

#?(:clj
   (deftest hot-datoms-visible-filters-rows
     ;; alice+bob are folded into the indexed snapshot (cold half); carol is
     ;; committed AFTER the fold, so she's pure novelty (hot half). A visible?
     ;; that excludes one entity from EACH half proves visible? reaches both
     ;; composed paths, not just one.
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)
           folded (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] folded test-encrypt-fn)
           alice-only (fn [{:keys [e]}] (= "alice" e))]
       (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}}
              (set (eng/hot-datoms get-fn c2 alice-only test-blind-fn test-decrypt-fn)))
           "visible? excludes bob (cold/snapshot half) AND carol (hot/novelty half)"))))

#?(:clj
   (deftest fold-bang-compacts-novelty-and-preserves-data
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)
           before (set (eng/hot-datoms get-fn c1 everything test-blind-fn test-decrypt-fn))
           folded (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)]
       (is (= 0 (eng/novelty-size get-fn folded)) "fold resets the tail")
       (is (some? (eng/latest-snapshot-cid get-fn folded)))
       (is (= before (set (eng/hot-datoms get-fn folded everything test-blind-fn test-decrypt-fn)))
           "folding never loses or duplicates data")
       (is (= before (set (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                           test-blind-fn test-decrypt-fn)))
           "the fold really did index the data -- a cold-only read (no novelty) sees it"))))

#?(:clj
   (deftest fold-bang-is-deterministic-across-independent-stores
     (testing "folding the identical (indexed, novelty) history from two independent
               stores yields the same snapshot CID -- content-addressing holds
               through the fold (preserved through encryption by test-encrypt-fn's
               content-derived, not random, nonce), so concurrent/redundant folds
               converge safely"
       (let [mk-fold (fn []
                       (let [{:keys [put! get-fn]} (mem-store)
                             c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
                             c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)]
                         (eng/latest-snapshot-cid get-fn (eng/fold! put! get-fn c1
                                                                     test-blind-fn test-encrypt-fn test-decrypt-fn))))]
         (is (= (mk-fold) (mk-fold)))))))

#?(:clj
   (deftest should-fold-flags-at-threshold
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)]
       (is (false? (eng/should-fold? get-fn c1 3)))
       (is (true? (eng/should-fold? get-fn c1 2)))
       (is (false? (eng/should-fold? get-fn c1)) "default threshold is well above 2"))))

#?(:clj
   (deftest normalize-state-reads-pre-d1-bare-link-chains
     (testing "a chain committed by the pre-D1 code (state = a bare snapshot Link,
               no {indexed novelty} wrapper) still reads correctly under the new
               code -- zero migration step for already-deployed actors"
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)
             db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
             snap-cid (qs/commit! put! db nil qs/current-schema-version test-blind-fn test-encrypt-fn)
             pre-d1-chain (cd/commit! put! get-fn (ipld/link snap-cid) nil)]
         (is (= snap-cid (eng/latest-snapshot-cid get-fn pre-d1-chain)))
         (is (= 0 (eng/novelty-size get-fn pre-d1-chain)))
         (is (= (set (eng/datoms db everything))
                (set (eng/hot-datoms get-fn pre-d1-chain everything test-blind-fn test-decrypt-fn))))
         (testing "committing new novelty on top of a pre-D1 chain works (mixed-era chain)"
           (let [c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] pre-d1-chain test-encrypt-fn)]
             (is (= 1 (eng/novelty-size get-fn c1)))
             (is (= (conj (set (eng/datoms db everything))
                          {:e "bob" :a "role" :v_edn "\"user\"" :added true})
                    (set (eng/hot-datoms get-fn c1 everything test-blind-fn test-decrypt-fn))))))))))

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
      (is (= (set (eng/datoms via-raw (constantly true))) (set (eng/datoms via-tx (constantly true))))
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
             (set (map (juxt :a :v_edn) (eng/datoms db (constantly true)))))))))

#?(:clj
   (deftest link-ref-survives-fold-and-cold-read
     (testing "a Link-valued datom, folded into a persisted snapshot and read back
               cold, is still a real Link -- and refs/refs-to still finds it on the
               rehydrated hot db (the full novelty -> fold -> cold-read round trip)"
       (let [{:keys [put! get-fn]} (mem-store)
             c0 (eng/commit! put! get-fn [{:s "alice" :p "knows" :o bob-link}] nil test-encrypt-fn)
             folded (eng/fold! put! get-fn c0 test-blind-fn test-encrypt-fn test-decrypt-fn)
             snap (eng/latest-snapshot-cid get-fn folded)]
         (testing "cold-datoms reconstructs the Link (not the raw edn-safe vector)"
           (let [row (first (eng/cold-datoms get-fn snap {:index :eavt :components ["alice"]} (constantly true)
                                              test-blind-fn test-decrypt-fn))]
             (is (= "[\"ipld/link\" \"bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m\"]"
                    (:v_edn row)))))
         (testing "hydrate-db reconstructs a real Link, so refs-to finds it again"
           (let [db (eng/hydrate-db get-fn snap test-blind-fn test-decrypt-fn)]
             (is (= {"knows" #{"alice"}} (qs/refs-to db bob-link)))))
         (testing "hot-datoms (novelty path, via dag-cbor) agrees with the cold path"
           (is (= (set (eng/hot-datoms get-fn c0 (constantly true) test-blind-fn test-decrypt-fn))
                  (set (eng/cold-datoms get-fn snap nil (constantly true) test-blind-fn test-decrypt-fn)))))))))
