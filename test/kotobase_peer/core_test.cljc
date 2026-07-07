(ns kotobase-peer.core-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing async] :include-macros true])
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

;; ── ADR-2607051000 test crypto (accepted 2026-07-06; cljs/crypto.subtle
;; sibling added per the ADR's Worker addendum) ───────────────────────────────
;; Real AES-256-GCM + HMAC-SHA256, not a mock, on BOTH platforms: `javax.
;; crypto` on the JVM (synchronous), `crypto.subtle` on cljs (Promise-based).
;; Same algorithm choice, same deterministic-nonce construction, same key
;; material on both sides -- only the calling convention differs. Identical
;; helpers to `arrangement.core-test`'s (kept independent per-repo rather
;; than shared, matching this codebase's existing test-helper duplication
;; convention across repos).
#?(:clj
   (def ^:private test-dek
     (SecretKeySpec. (byte-array (range 1 33)) "AES")))

#?(:clj
   (def ^:private test-blind-key
     (SecretKeySpec. (byte-array (range 33 65)) "HmacSHA256")))

#?(:clj
   (def ^:private test-nonce-key
     (SecretKeySpec. (byte-array (range 65 97)) "HmacSHA256")))

#?(:clj
   (defn test-encrypt-fn [^bytes plaintext]
     (let [mac (Mac/getInstance "HmacSHA256")
           _ (.init mac test-nonce-key)
           nonce (byte-array (take 12 (.doFinal mac plaintext)))
           cipher (Cipher/getInstance "AES/GCM/NoPadding")]
       (.init cipher Cipher/ENCRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
       (byte-array (concat nonce (.doFinal cipher plaintext))))))

#?(:clj
   (defn test-decrypt-fn [^bytes blob]
     (let [nonce (byte-array (take 12 blob))
           ct (byte-array (drop 12 blob))
           cipher (Cipher/getInstance "AES/GCM/NoPadding")]
       (.init cipher Cipher/DECRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
       (.doFinal cipher ct))))

#?(:clj
   (defn test-blind-fn [component]
     (let [mac (Mac/getInstance "HmacSHA256")]
       (.init mac test-blind-key)
       (let [digest (.doFinal mac (.getBytes (pr-str component) "UTF-8"))]
         (.encodeToString (Base64/getEncoder) digest)))))

#?(:cljs
   (def ^:private subtle (.-subtle js/crypto)))

#?(:cljs
   (def ^:private test-dek-bytes (js/Uint8Array. (clj->js (vec (range 1 33))))))

#?(:cljs
   (def ^:private test-blind-key-bytes (js/Uint8Array. (clj->js (vec (range 33 65))))))

#?(:cljs
   (def ^:private test-nonce-key-bytes (js/Uint8Array. (clj->js (vec (range 65 97))))))

#?(:cljs
   (defn- import-aes-key []
     (.importKey subtle "raw" test-dek-bytes #js {:name "AES-GCM"} false #js ["encrypt" "decrypt"])))

#?(:cljs
   (defn- import-hmac-key [key-bytes]
     (.importKey subtle "raw" key-bytes #js {:name "HMAC" :hash "SHA-256"} false #js ["sign"])))

#?(:cljs
   (defn- concat-bytes [^js a ^js b]
     (let [out (js/Uint8Array. (+ (.-byteLength a) (.-byteLength b)))]
       (.set out a 0)
       (.set out b (.-byteLength a))
       out)))

#?(:cljs
   (defn- bytes->base64 [^js buf]
     (let [bytes (js/Uint8Array. buf)]
       (js/btoa (.. js/Array -prototype -reduce
                    (call bytes (fn [acc b] (str acc (.fromCharCode js/String b))) ""))))))

#?(:cljs
   (defn test-encrypt-fn [plaintext]
     (-> (js/Promise.all #js [(import-hmac-key test-nonce-key-bytes) (import-aes-key)])
         (.then (fn [[nonce-key aes-key]]
                  (-> (.sign subtle #js {:name "HMAC"} nonce-key plaintext)
                      (.then (fn [mac] (.slice (js/Uint8Array. mac) 0 12)))
                      (.then (fn [nonce]
                               (-> (.encrypt subtle #js {:name "AES-GCM" :iv nonce :tagLength 128} aes-key plaintext)
                                   (.then (fn [ct] (concat-bytes nonce (js/Uint8Array. ct)))))))))))))

#?(:cljs
   (defn test-decrypt-fn [^js blob]
     (let [bytes (js/Uint8Array. blob)
           nonce (.slice bytes 0 12)
           ct (.slice bytes 12)]
       (-> (import-aes-key)
           (.then (fn [aes-key] (.decrypt subtle #js {:name "AES-GCM" :iv nonce :tagLength 128} aes-key ct)))
           (.then (fn [pt] (js/Uint8Array. pt)))))))

#?(:cljs
   (defn test-blind-fn [component]
     (-> (import-hmac-key test-blind-key-bytes)
         (.then (fn [key] (.sign subtle #js {:name "HMAC"}
                                 key (.encode (js/TextEncoder.) (pr-str component)))))
         (.then bytes->base64))))

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

(deftest query-negation-end-to-end
  ;; ADR-2607061200 Stage 2, reachable through kotobase-peer.core/query, not
  ;; just arrangement.datalog/q directly (query is a straight passthrough).
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "role" :o "admin"}
                           {:s "alice" :p "name" :o "Alice"}
                           {:s "bob" :p "role" :o "user"}
                           {:s "bob" :p "name" :o "Bob"}])
        everything (constantly true)]
    (is (= #{["bob"]}
           (eng/query db {:find '[?s]
                          :where '[[?s "name" _]
                                   (not [?s "role" "admin"])]}
                      everything)))))

(deftest query-negation-still-respects-visible-through-the-peer-layer
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "role" :o "admin"}
                           {:s "alice" :p "name" :o "Alice"}
                           {:s "bob" :p "role" :o "user"}
                           {:s "bob" :p "name" :o "Bob"}
                           {:s "carol" :p "role" :o "admin"}
                           {:s "carol" :p "name" :o "Carol"}])
        hide-carols-admin-fact (fn [{:keys [s p o]}] (not (and (= s "carol") (= p "role") (= o "admin"))))]
    (is (= #{["bob"] ["carol"]}
           (eng/query db {:find '[?s]
                          :where '[[?s "name" _]
                                   (not [?s "role" "admin"])]}
                      hide-carols-admin-fact))
        "carol's admin fact is hidden from this caller -- indistinguishable from bob's genuine non-admin status")))

(deftest query-aggregation-end-to-end
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "role" :o "admin"}
                           {:s "bob" :p "role" :o "user"}
                           {:s "carol" :p "role" :o "admin"}])
        everything (constantly true)]
    (is (= #{["admin" 2] ["user" 1]}
           (eng/query db {:find '[?role (count ?s)] :where '[[?s "role" ?role]]} everything)))))

(deftest query-recursive-rule-end-to-end
  ;; ADR-2607061200 Stage 3/4, reachable through kotobase-peer.core/query.
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "parent" :o "bob"}
                           {:s "bob" :p "parent" :o "carol"}
                           {:s "carol" :p "parent" :o "dave"}])
        everything (constantly true)
        ancestor-rules '[[(ancestor ?x ?y) [?x "parent" ?y]]
                         [(ancestor ?x ?y) [?x "parent" ?z] (ancestor ?z ?y)]]]
    (is (= #{["bob"] ["carol"] ["dave"]}
           (eng/query db {:find '[?y] :where '[(ancestor "alice" ?y)] :rules ancestor-rules} everything)))))

(deftest query-recursive-rule-still-respects-visible-through-the-peer-layer
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "parent" :o "bob"}
                           {:s "bob" :p "parent" :o "carol"}
                           {:s "carol" :p "parent" :o "dave"}])
        hide-bob-carol (fn [{:keys [s p o]}] (not (and (= s "bob") (= p "parent") (= o "carol"))))
        ancestor-rules '[[(ancestor ?x ?y) [?x "parent" ?y]]
                         [(ancestor ?x ?y) [?x "parent" ?z] (ancestor ?z ?y)]]]
    (is (= #{["bob"]}
           (eng/query db {:find '[?y] :where '[(ancestor "alice" ?y)] :rules ancestor-rules} hide-bob-carol))
        "bob->carol is hidden from this caller -- the recursive fixpoint can't derive past bob")))

(deftest query-language-extensions-end-to-end
  ;; ADR-2607061200 query-language follow-up (:in, function clauses, or),
  ;; reachable through kotobase-peer.core/query, not just
  ;; arrangement.datalog/q directly. Built via arrangement.core/assert-quad
  ;; directly (not eng/transact) to keep :age as a real number -- kotobase-
  ;; peer.core's own ->quad-value stringifies every non-Link value (a
  ;; separate, pre-existing, orthogonal limitation this test isn't about).
  (let [db (-> (qs/empty-db)
               (qs/assert-quad {:s "alice" :p "age" :o 30})
               (qs/assert-quad {:s "bob" :p "age" :o 15})
               (qs/assert-quad {:s "carol" :p "age" :o 45}))
        everything (constantly true)]
    (is (= #{["alice"] ["carol"]}
           (eng/query db {:find '[?s] :in '[?min-age] :where '[[?s "age" ?age] [(> ?age ?min-age)]]}
                      everything [18])))
    (is (= #{["alice"] ["carol"]}
           (eng/query db {:find '[?s] :where '[(or [?s "age" 30] [?s "age" 45])]} everything)))))

(deftest pull-returns-entity-attrs
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}
                                          {:s "alice" :p "name" :o "Alice"}])]
    (is (= {"role" #{"admin"} "name" #{"Alice"}} (eng/pull db "alice")))))

;; ── pull patterns (ADR-2607061200 "3 pillars" landing) ──────────────────────

(deftest pull-pattern-plain-attrs
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}
                                          {:s "alice" :p "name" :o "Alice"}
                                          {:s "alice" :p "age" :o "30"}])]
    (is (= {"role" #{"admin"} "name" #{"Alice"}}
           (eng/pull db "alice" ["role" "name"])))))

(deftest pull-pattern-wildcard-matches-2-arg-form
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}
                                          {:s "alice" :p "name" :o "Alice"}])]
    (is (= (eng/pull db "alice") (eng/pull db "alice" '[*])))))

(deftest pull-pattern-nested-ref-forward
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "name" :o "Alice"}
                                          {:s "bob" :p "manager" :o "alice"}
                                          {:s "bob" :p "name" :o "Bob"}])]
    (is (= {"manager" #{{"name" #{"Alice"}}}}
           (eng/pull db "bob" [{"manager" ["name"]}])))))

(deftest pull-pattern-reverse-ref-needs-link-valued-refs
  ;; refs-to (VAET-style) only tracks Link-valued references (ref?'s
  ;; default is ipld/link?) -- same convention `refs` already documents.
  (let [alice-link (ipld/link "bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m")
        db (eng/transact (eng/empty-db) [{:s "alice" :p "name" :o "Alice"}
                                          {:s "bob" :p "manager" :o alice-link}
                                          {:s "bob" :p "name" :o "Bob"}
                                          {:s "carol" :p "manager" :o alice-link}
                                          {:s "carol" :p "name" :o "Carol"}])]
    (testing "flat reverse ref"
      (is (= #{"bob" "carol"} (get (eng/pull db alice-link ["_manager"]) "_manager"))))
    (testing "nested reverse ref"
      (is (= #{{"name" #{"Bob"}} {"name" #{"Carol"}}}
             (get (eng/pull db alice-link [{"_manager" ["name"]}]) "_manager"))))))

(deftest pull-pattern-recursion-depth-limit
  (let [db (eng/transact (eng/empty-db)
                         [{:s "a" :p "name" :o "A"} {:s "a" :p "friend" :o "b"}
                          {:s "b" :p "name" :o "B"} {:s "b" :p "friend" :o "c"}
                          {:s "c" :p "name" :o "C"} {:s "c" :p "friend" :o "a"}]
                         (constantly false))]
    (is (= {"friend" #{{"name" #{"B"} "friend" #{{"name" #{"C"} "friend" #{"a"}}}}}}
           (eng/pull db "a" [{"friend" 2}]))
        "depth 2: expands friend twice (a->b->c), then leaves c's own friend value bare (a, unexpanded)")))

(deftest pull-pattern-unlimited-recursion-is-cycle-safe
  (let [db (eng/transact (eng/empty-db)
                         [{:s "a" :p "friend" :o "b"}
                          {:s "b" :p "friend" :o "c"}
                          {:s "c" :p "friend" :o "a"}]
                         (constantly false))]
    (testing "a->b->c->a is a cycle -- '... must terminate, not hang, and treat the closed loop as {}"
      (is (= {"friend" #{{"friend" #{{"friend" #{{}}}}}}}
             (eng/pull db "a" [{"friend" '...}]))))))

(deftest transact-is-immutable
  (let [db0 (eng/empty-db)
        db1 (eng/transact db0 [{:s "alice" :p "role" :o "admin"}])
        everything (constantly true)]
    (is (= 0 (count (eng/datoms db0 everything))))
    (is (= 1 (count (eng/datoms db1 everything))))))

;; ── schema: Datomic-style "schema is just datoms too" (ADR-2607061200) ──────

(deftest schema-is-derived-from-installed-datoms
  (let [db (eng/install-schema (eng/empty-db)
                                {"role" {:db/valueType :string :db/cardinality :one}
                                 "age"  {:db/valueType :long}
                                 "email" {:db/valueType :string :db/unique :identity}})]
    (is (= {"role" {:value-type "string" :cardinality "one" :unique nil :tuple-types nil}
            "age" {:value-type "long" :cardinality "many" :unique nil :tuple-types nil}
            "email" {:value-type "string" :cardinality "many" :unique "identity" :tuple-types nil}}
           (eng/schema-of db)))))

(deftest plain-transact-is-unaffected-by-schema-no-migration-needed
  ;; every existing caller of plain `transact` keeps working exactly as
  ;; before -- schema enforcement is opt-in via transact-with-schema only.
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "totally-unknown-attr" :o "anything"}])]
    (is (= #{"anything"} (get (eng/pull db "alice") "totally-unknown-attr")))))

(deftest transact-with-schema-rejects-unknown-attribute
  (let [db (eng/install-schema (eng/empty-db) {"role" {:db/valueType :string}})]
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                           #"unknown attribute"
                           (eng/transact-with-schema db [{:s "alice" :p "nickname" :o "Al"}] {})))))

(deftest transact-with-schema-can-declare-new-attributes-inline
  (let [db (eng/transact-with-schema (eng/empty-db)
                                      [{:s "alice" :p "role" :o "admin"}]
                                      {"role" {:db/valueType :string}})]
    (is (= #{"admin"} (get (eng/pull db "alice") "role")))))

(deftest transact-with-schema-rejects-value-type-mismatch
  (let [db (eng/install-schema (eng/empty-db) {"age" {:db/valueType :long}})]
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                           #"schema violation"
                           (eng/transact-with-schema db [{:s "bob" :p "age" :o "not-a-number"}] {})))
    (testing "a real long passes validation (storage still stringifies, matching the rest of this codebase's convention -- schema validates the ORIGINAL value's type, it doesn't change on-disk representation)"
      (is (= #{"42"} (get (eng/pull (eng/transact-with-schema db [{:s "bob" :p "age" :o 42}] {}) "bob") "age"))))))

(deftest transact-with-schema-cardinality-one-replaces-not-accumulates
  (let [db0 (eng/install-schema (eng/empty-db) {"role" {:db/valueType :string :db/cardinality :one}})
        db1 (eng/transact-with-schema db0 [{:s "alice" :p "role" :o "admin"}] {})
        db2 (eng/transact-with-schema db1 [{:s "alice" :p "role" :o "superadmin"}] {})]
    (is (= #{"admin"} (get (eng/pull db1 "alice") "role")))
    (is (= #{"superadmin"} (get (eng/pull db2 "alice") "role"))
        "cardinality :one retracts the prior value instead of accumulating -- unlike arrangement's own native cardinality-many default")))

(deftest transact-with-schema-cardinality-many-still-accumulates
  (let [db0 (eng/install-schema (eng/empty-db) {"tag" {:db/valueType :string}})
        db1 (eng/transact-with-schema db0 [{:s "alice" :p "tag" :o "a"}] {})
        db2 (eng/transact-with-schema db1 [{:s "alice" :p "tag" :o "b"}] {})]
    (is (= #{"a" "b"} (get (eng/pull db2 "alice") "tag")))))

(deftest transact-with-schema-unique-identity-rejects-cross-entity-collision
  (let [db0 (eng/install-schema (eng/empty-db) {"email" {:db/valueType :string :db/unique :identity}})
        db1 (eng/transact-with-schema db0 [{:s "alice" :p "email" :o "a@x.com"}] {})]
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                           #"unique attribute violation"
                           (eng/transact-with-schema db1 [{:s "bob" :p "email" :o "a@x.com"}] {})))
    (testing "the SAME entity re-asserting its own unique value is not a violation"
      (is (= #{"a@x.com"}
             (get (eng/pull (eng/transact-with-schema db1 [{:s "alice" :p "email" :o "a@x.com"}] {}) "alice") "email"))))))

;; ── value-type extension: uuid/instant/keyword/symbol/bytes/tuple ──────────
;; (ADR-2607061200 follow-up)

(deftest inline-schema-declaration-actually-validates-not-a-silent-noop
  ;; regression test for a real bug: passing {:db/valueType ...} directly
  ;; to transact-with-schema (never pre-installed via install-schema)
  ;; used to silently skip value-type validation entirely.
  (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                        #"schema violation"
                        (eng/transact-with-schema (eng/empty-db)
                                                  [{:s "bob" :p "age" :o "not-a-number"}]
                                                  {"age" {:db/valueType :long}}))))

(deftest uuid-value-type
  (let [db0 (eng/install-schema (eng/empty-db) {"id" {:db/valueType :uuid}})
        the-id #uuid "0f1e86a9-aa89-414e-8368-d4e95e9fcd4c"]
    (testing "validates against the ORIGINAL value's type; storage still stringifies (same convention as long/etc.)"
      (is (= #{(str the-id)} (get (eng/pull (eng/transact-with-schema db0 [{:s "a" :p "id" :o the-id}] {}) "a") "id"))))
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                          #"schema violation"
                          (eng/transact-with-schema db0 [{:s "a" :p "id" :o "not-a-uuid"}] {})))))

(deftest instant-value-type
  (let [db0 (eng/install-schema (eng/empty-db) {"born" {:db/valueType :instant}})]
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                          #"schema violation"
                          (eng/transact-with-schema db0 [{:s "a" :p "born" :o "not-a-date"}] {})))))

(deftest keyword-and-symbol-value-types
  (let [db0 (eng/install-schema (eng/empty-db) {"status" {:db/valueType :keyword} "op" {:db/valueType :symbol}})
        db1 (eng/transact-with-schema db0 [{:s "a" :p "status" :o :active} {:s "a" :p "op" :o 'foo}] {})]
    (is (= #{(str :active)} (get (eng/pull db1 "a") "status")))
    (is (= #{(str 'foo)} (get (eng/pull db1 "a") "op")))
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                          #"schema violation"
                          (eng/transact-with-schema db0 [{:s "a" :p "status" :o "active"}] {})))))

(deftest tuple-value-type
  (let [db0 (eng/install-schema (eng/empty-db)
                                {"coords" {:db/valueType :tuple :db/tupleTypes [:double :double]}})]
    (is (= #{(str [1.0 2.0])} (get (eng/pull (eng/transact-with-schema db0 [{:s "a" :p "coords" :o [1.0 2.0]}] {}) "a") "coords")))
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                          #"tuple arity mismatch"
                          (eng/transact-with-schema db0 [{:s "a" :p "coords" :o [1.0 2.0 3.0]}] {})))
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                          #"schema violation"
                          (eng/transact-with-schema db0 [{:s "a" :p "coords" :o [1.0 "not-a-double"]}] {})))))

;; ── entity: lazy navigational entity API (ADR-2607061200 follow-up) ────────

(deftest entity-is-pulls-flat-2-arg-form
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"} {:s "alice" :p "name" :o "Alice"}])]
    (is (= (eng/pull db "alice") (eng/entity db "alice")))))

(deftest entity-attr-navigates-into-a-ref-values-own-entity
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "name" :o "Alice"}
                           {:s "bob" :p "manager" :o "alice"}
                           {:s "bob" :p "name" :o "Bob"}])]
    (is (= #{{"name" #{"Alice"}}} (eng/entity-attr db (eng/entity db "bob") "manager")))))

(deftest entity-attr-on-a-value-that-was-never-a-subject-is-empty
  (let [db (eng/transact (eng/empty-db) [{:s "bob" :p "hobby" :o "chess"}])]
    (is (= #{{}} (eng/entity-attr db (eng/entity db "bob") "hobby")))))

;; ── tx-report / with (ADR-2607061200 "3 pillars" follow-up) ─────────────────

(deftest transact-with-report-shape
  (let [db0 (eng/empty-db)
        report (eng/transact-with-report db0 [{:s "alice" :p "role" :o "admin"}])]
    (is (= #{:db-before :db-after :tx-data} (set (keys report))))
    (is (= db0 (:db-before report)))
    (is (= #{"admin"} (get (eng/pull (:db-after report) "alice") "role")))
    (is (= [{:s "alice" :p "role" :o "admin"}] (:tx-data report)))))

(deftest with-is-transact-with-report-under-datomics-own-name
  (let [db0 (eng/empty-db)
        tx-data [{:s "alice" :p "role" :o "admin"}]]
    (is (= (eng/transact-with-report db0 tx-data) (eng/with db0 tx-data)))))

(deftest with-never-touches-the-original-db-value
  (let [db0 (eng/empty-db)
        {:keys [db-after]} (eng/with db0 [{:s "alice" :p "role" :o "admin"}])]
    (is (= 0 (count (eng/datoms db0 (constantly true)))) "db0 itself is untouched -- with is purely speculative")
    (is (= 1 (count (eng/datoms db-after (constantly true)))))))

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
   (deftest as-of-finds-the-exact-chain-cid-at-each-seq
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v2"}] c0 test-encrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v3"}] c1 test-encrypt-fn)]
       (is (= c0 (eng/as-of get-fn c2 0)))
       (is (= c1 (eng/as-of get-fn c2 1)))
       (is (= c2 (eng/as-of get-fn c2 2)))
       (testing "seq beyond the tip clamps to the tip (Datomic's own \"as-of the future = now\")"
         (is (= c2 (eng/as-of get-fn c2 999))))
       (testing "nil chain-cid -> nil (no prior commit at all)"
         (is (nil? (eng/as-of get-fn nil 0)))))))

#?(:clj
   (deftest hydrate-chain-gives-a-queryable-db-value-as-of-any-point
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v2"}] c0 test-encrypt-fn)
           _ (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)]
       (testing "as-of composes directly with hydrate-chain -- true db value at that point"
         (let [db-at-0 (eng/hydrate-chain get-fn (eng/as-of get-fn c1 0) test-blind-fn test-decrypt-fn)
               db-at-1 (eng/hydrate-chain get-fn (eng/as-of get-fn c1 1) test-blind-fn test-decrypt-fn)]
           (is (= #{"v1"} (get (eng/pull db-at-0 "alice") "role")))
           (is (= #{"v1" "v2"} (get (eng/pull db-at-1 "alice") "role")))))
       (testing "hydrate-chain is queryable through arrangement.datalog/q too, not just pull"
         (let [db-at-0 (eng/hydrate-chain get-fn (eng/as-of get-fn c1 0) test-blind-fn test-decrypt-fn)]
           (is (= #{["v1"]} (eng/query db-at-0 {:find '[?v] :where '[["alice" "role" ?v]]} everything))))))))

#?(:clj
   (deftest commit-serialized-basic-single-writer-usage
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas! (fn [head-key expected new]
                  (let [result (atom nil)]
                    (swap! heads (fn [m]
                                   (if (= (get m head-key) expected)
                                     (do (reset! result new) (assoc m head-key new))
                                     (do (reset! result (get m head-key)) m))))
                    @result))
           c0 (eng/commit-serialized! put! get-fn cas! "actor:alice" nil [{:s "alice" :p "role" :o "v1"}] test-encrypt-fn)
           c1 (eng/commit-serialized! put! get-fn cas! "actor:alice" c0 [{:s "alice" :p "role" :o "v2"}] test-encrypt-fn)]
       (is (= c1 (get @heads "actor:alice")) "head-key tracks the latest committed chain-cid")
       (is (= [0 1] (map :seq (eng/chain get-fn c1)))))))

#?(:clj
   (deftest commit-serialized-retries-against-the-real-head-on-a-lost-race
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas! (fn [head-key expected new]
                  (let [result (atom nil)]
                    (swap! heads (fn [m]
                                   (if (= (get m head-key) expected)
                                     (do (reset! result new) (assoc m head-key new))
                                     (do (reset! result (get m head-key)) m))))
                    @result))
           c0 (eng/commit-serialized! put! get-fn cas! "actor:alice" nil [{:s "alice" :p "role" :o "v1"}] test-encrypt-fn)
           c1 (eng/commit-serialized! put! get-fn cas! "actor:alice" c0 [{:s "alice" :p "role" :o "v2"}] test-encrypt-fn)
           ;; a stale caller still believes the head is c0 (a concurrent writer already
           ;; advanced it to c1) -- commit-serialized! must retry against the REAL head,
           ;; never fork the chain.
           c2 (eng/commit-serialized! put! get-fn cas! "actor:alice" c0 [{:s "alice" :p "role" :o "v3-from-stale-caller"}] test-encrypt-fn)]
       (is (= [0 1 2] (map :seq (eng/chain get-fn c2))) "no fork -- a clean, gapless sequence")
       (is (= c2 (get @heads "actor:alice")) "the retried commit is the one that actually won"))))

#?(:clj
   (deftest commit-serialized-throws-on-persistent-contention
     (let [{:keys [put! get-fn]} (mem-store)
           always-losing-cas! (fn [head-key expected new] nil)]
       (is (thrown-with-msg? clojure.lang.ExceptionInfo #"max-cas-retries"
                             (eng/commit-serialized! put! get-fn always-losing-cas! "actor:bob" nil
                                                     [{:s "bob" :p "x" :o "y"}] test-encrypt-fn 3))))))

#?(:clj
   (deftest commit-with-report-bang-shape
     (let [{:keys [put! get-fn]} (mem-store)
           report (eng/commit-with-report! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)]
       (is (= #{:chain-cid-before :chain-cid-after :tx-data} (set (keys report))))
       (is (nil? (:chain-cid-before report)))
       (is (some? (:chain-cid-after report)))
       (is (= [{:s "alice" :p "role" :o "admin"}] (:tx-data report))))))

#?(:clj
   (deftest commit-serialized-with-report-bang-shape
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas! (fn [head-key expected new]
                  (let [result (atom nil)]
                    (swap! heads (fn [m]
                                   (if (= (get m head-key) expected)
                                     (do (reset! result new) (assoc m head-key new))
                                     (do (reset! result (get m head-key)) m))))
                    @result))
           report (eng/commit-serialized-with-report! put! get-fn cas! "actor:alice" nil
                                                       [{:s "alice" :p "role" :o "admin"}] test-encrypt-fn)]
       (is (= #{:chain-cid-before :chain-cid-after :tx-data} (set (keys report))))
       (is (= (:chain-cid-after report) (get @heads "actor:alice"))))))

#?(:clj
   (deftest since-shows-only-commits-after-a-seq
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "v2"}] c0 test-encrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "v3"}] c1 test-encrypt-fn)
           delta (eng/since get-fn c2 0 test-decrypt-fn)]
       (is (nil? (get (eng/pull delta "alice") "role")) "alice's write was AT seq 0, not after it")
       (is (= #{"v2"} (get (eng/pull delta "bob") "role")))
       (is (= #{"v3"} (get (eng/pull delta "carol") "role"))))))

#?(:clj
   (deftest history-includes-everything-across-a-fold
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "v2"}] c0 test-encrypt-fn)
           folded (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "carol" :p "role" :o "v3"}] folded test-encrypt-fn)
           full-history (eng/history get-fn c2 test-blind-fn test-decrypt-fn)]
       (is (= #{"v1"} (get (eng/pull full-history "alice") "role"))
           "alice's fact was folded into the indexed snapshot -- still visible")
       (is (= #{"v2"} (get (eng/pull full-history "bob") "role")))
       (is (= #{"v3"} (get (eng/pull full-history "carol") "role"))
           "carol's fact is post-fold novelty -- also visible"))))

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

;; ── cljs mirror of the JVM-only block above: same test names/assertions,
;; Promise-based via `cljs.test/async` since `eng/snapshot!`/`eng/commit!`/
;; `eng/cold-datoms`/`eng/hydrate-db`/`eng/hot-datoms`/`eng/fold!` all return
;; a `js/Promise` on cljs (see `kotobase-peer.core`'s platform-split note).
#?(:cljs
   (deftest cold-datoms-reads-filtered-from-snapshot
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             db (eng/transact (eng/empty-db)
                               [["keybackup/zAlice" ":aozora.keyBackup/did" "did:key:zAlice"]
                                ["keybackup/zAlice" ":aozora.keyBackup/blob" "{blobA}"]
                                ["keybackup/zBob"   ":aozora.keyBackup/did" "did:key:zBob"]
                                ["acct/alice" ":atproto.account/handle" "alice.aozora.app"]])
             everything (constantly true)]
         (-> (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
             (.then (fn [chain-cid]
                      (let [snap (eng/latest-snapshot-cid get-fn chain-cid)]
                        (-> (js/Promise.all
                             #js [(eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/zAlice"]} everything
                                                    test-blind-fn test-decrypt-fn)
                                  (eng/cold-datoms get-fn snap {:index :avet
                                                                :components [":aozora.keyBackup/did" "did:key:zBob"]}
                                                    everything test-blind-fn test-decrypt-fn)
                                  (eng/cold-datoms get-fn snap {:index :avet
                                                                :components [":aozora.keyBackup/did"]}
                                                    everything test-blind-fn test-decrypt-fn)
                                  (eng/cold-datoms get-fn snap {:index :avet
                                                                :components [":aozora.keyBackup/did"] :limit 1}
                                                    everything test-blind-fn test-decrypt-fn)
                                  (eng/cold-datoms get-fn snap {:index :eavt :components ["keybackup/nope"]} everything
                                                    test-blind-fn test-decrypt-fn)
                                  (eng/cold-datoms get-fn snap {:index :avet
                                                                :components [":aozora.keyBackup/did" "did:key:zNope"]}
                                                    everything test-blind-fn test-decrypt-fn)])
                            (.then (fn [results]
                                     (let [[eavt-alice avet-point avet-attr avet-limit eavt-missing avet-missing]
                                           (vec results)]
                                       (testing "cold :eavt [e] equals the hot filter (the getBackup query)"
                                         (is (= (set (eng/datoms db {:index :eavt :components ["keybackup/zAlice"]} everything))
                                                (set eavt-alice)))
                                         (is (= 2 (count eavt-alice))))
                                       (testing "cold :avet [attr value] point lookup returns one datom"
                                         (is (= [{:e "keybackup/zBob" :a ":aozora.keyBackup/did"
                                                  :v_edn "\"did:key:zBob\"" :added true}] avet-point)))
                                       (testing "cold :avet [attr] returns all subjects for the attribute"
                                         (is (= 2 (count avet-attr))))
                                       (testing ":limit caps cold rows"
                                         (is (= 1 (count avet-limit))))
                                       (testing "missing entity/value → empty"
                                         (is (= [] eavt-missing))
                                         (is (= [] avet-missing)))
                                       (done)))))))))))))

#?(:cljs
   (deftest cold-datoms-visible-is-required
     ;; Cross-platform note: cljs does NOT throw an arity error for a
     ;; single-fixed-arity fn called with too few args (JS's own permissive
     ;; calling convention just binds the missing params `undefined`,
     ;; unlike JVM Clojure's IFn, which always arity-checks) -- so the JVM
     ;; test's arity-mismatch trick doesn't carry over as-is. What DOES
     ;; carry over: passing `nil` for `visible?` (full arity otherwise) and
     ;; letting `(filter nil rows)` itself throw when actually invoked --
     ;; the real "no permissive default" guarantee, exercised through USE
     ;; rather than an arity probe. Since `cold-datoms` is Promise-returning
     ;; on cljs, that throw surfaces as a REJECTED promise, not a
     ;; synchronous `thrown?`.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             db (eng/transact (eng/empty-db) [["alice" "role" "admin"]])]
         (-> (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
             (.then (fn [chain-cid]
                      (let [snap (eng/latest-snapshot-cid get-fn chain-cid)]
                        (-> (eng/cold-datoms get-fn snap nil nil test-blind-fn test-decrypt-fn)
                            (.then (fn [_] (is false "expected cold-datoms to reject when visible? is nil"))
                                   (fn [_err] (is true "cold-datoms requires an explicit visibility decision -- no permissive default")))
                            (.then (fn [_] (done))))))))))))

#?(:cljs
   (deftest cold-datoms-visible-filters-rows
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             db (eng/transact (eng/empty-db)
                               [["alice" "role" "admin"] ["bob" "role" "user"]])
             alice-only (fn [{:keys [e]}] (= "alice" e))]
         (-> (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
             (.then (fn [chain-cid]
                      (let [snap (eng/latest-snapshot-cid get-fn chain-cid)]
                        (-> (eng/cold-datoms get-fn snap nil alice-only test-blind-fn test-decrypt-fn)
                            (.then (fn [rows]
                                     (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}] rows)
                                         "visible? actually excludes rows read cold from the snapshot")
                                     (done))))))))))))

#?(:cljs
   (deftest hydrate-db-roundtrips-the-whole-db
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)
             db0 (eng/transact (eng/empty-db)
                                [["alice" "role" "admin"] ["alice" "name" "Alice"]
                                 ["bob" "role" "user"]])]
         (-> (eng/snapshot! put! get-fn db0 nil test-blind-fn test-encrypt-fn)
             (.then (fn [c0]
                      (let [s0 (eng/latest-snapshot-cid get-fn c0)]
                        (-> (eng/hydrate-db get-fn s0 test-blind-fn test-decrypt-fn)
                            (.then (fn [db1]
                                     (is (= (set (eng/datoms db0 everything)) (set (eng/datoms db1 everything)))
                                         "hydrated db == original")
                                     (let [db2 (eng/transact db1 [["carol" "role" "guest"]])]
                                       (-> (eng/snapshot! put! get-fn db2 c0 test-blind-fn test-encrypt-fn)
                                           (.then (fn [c1]
                                                    (let [s1 (eng/latest-snapshot-cid get-fn c1)]
                                                      (-> (js/Promise.all
                                                           #js [(eng/cold-datoms get-fn s1 {:index :aevt :components ["role"]} everything
                                                                                  test-blind-fn test-decrypt-fn)
                                                                (eng/cold-datoms get-fn s1 {:index :avet :components ["role" "guest"]} everything
                                                                                  test-blind-fn test-decrypt-fn)
                                                                (eng/hydrate-db get-fn nil test-blind-fn test-decrypt-fn)])
                                                          (.then (fn [results]
                                                                   (let [[role-rows guest-rows empty-db] (vec results)]
                                                                     (testing "hydrate → assert → commit is a clean incremental write"
                                                                       (is (= 3 (count role-rows)))
                                                                       (is (= [{:e "carol" :a "role" :v_edn "\"guest\"" :added true}]
                                                                              guest-rows)))
                                                                     (testing "nil snapshot → empty db"
                                                                       (is (= [] (eng/datoms empty-db everything))))
                                                                     (done)))))))))))))))))))))

#?(:cljs
   (deftest commit-snapshots-are-content-addressed-and-deterministic
     (async done
       (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
             {:keys [put! get-fn]} (mem-store)
             {put2! :put! get-fn2 :get-fn} (mem-store)]
         (-> (js/Promise.all
              #js [(eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
                   (eng/snapshot! put2! get-fn2 db nil test-blind-fn test-encrypt-fn)])
             (.then (fn [results]
                      (let [[c1 c2] (vec results)
                            a (:state (eng/head get-fn c1))
                            b (:state (eng/head get-fn2 c2))]
                        (testing "committing the identical db content twice, from two independent
                                  chains/stores, yields the same snapshot CID -- arrangement's own
                                  content-addressing guarantee (preserved through encryption because
                                  test-encrypt-fn's nonce is content-derived, not random), exercised
                                  end to end through this namespace's snapshot! composition"
                          (is (= a b)))
                        (done)))))))))

#?(:cljs
   (deftest chain-tracks-multiple-snapshots
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             db1 (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
             db2 (eng/transact db1 [{:s "bob" :p "role" :o "user"}])]
         (-> (eng/snapshot! put! get-fn db1 nil test-blind-fn test-encrypt-fn)
             (.then (fn [c0]
                      (-> (eng/snapshot! put! get-fn db2 c0 test-blind-fn test-encrypt-fn)
                          (.then (fn [c1]
                                   (let [history (eng/chain get-fn c1)]
                                     (is (= 2 (count history)))
                                     (is (= [0 1] (map :seq history)))
                                     (is (not= (:state (first history)) (:state (second history)))
                                         "different db content -> different snapshot CID")
                                     (is (= (eng/latest-snapshot-cid get-fn c1)
                                            (ipld/link-cid (get (:state (last history)) "indexed")))
                                         "chain state is {\"indexed\" Link \"novelty\" []} wrapping the snapshot CID")
                                     (is (true? (eng/verify-chain get-fn c1)))
                                     (done))))))))))))

#?(:cljs
   (deftest verify-chain-catches-a-store-that-lies
     (async done
       (let [{:keys [put! get-fn store]} (mem-store)
             db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])]
         (-> (eng/snapshot! put! get-fn db nil test-blind-fn test-encrypt-fn)
             (.then (fn [c0]
                      (let [db2 (eng/transact db [{:s "bob" :p "role" :o "user"}])]
                        (-> (js/Promise.all
                             #js [(eng/snapshot! put! get-fn db2 c0 test-blind-fn test-encrypt-fn)
                                  (eng/snapshot! put! get-fn (eng/transact (eng/empty-db)
                                                                           [{:s "mallory" :p "role" :o "evil"}])
                                                 nil test-blind-fn test-encrypt-fn)])
                            (.then (fn [results]
                                     (let [[c1 other-cid] (vec results)]
                                       (is (true? (eng/verify-chain get-fn c1)))
                                       ;; splice a DIFFERENT (but validly dag-cbor-encoded) commit's
                                       ;; bytes under c0's own cid key -- a dishonest/corrupted store,
                                       ;; without changing c0 itself. verify-chain must catch this via
                                       ;; CID re-derivation, not throw.
                                       (swap! store assoc c0 (get @store other-cid))
                                       (is (false? (eng/verify-chain get-fn c1)))
                                       (done)))))))))))))

#?(:cljs
   (deftest commit-bang-appends-novelty-without-touching-snapshot
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0]
                      (testing "commit! on a fresh chain writes ONLY novelty -- nothing folded yet"
                        (is (= 1 (eng/novelty-size get-fn c0)))
                        (is (nil? (eng/latest-snapshot-cid get-fn c0))
                            "nothing folded yet -- an all-novelty chain has no indexed snapshot")
                        (-> (eng/hot-datoms get-fn c0 (constantly true) test-blind-fn test-decrypt-fn)
                            (.then (fn [rows]
                                     (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}] rows)
                                         "hot-datoms still sees the data via novelty merge")
                                     (done))))))))))))

#?(:cljs
   (deftest commit-bang-multiple-writes-accumulate-novelty
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] c1 test-encrypt-fn)))
             (.then (fn [c2]
                      (is (= 3 (eng/novelty-size get-fn c2)))
                      (is (= [0 1 2] (map :seq (eng/chain get-fn c2))))
                      (-> (eng/hot-datoms get-fn c2 (constantly true) test-blind-fn test-decrypt-fn)
                          (.then (fn [rows]
                                   (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                                            {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                                            {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
                                          (set rows))
                                       "no loss/dup across three sequential novelty-append commits")
                                   (done)))))))))))

#?(:cljs
   (deftest as-of-finds-the-exact-chain-cid-at-each-seq
     ;; as-of/chain are synchronous on BOTH platforms -- only the commit!
     ;; calls building the chain need Promise-chaining here. Each commit!'s
     ;; own cid is carried forward as an accumulating vector so every
     ;; earlier cid stays available for the final assertions.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
             (.then (fn [c0] (.then (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v2"}] c0 test-encrypt-fn)
                                    (fn [c1] [c0 c1]))))
             (.then (fn [[c0 c1]] (.then (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v3"}] c1 test-encrypt-fn)
                                         (fn [c2] [c0 c1 c2]))))
             (.then (fn [[c0 c1 c2]]
                      (is (= c0 (eng/as-of get-fn c2 0)))
                      (is (= c1 (eng/as-of get-fn c2 1)))
                      (is (= c2 (eng/as-of get-fn c2 2)))
                      (is (= c2 (eng/as-of get-fn c2 999))
                          "seq beyond the tip clamps to the tip")
                      (is (nil? (eng/as-of get-fn nil 0)))
                      (done))))))))

#?(:cljs
   (deftest hydrate-chain-gives-a-queryable-db-value-as-of-any-point
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v2"}] c0 test-encrypt-fn)))
             (.then (fn [c1] (-> (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                 (.then (fn [_] c1)))))
             (.then (fn [c1]
                      (-> (js/Promise.all
                           #js [(eng/hydrate-chain get-fn (eng/as-of get-fn c1 0) test-blind-fn test-decrypt-fn)
                                (eng/hydrate-chain get-fn (eng/as-of get-fn c1 1) test-blind-fn test-decrypt-fn)])
                          (.then (fn [results]
                                   (let [[db-at-0 db-at-1] (vec results)]
                                     (is (= #{"v1"} (get (eng/pull db-at-0 "alice") "role")))
                                     (is (= #{"v1" "v2"} (get (eng/pull db-at-1 "alice") "role")))
                                     (is (= #{["v1"]} (eng/query db-at-0 {:find '[?v] :where '[["alice" "role" ?v]]} everything))
                                         "hydrate-chain is queryable through arrangement.datalog/q too, not just pull")
                                     (done))))))))))))

#?(:cljs
   (deftest commit-serialized-basic-single-writer-usage
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             heads (atom {})
             cas! (fn [head-key expected new]
                    (let [result (atom nil)]
                      (swap! heads (fn [m]
                                     (if (= (get m head-key) expected)
                                       (do (reset! result new) (assoc m head-key new))
                                       (do (reset! result (get m head-key)) m))))
                      @result))]
         (-> (eng/commit-serialized! put! get-fn cas! "actor:alice" nil [{:s "alice" :p "role" :o "v1"}] test-encrypt-fn)
             (.then (fn [c0] (eng/commit-serialized! put! get-fn cas! "actor:alice" c0 [{:s "alice" :p "role" :o "v2"}] test-encrypt-fn)))
             (.then (fn [c1]
                      (is (= c1 (get @heads "actor:alice")) "head-key tracks the latest committed chain-cid")
                      (is (= [0 1] (map :seq (eng/chain get-fn c1))))
                      (done))))))))

#?(:cljs
   (deftest commit-serialized-retries-against-the-real-head-on-a-lost-race
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             heads (atom {})
             cas! (fn [head-key expected new]
                    (let [result (atom nil)]
                      (swap! heads (fn [m]
                                     (if (= (get m head-key) expected)
                                       (do (reset! result new) (assoc m head-key new))
                                       (do (reset! result (get m head-key)) m))))
                      @result))]
         (-> (eng/commit-serialized! put! get-fn cas! "actor:alice" nil [{:s "alice" :p "role" :o "v1"}] test-encrypt-fn)
             (.then (fn [c0] (.then (eng/commit-serialized! put! get-fn cas! "actor:alice" c0 [{:s "alice" :p "role" :o "v2"}] test-encrypt-fn)
                                    (fn [c1] c0))))
             ;; stale caller still believes head is c0; a concurrent writer already advanced it.
             (.then (fn [c0]
                      (.then (eng/commit-serialized! put! get-fn cas! "actor:alice" c0 [{:s "alice" :p "role" :o "v3-from-stale-caller"}] test-encrypt-fn)
                             (fn [c2]
                               (is (= [0 1 2] (map :seq (eng/chain get-fn c2))) "no fork -- a clean, gapless sequence")
                               (is (= c2 (get @heads "actor:alice")))
                               (done))))))))))

#?(:cljs
   (deftest commit-serialized-throws-on-persistent-contention
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             always-losing-cas! (fn [head-key expected new] nil)]
         (-> (eng/commit-serialized! put! get-fn always-losing-cas! "actor:bob" nil
                                     [{:s "bob" :p "x" :o "y"}] test-encrypt-fn 3)
             (.then (fn [_] (is false "should have rejected") (done))
                    (fn [err] (is (re-find #"max-cas-retries" (.-message err))) (done))))))))

#?(:cljs
   (deftest commit-with-report-bang-shape
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit-with-report! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [report]
                      (is (= #{:chain-cid-before :chain-cid-after :tx-data} (set (keys report))))
                      (is (nil? (:chain-cid-before report)))
                      (is (some? (:chain-cid-after report)))
                      (is (= [{:s "alice" :p "role" :o "admin"}] (:tx-data report)))
                      (done))))))))

#?(:cljs
   (deftest commit-serialized-with-report-bang-shape
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             heads (atom {})
             cas! (fn [head-key expected new]
                    (let [result (atom nil)]
                      (swap! heads (fn [m]
                                     (if (= (get m head-key) expected)
                                       (do (reset! result new) (assoc m head-key new))
                                       (do (reset! result (get m head-key)) m))))
                      @result))]
         (-> (eng/commit-serialized-with-report! put! get-fn cas! "actor:alice" nil
                                                 [{:s "alice" :p "role" :o "admin"}] test-encrypt-fn)
             (.then (fn [report]
                      (is (= #{:chain-cid-before :chain-cid-after :tx-data} (set (keys report))))
                      (is (= (:chain-cid-after report) (get @heads "actor:alice")))
                      (done))))))))

#?(:cljs
   (deftest since-shows-only-commits-after-a-seq
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
             (.then (fn [c0] (.then (eng/commit! put! get-fn [{:s "bob" :p "role" :o "v2"}] c0 test-encrypt-fn)
                                    (fn [c1] c1))))
             (.then (fn [c1] (.then (eng/commit! put! get-fn [{:s "carol" :p "role" :o "v3"}] c1 test-encrypt-fn)
                                    (fn [c2] c2))))
             (.then (fn [c2]
                      (.then (eng/since get-fn c2 0 test-decrypt-fn)
                             (fn [delta]
                               (is (nil? (get (eng/pull delta "alice") "role")) "alice's write was AT seq 0, not after it")
                               (is (= #{"v2"} (get (eng/pull delta "bob") "role")))
                               (is (= #{"v3"} (get (eng/pull delta "carol") "role")))
                               (done))))))))))

#?(:cljs
   (deftest history-includes-everything-across-a-fold
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "v1"}] nil test-encrypt-fn)
             (.then (fn [c0] (.then (eng/commit! put! get-fn [{:s "bob" :p "role" :o "v2"}] c0 test-encrypt-fn)
                                    (fn [c1] c1))))
             (.then (fn [c1] (.then (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                    (fn [folded] folded))))
             (.then (fn [folded] (.then (eng/commit! put! get-fn [{:s "carol" :p "role" :o "v3"}] folded test-encrypt-fn)
                                        (fn [c2] c2))))
             (.then (fn [c2]
                      (.then (eng/history get-fn c2 test-blind-fn test-decrypt-fn)
                             (fn [full-history]
                               (is (= #{"v1"} (get (eng/pull full-history "alice") "role"))
                                   "alice's fact was folded into the indexed snapshot -- still visible")
                               (is (= #{"v2"} (get (eng/pull full-history "bob") "role")))
                               (is (= #{"v3"} (get (eng/pull full-history "carol") "role"))
                                   "carol's fact is post-fold novelty -- also visible")
                               (done))))))))))

#?(:cljs
   (deftest hot-datoms-merges-indexed-and-novelty
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)))
             (.then (fn [folded]
                      (-> (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] folded test-encrypt-fn)
                          (.then (fn [c2]
                                   (is (some? (eng/latest-snapshot-cid get-fn c2)) "folded snapshot carries forward")
                                   (is (= 1 (eng/novelty-size get-fn c2)) "only the post-fold write is novelty")
                                   (-> (js/Promise.all
                                        #js [(eng/hot-datoms get-fn c2 everything test-blind-fn test-decrypt-fn)
                                             (eng/hot-datoms get-fn c2 {:index :eavt :components ["carol"]} everything
                                                             test-blind-fn test-decrypt-fn)
                                             (eng/hot-datoms get-fn c2 {:index :eavt :components ["alice"]} everything
                                                             test-blind-fn test-decrypt-fn)])
                                       (.then (fn [results]
                                                (let [[all-rows carol-rows alice-rows] (vec results)]
                                                  (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                                                           {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                                                           {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
                                                         (set all-rows))
                                                      "reads see both the folded-in history AND the fresh novelty")
                                                  (testing "filtered hot-datoms honors opts across the snapshot/novelty split"
                                                    (is (= [{:e "carol" :a "role" :v_edn "\"guest\"" :added true}] carol-rows))
                                                    (is (= [{:e "alice" :a "role" :v_edn "\"admin\"" :added true}] alice-rows)))
                                                  (done)))))))))))))))

#?(:cljs
   (deftest hot-datoms-visible-is-required
     ;; multi-arity (2-arg / 3-arg), so cljs DOES arity-check this one
     ;; correctly (unlike cold-datoms's single-fixed-arity case above) --
     ;; kept non-empty anyway for consistency with the rest of this suite.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0]
                      (is (thrown? js/Error (eng/hot-datoms get-fn c0))
                          "hot-datoms requires an explicit visibility decision -- no permissive default")
                      (done))))))))

#?(:cljs
   (deftest hot-datoms-visible-filters-rows
     ;; alice+bob are folded into the indexed snapshot (cold half); carol is
     ;; committed AFTER the fold, so she's pure novelty (hot half). A visible?
     ;; that excludes one entity from EACH half proves visible? reaches both
     ;; composed paths, not just one.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             alice-only (fn [{:keys [e]}] (= "alice" e))]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)))
             (.then (fn [folded] (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] folded test-encrypt-fn)))
             (.then (fn [c2] (eng/hot-datoms get-fn c2 alice-only test-blind-fn test-decrypt-fn)))
             (.then (fn [rows]
                      (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}} (set rows))
                          "visible? excludes bob (cold/snapshot half) AND carol (hot/novelty half)")
                      (done))))))))

#?(:cljs
   (deftest fold-bang-compacts-novelty-and-preserves-data
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1]
                      (-> (eng/hot-datoms get-fn c1 everything test-blind-fn test-decrypt-fn)
                          (.then (fn [before-rows]
                                   (-> (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                       (.then (fn [folded]
                                                (is (= 0 (eng/novelty-size get-fn folded)) "fold resets the tail")
                                                (is (some? (eng/latest-snapshot-cid get-fn folded)))
                                                (-> (js/Promise.all
                                                     #js [(eng/hot-datoms get-fn folded everything test-blind-fn test-decrypt-fn)
                                                          (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                                                           test-blind-fn test-decrypt-fn)])
                                                    (.then (fn [results]
                                                             (let [[after-rows cold-rows] (vec results)
                                                                   before (set before-rows)]
                                                               (is (= before (set after-rows))
                                                                   "folding never loses or duplicates data")
                                                               (is (= before (set cold-rows))
                                                                   "the fold really did index the data -- a cold-only read (no novelty) sees it")
                                                               (done))))))))))))))))))

#?(:cljs
   (deftest fold-bang-is-deterministic-across-independent-stores
     ;; folding the identical (indexed, novelty) history from two independent
     ;; stores yields the same snapshot CID -- content-addressing holds
     ;; through the fold (preserved through encryption by test-encrypt-fn's
     ;; content-derived, not random, nonce), so concurrent/redundant folds
     ;; converge safely
     (async done
       (letfn [(mk-fold []
                 (let [{:keys [put! get-fn]} (mem-store)]
                   (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
                       (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
                       (.then (fn [c1] (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)))
                       (.then (fn [folded] (eng/latest-snapshot-cid get-fn folded))))))]
         (-> (js/Promise.all #js [(mk-fold) (mk-fold)])
             (.then (fn [results]
                      (let [[cid1 cid2] (vec results)]
                        (is (= cid1 cid2))
                        (done)))))))))

#?(:cljs
   (deftest should-fold-flags-at-threshold
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1]
                      (is (false? (eng/should-fold? get-fn c1 3)))
                      (is (true? (eng/should-fold? get-fn c1 2)))
                      (is (false? (eng/should-fold? get-fn c1)) "default threshold is well above 2")
                      (done))))))))

#?(:cljs
   (deftest normalize-state-reads-pre-d1-bare-link-chains
     ;; a chain committed by the pre-D1 code (state = a bare snapshot Link,
     ;; no {indexed novelty} wrapper) still reads correctly under the new
     ;; code -- zero migration step for already-deployed actors
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)
             db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])]
         (-> (qs/commit! put! db nil qs/current-schema-version test-blind-fn test-encrypt-fn)
             (.then (fn [snap-cid]
                      (let [pre-d1-chain (cd/commit! put! get-fn (ipld/link snap-cid) nil)]
                        (is (= snap-cid (eng/latest-snapshot-cid get-fn pre-d1-chain)))
                        (is (= 0 (eng/novelty-size get-fn pre-d1-chain)))
                        (-> (eng/hot-datoms get-fn pre-d1-chain everything test-blind-fn test-decrypt-fn)
                            (.then (fn [rows]
                                     (is (= (set (eng/datoms db everything)) (set rows)))
                                     (-> (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] pre-d1-chain test-encrypt-fn)
                                         (.then (fn [c1]
                                                  (is (= 1 (eng/novelty-size get-fn c1)))
                                                  (-> (eng/hot-datoms get-fn c1 everything test-blind-fn test-decrypt-fn)
                                                      (.then (fn [rows2]
                                                               (testing "committing new novelty on top of a pre-D1 chain works (mixed-era chain)"
                                                                 (is (= (conj (set (eng/datoms db everything))
                                                                              {:e "bob" :a "role" :v_edn "\"user\"" :added true})
                                                                        (set rows2))))
                                                               (done))))))))))))))))))

#?(:cljs
   (deftest link-ref-survives-fold-and-cold-read
     ;; a Link-valued datom, folded into a persisted snapshot and read back
     ;; cold, is still a real Link -- and refs/refs-to still finds it on the
     ;; rehydrated hot db (the full novelty -> fold -> cold-read round trip)
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "knows" :o bob-link}] nil test-encrypt-fn)
             (.then (fn [c0]
                      (-> (eng/fold! put! get-fn c0 test-blind-fn test-encrypt-fn test-decrypt-fn)
                          (.then (fn [folded]
                                   (let [snap (eng/latest-snapshot-cid get-fn folded)]
                                     (-> (js/Promise.all
                                          #js [(eng/cold-datoms get-fn snap {:index :eavt :components ["alice"]} (constantly true)
                                                                 test-blind-fn test-decrypt-fn)
                                               (eng/hydrate-db get-fn snap test-blind-fn test-decrypt-fn)
                                               (eng/hot-datoms get-fn c0 (constantly true) test-blind-fn test-decrypt-fn)
                                               (eng/cold-datoms get-fn snap nil (constantly true) test-blind-fn test-decrypt-fn)])
                                         (.then (fn [results]
                                                  (let [[cold-rows db hot-rows cold-rows-2] (vec results)
                                                        row (first cold-rows)]
                                                    (testing "cold-datoms reconstructs the Link (not the raw edn-safe vector)"
                                                      (is (= "[\"ipld/link\" \"bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m\"]"
                                                             (:v_edn row))))
                                                    (testing "hydrate-db reconstructs a real Link, so refs-to finds it again"
                                                      (is (= {"knows" #{"alice"}} (qs/refs-to db bob-link))))
                                                    (testing "hot-datoms (novelty path, via dag-cbor) agrees with the cold path"
                                                      (is (= (set hot-rows) (set cold-rows-2))))
                                                    (done))))))))))))))))

;; ── datom retraction Phase 1 (ADR-2607071610) ────────────────────────────────

(deftest transact-applies-retraction-forms
  (let [everything (constantly true)
        db (eng/transact (eng/empty-db)
                         [["e1" ":a/x" "v1"] ["e1" ":a/y" "v2"] ["e2" ":a/x" "v3"]])]
    (testing "[:db/retract e a v] removes exactly one datom"
      (let [db' (eng/transact db [[:db/retract "e1" ":a/x" "v1"]])]
        (is (= 2 (count (eng/datoms db' {:index :eavt} everything))))
        (is (= [] (eng/datoms db' {:index :eavt :components ["e1" ":a/x"]} everything)))))
    (testing "[:db/retractEntity e] removes the whole entity"
      (let [db' (eng/transact db [[:db/retractEntity "e1"]])]
        (is (= [] (eng/datoms db' {:index :eavt :components ["e1"]} everything)))
        (is (= 1 (count (eng/datoms db' {:index :eavt} everything))))))
    (testing "retract then re-assert wins in order"
      (let [db' (eng/transact db [[:db/retractEntity "e1"] ["e1" ":a/x" "v9"]])]
        (is (= [{:e "e1" :a ":a/x" :v_edn "\"v9\"" :added true}]
               (eng/datoms db' {:index :eavt :components ["e1"]} everything)))))))

#?(:clj
   (deftest commit-retraction-cancels-across-blocks-and-fold
     ;; assert in block 1 → retract in block 2 → current reads drop it, both
     ;; before AND after fold (ADR-2607071610 Phase 1: current-state 正しさ)
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn
                           [["post/1" ":yoro.post/text" "hello"]
                            ["post/1" ":yoro.post/author" "did:key:zA"]
                            ["post/2" ":yoro.post/text" "keep"]]
                           nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [[:db/retract "post/1" ":yoro.post/text" "hello"]]
                           c1 test-encrypt-fn)]
       (testing "hot-datoms: novelty retract cancels earlier novelty assert"
         (let [rows (eng/hot-datoms get-fn c2 everything test-blind-fn test-decrypt-fn)]
           (is (= #{["post/1" ":yoro.post/author"] ["post/2" ":yoro.post/text"]}
                  (set (map (juxt :e :a) rows))))))
       (testing "retract survives fold (snapshot actually shrinks)"
         (let [c3 (eng/fold! put! get-fn c2 test-blind-fn test-encrypt-fn test-decrypt-fn)
               rows (eng/hot-datoms get-fn c3 everything test-blind-fn test-decrypt-fn)]
           (is (= #{["post/1" ":yoro.post/author"] ["post/2" ":yoro.post/text"]}
                  (set (map (juxt :e :a) rows))))))
       (testing "retract cancels a row already IN the indexed snapshot"
         (let [cf (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
               cr (eng/commit! put! get-fn [[:db/retractEntity "post/1"]] cf test-encrypt-fn)
               rows (eng/hot-datoms get-fn cr everything test-blind-fn test-decrypt-fn)]
           (is (= #{["post/2" ":yoro.post/text"]}
                  (set (map (juxt :e :a) rows))))
           (testing "and the NEXT fold bakes the shrink into the snapshot"
             (let [cff (eng/fold! put! get-fn cr test-blind-fn test-encrypt-fn test-decrypt-fn)
                   rows (eng/hot-datoms get-fn cff everything test-blind-fn test-decrypt-fn)]
               (is (= #{["post/2" ":yoro.post/text"]}
                      (set (map (juxt :e :a) rows)))))))))))

#?(:clj
   (deftest old-blocks-without-op-stay-valid
     ;; wire compat: plain 3-key quads (all pre-ADR blocks) read back as asserts
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn [["e" ":a/x" "v"]] nil test-encrypt-fn)
           rows (eng/hot-datoms get-fn c1 everything test-blind-fn test-decrypt-fn)]
       (is (= [{:e "e" :a ":a/x" :v_edn "\"v\"" :added true}] (vec rows))))))
