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
                 #_:clj-kondo/ignore (eng/datoms db))
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
                    #_:clj-kondo/ignore (eng/cold-datoms get-fn snap nil))
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
                 #_:clj-kondo/ignore (eng/q db [nil nil nil]))
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

(deftest query-plans-selective-connected-triples-before-broad-clauses
  (let [db (eng/transact
            (eng/empty-db)
            (concat [{:s "alice" :p "role" :o "admin"}
                     {:s "alice" :p "name" :o "Alice"}]
                    (mapcat (fn [i]
                              [{:s (str "user-" i) :p "role" :o "user"}
                               {:s (str "user-" i) :p "name" :o (str "User " i)}])
                            (range 100))))
        everything (constantly true)
        query {:find '[?name]
               :where '[[?s "name" ?name]
                        [?s "role" "admin"]]}
        plan (eng/datalog-query-plan db query everything)]
    (is (:optimized? plan))
    (is (= '[[?s "role" "admin"] [?s "name" ?name]]
           (get-in plan [:query :where])))
    (is (= [1 0] (mapv :id (:plan plan))))
    (is (= #{["Alice"]} (eng/query db query everything)))))

(deftest query-plan-preserves-order-for-binding-sensitive-forms
  (let [db (eng/empty-db)
        query {:find '[?s]
               :where '[[?s "age" ?age] [(> ?age 18)]]}
        plan (eng/datalog-query-plan db query (constantly true))]
    (is (false? (:optimized? plan)))
    (is (= (:where query) (get-in plan [:query :where])))))

(deftest query-plan-consumes-scoped-materialized-statistics-without-scanning
  (let [visibility-calls (atom 0)
        visible? (fn [_] (swap! visibility-calls inc) true)
        query {:find '[?name]
               :where '[[?s "name" ?name] [?s "role" "admin"]]
               :statistics-scope "tenant-a/public-v1"
               :query-epoch 7
               :query-statistics {"visibility-scope" "tenant-a/public-v1"
                                  "epoch" 7
                                  "clauses" [{"pattern" [nil "name" nil] "rows" 101}
                                             {"pattern" [nil "role" "admin"] "rows" 1}]}}
        plan (eng/datalog-query-plan (eng/empty-db) query visible?)]
    (is (= [1 0] (mapv :id (:plan plan))))
    (is (= [:materialized-statistics :materialized-statistics]
           (mapv :estimate-source (:plan plan))))
    (is (zero? @visibility-calls))))

(deftest query-plan-rejects-statistics-from-another-visibility-scope
  (let [query {:find '[?s]
               :where '[[?s "role" "admin"]]
               :statistics-scope "tenant-b/private-v1"
               :query-statistics {"visibility-scope" "tenant-a/public-v1"
                                  "clauses" [{"pattern" [nil "role" "admin"] "rows" 1}]}}
        plan (eng/datalog-query-plan (eng/empty-db) query (constantly true))]
    (is (= :visible-scan (-> plan :plan first :estimate-source)))))

(deftest query-plan-falls-back-when-materialized-statistics-are-stale
  (let [query {:find '[?s] :where '[[?s "role" "admin"]]
               :statistics-scope "tenant-a/public-v1"
               :query-epoch 9 :max-statistics-age 1
               :query-statistics {"visibility-scope" "tenant-a/public-v1"
                                  "epoch" 7
                                  "clauses" [{"pattern" [nil "role" "admin"]
                                              "rows" 1}]}}
        plan (eng/datalog-query-plan (eng/empty-db) query (constantly true))]
    (is (= :visible-scan (-> plan :plan first :estimate-source)))))

(deftest query-visible-is-required
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])]
    (is (thrown? #?(:clj clojure.lang.ArityException :cljs js/Error)
                 #_:clj-kondo/ignore (eng/query db {:find '[?s] :where '[[?s "role" "admin"]]})))))

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

;; ── entid / ident: :db/ident as a plain attribute, no schema-enforced
;; uniqueness (Datomic ships :db/unique :db.unique/identity on :db/ident
;; itself; this substrate has no unique-attribute enforcement yet, so these
;; tests only assert the well-formed case) ───────────────────────────────────

(deftest entid-passes-through-a-plain-non-keyword-id
  (is (= "alice" (eng/entid (eng/empty-db) "alice"))))

(deftest entid-resolves-a-keyword-ident-via-a-real-transact-shaped-attr
  ;; ":db/ident" (colon-prefixed) matches what entities->datoms actually
  ;; produces for a real {:db/id "e" :db/ident :my/thing} tx-edn item -- see
  ;; ->quad's (str a) coercion, not this test file's OWN convention
  ;; elsewhere of bare unprefixed :p strings for low-level transact calls.
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p ":db/ident" :o ":person/alice"}
                                          {:s "alice" :p ":name" :o "Alice"}])]
    (is (= "alice" (eng/entid db :person/alice)))))

(deftest entid-returns-nil-for-an-ident-nothing-asserts
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p ":name" :o "Alice"}])]
    (is (nil? (eng/entid db :person/nobody)))))

(deftest ident-is-the-inverse-of-entid
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p ":db/ident" :o ":person/alice"}])]
    (is (= :person/alice (eng/ident db "alice")))))

(deftest ident-is-nil-when-no-db-ident-is-asserted
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p ":name" :o "Alice"}])]
    (is (nil? (eng/ident db "alice")))))

(deftest ident-is-nil-when-db-ident-value-is-not-keyword-shaped
  (let [db (eng/transact (eng/empty-db) [{:s "alice" :p ":db/ident" :o "not-a-keyword"}])]
    (is (nil? (eng/ident db "alice")))))

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
   (deftest commit-serialized-effective-prunes-persisted-no-ops
     (let [{:keys [put! get-fn store]} (mem-store)
           heads (atom {})
           cas-calls (atom 0)
           cas! (fn [head-key expected new]
                  (swap! cas-calls inc)
                  (if (= (get @heads head-key) expected)
                    (do (swap! heads assoc head-key new) new)
                    (get @heads head-key)))
           initial (eng/commit-serialized-effective!
                    put! get-fn cas! "actor:alice" nil
                    [{:s "alice" :p "role" :o "admin"}]
                    test-encrypt-fn test-blind-fn test-decrypt-fn)
           blocks-before (count @store)
           calls-before @cas-calls
           no-op (eng/commit-serialized-effective!
                  put! get-fn cas! "actor:alice" (:chain-cid-after initial)
                  [{:s "alice" :p "role" :o "admin"}
                   {:s "alice" :p "missing" :o "value" :op :retract}]
                  test-encrypt-fn test-blind-fn test-decrypt-fn)]
       (is (false? (:committed? no-op)))
       (is (= (:chain-cid-before no-op) (:chain-cid-after no-op)))
       (is (empty? (:effective-deltas no-op)))
       (is (= blocks-before (count @store)) "semantic no-op writes no CAS blocks")
       (is (= calls-before @cas-calls) "semantic no-op does not touch mutable head"))))

#?(:clj
   (deftest transaction-slice-is-prefix-pruned-and-novelty-correct
     (let [{:keys [put! get-fn]} (mem-store)
           seed (mapv (fn [i] [(str "entity-" i) "role" "user"]) (range 1000))
           c0 (eng/commit! put! get-fn seed nil test-encrypt-fn)
           folded (eng/fold! put! get-fn c0 test-blind-fn test-encrypt-fn test-decrypt-fn)
           c1 (eng/commit! put! get-fn
                           [[:db/retract "entity-42" "role" "user"]
                            [:db/add "entity-42" "role" "admin"]
                            [:db/add "unrelated" "role" "guest"]]
                           folded test-encrypt-fn)
           slice (eng/hydrate-transaction-slice
                  get-fn c1 [[:db/add "entity-42" "role" "admin"]]
                  test-blind-fn test-decrypt-fn)]
       (is (= #{"entity-42"} (set (keys (:spo slice))))
           "snapshot prefix and novelty replay exclude unrelated subjects")
       (is (= #{"admin"} (get-in slice [:spo "entity-42" "role"])))
       (is (empty? (:effective-deltas
                    (eng/transact-effective slice
                                            [[:db/add "entity-42" "role" "admin"]])))))))

#?(:clj
   (deftest transaction-slice-skips-unrelated-indexed-novelty-ciphertexts
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas! (fn [head-key expected new]
                  (if (= (get @heads head-key) expected)
                    (do (swap! heads assoc head-key new) new)
                    (get @heads head-key)))
           head (reduce (fn [head i]
                          (:chain-cid-after
                           (eng/commit-serialized-effective!
                            put! get-fn cas! "actors" head
                            [[(str "entity-" i) "role" "user"]]
                            test-encrypt-fn test-blind-fn test-decrypt-fn)))
                        nil (range 20))
           decrypts (atom 0)
           reads (atom 0)
           counted-get (fn [cid] (swap! reads inc) (get-fn cid))
           counting-decrypt (fn [ciphertext]
                              (swap! decrypts inc)
                              (test-decrypt-fn ciphertext))
           slice (eng/hydrate-transaction-slice
                  counted-get head [["entity-7" "role" "user"]]
                  test-blind-fn counting-decrypt)]
       (is (= #{"user"} (get-in slice [:spo "entity-7" "role"])))
       (is (= 1 @decrypts)
           "20 novelty entries are classified by blind token; only the matching tx ciphertext is opened")
       (is (<= @reads 6)
           "20 entries fit in two metadata segments instead of requiring 20 queue-node reads"))))

#?(:clj
   (deftest partial-fold-preserves-subject-segment-pruning
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas! (fn [head-key expected new]
                  (if (= (get @heads head-key) expected)
                    (do (swap! heads assoc head-key new) new)
                    (get @heads head-key)))
           head (reduce (fn [head i]
                          (:chain-cid-after
                           (eng/commit-serialized-effective!
                            put! get-fn cas! "actors" head
                            [[(str "entity-" i) "role" "user"]]
                            test-encrypt-fn test-blind-fn test-decrypt-fn)))
                        nil (range 20))
           folded (eng/fold! put! get-fn head ipld/link? 5
                             test-blind-fn test-encrypt-fn test-decrypt-fn)
           reads (atom 0)
           decrypts (atom 0)
           counted-get (fn [cid] (swap! reads inc) (get-fn cid))
           slice (eng/hydrate-transaction-slice
                  counted-get folded [["entity-10" "role" "user"]]
                  test-blind-fn
                  (fn [ciphertext] (swap! decrypts inc) (test-decrypt-fn ciphertext)))]
       (is (= 15 (eng/novelty-size get-fn folded)))
       (is (= #{"user"} (get-in slice [:spo "entity-10" "role"])))
       (is (= 1 @decrypts) "only the matching remaining novelty ciphertext is opened")
       (is (<= @reads 7) "remaining entries are rebuilt into one verified metadata segment"))))

#?(:clj
   (deftest fold-serialized-if-needed-enforces-threshold-and-bounded-progress
     (let [{:keys [put! get-fn store]} (mem-store)
           heads (atom {})
           cas-calls (atom 0)
           cas! (fn [head-key expected new]
                  (swap! cas-calls inc)
                  (if (= (get @heads head-key) expected)
                    (do (swap! heads assoc head-key new) new)
                    (get @heads head-key)))
           head3 (reduce (fn [head i]
                           (:chain-cid-after
                            (eng/commit-serialized-effective!
                             put! get-fn cas! "actors" head
                             [[(str "entity-" i) "role" "user"]]
                             test-encrypt-fn test-blind-fn test-decrypt-fn)))
                         nil (range 3))
           blocks-before (count @store)
           calls-before @cas-calls
           below (eng/fold-serialized-if-needed!
                  put! get-fn cas! "actors" head3
                  test-blind-fn test-encrypt-fn test-decrypt-fn
                  {:threshold 4 :max-novelty 2})
           blocks-after-below (count @store)
           calls-after-below @cas-calls
           head4 (:chain-cid-after
                  (eng/commit-serialized-effective!
                   put! get-fn cas! "actors" head3
                   [["entity-3" "role" "user"]]
                   test-encrypt-fn test-blind-fn test-decrypt-fn))
           folded (eng/fold-serialized-if-needed!
                   put! get-fn cas! "actors" head4
                   test-blind-fn test-encrypt-fn test-decrypt-fn
                   {:threshold 4 :max-novelty 2})]
       (is (false? (:committed? below)))
       (is (= blocks-before blocks-after-below) "below threshold writes no blocks")
       (is (= calls-before calls-after-below) "below threshold performs no CAS")
       (is (true? (:committed? folded)))
       (is (= 4 (:novelty-before folded)))
       (is (= 2 (:novelty-after folded)) "one scheduler invocation makes bounded progress")
       (is (= (:chain-cid-after folded) (get @heads "actors"))))))

#?(:clj
   (deftest commit-serialized-effective-publishes-only-effective-deltas
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas! (fn [head-key expected new]
                  (if (= (get @heads head-key) expected)
                    (do (swap! heads assoc head-key new) new)
                    (get @heads head-key)))
           initial (eng/commit-serialized-effective!
                    put! get-fn cas! "actor:alice" nil
                    [{:s "alice" :p "role" :o "admin"}]
                    test-encrypt-fn test-blind-fn test-decrypt-fn)
           report (eng/commit-serialized-effective!
                   put! get-fn cas! "actor:alice" (:chain-cid-after initial)
                   [[:db/retract "alice" "role" "admin"]
                    [:db/add "alice" "role" "user"]
                    [:db/add "alice" "role" "user"]]
                   test-encrypt-fn test-blind-fn test-decrypt-fn)
           rows (eng/hot-datoms get-fn (:chain-cid-after report)
                                (constantly true) test-blind-fn test-decrypt-fn)]
       (is (true? (:committed? report)))
       (is (= [:retract :assert] (mapv :op (:effective-deltas report))))
       (is (= #{{:e "alice" :a "role" :v_edn "\"user\"" :added true}}
              (set rows))))))

#?(:clj
   (deftest commit-serialized-effective-renormalizes-after-lost-cas
     (let [{:keys [put! get-fn]} (mem-store)
           heads (atom {})
           cas-calls (atom 0)
           cas! (fn [head-key expected new]
                  (swap! cas-calls inc)
                  (if (= (get @heads head-key) expected)
                    (do (swap! heads assoc head-key new) new)
                    (get @heads head-key)))
           c0 (:chain-cid-after
               (eng/commit-serialized-effective!
                put! get-fn cas! "actor:alice" nil
                [[:db/add "alice" "role" "admin"]]
                test-encrypt-fn test-blind-fn test-decrypt-fn))
           c1 (:chain-cid-after
               (eng/commit-serialized-effective!
                put! get-fn cas! "actor:alice" c0
                [[:db/add "alice" "role" "user"]
                 [:db/add "alice" "status" "active"]]
                test-encrypt-fn test-blind-fn test-decrypt-fn))
           calls-before @cas-calls
           stale-report (eng/commit-serialized-effective!
                         put! get-fn cas! "actor:alice" c0
                         [[:db/add "alice" "role" "user"]]
                         test-encrypt-fn test-blind-fn test-decrypt-fn)]
       (is (false? (:committed? stale-report)))
       (is (= c1 (:chain-cid-after stale-report)))
       (is (= 1 (:attempts stale-report)))
       (is (= (inc calls-before) @cas-calls)
           "first stale attempt loses CAS; retry becomes no-op and performs no second CAS")
       (is (= [0 1] (mapv :seq (eng/chain get-fn c1))) "head has no duplicate commit"))))

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
                    #_:clj-kondo/ignore (eng/hot-datoms get-fn c0))
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
   (deftest fold-bang-max-novelty-folds-a-bounded-prefix-and-leaves-the-rest
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "actor-0" :p "role" :o "member"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "actor-1" :p "role" :o "member"}] c0 test-encrypt-fn)
           c2 (eng/commit! put! get-fn [{:s "actor-2" :p "role" :o "member"}] c1 test-encrypt-fn)
           c3 (eng/commit! put! get-fn [{:s "actor-3" :p "role" :o "member"}] c2 test-encrypt-fn)
           before (set (eng/hot-datoms get-fn c3 everything test-blind-fn test-decrypt-fn))
           folded (eng/fold! put! get-fn c3 ipld/link? 2 test-blind-fn test-encrypt-fn test-decrypt-fn)]
       (is (= 2 (eng/novelty-size get-fn folded))
           "max-novelty=2 against 4 novelty entries folds only the oldest 2, leaves 2")
       (is (some? (eng/latest-snapshot-cid get-fn folded)))
       (is (= before (set (eng/hot-datoms get-fn folded everything test-blind-fn test-decrypt-fn)))
           "hot-datoms still sees everything -- the 2 unfolded entries are still novelty, still merged in")
       (is (= #{{:e "actor-0" :a "role" :v_edn "\"member\"" :added true}
                {:e "actor-1" :a "role" :v_edn "\"member\"" :added true}}
              (set (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                    test-blind-fn test-decrypt-fn)))
           "only the 2 oldest (actor-0, actor-1) actually got indexed into the new snapshot; actor-2/actor-3 are still in novelty, not yet cold")
       (let [folded-again (eng/fold! put! get-fn folded ipld/link? 2 test-blind-fn test-encrypt-fn test-decrypt-fn)]
         (is (= 0 (eng/novelty-size get-fn folded-again))
             "a second bounded fold with the same budget clears the remaining tail")
         (is (= before (set (eng/hot-datoms get-fn folded-again everything test-blind-fn test-decrypt-fn)))
             "two bounded folds together lose/duplicate nothing vs. one unbounded fold")
         (is (= before (set (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded-again) nil everything
                                             test-blind-fn test-decrypt-fn)))
             "after both bounded folds, everything is indexed -- same end state an unbounded fold would reach")))))

#?(:clj
   (deftest fold-bang-max-novelty-nil-is-unbounded-exactly-like-the-default-arity
     ;; regression: explicit nil max-novelty (the 8-arity form) must behave
     ;; identically to the pre-existing 6-/7-arity unbounded calls -- this is
     ;; the "default nil = unbounded, zero behavior change for every existing
     ;; caller" contract the fold! docstring promises.
     (let [{:keys [put! get-fn]} (mem-store)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)
           folded (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn test-decrypt-fn)]
       (is (= 0 (eng/novelty-size get-fn folded)) "nil max-novelty still fully compacts"))))

;; ── kotoba-lang/kotobase-peer#16: front/back persistent-queue novelty ──────

#?(:clj
   (deftest legacy-flat-vector-novelty-state-reads-and-migrates-correctly
     (testing "a chain hand-constructed with the ORIGINAL (pre-#16) flat-vector
               \"novelty\" state shape -- exactly what every already-deployed
               chain has on disk -- reads correctly through every public
               accessor, and a NEW commit! on top of it migrates to the
               front/back shape without losing or reordering anything"
       ;; put-tx-block! is private -- commit two entries the normal (public)
       ;; way so real, correctly-shaped tx-blocks land in the store, then pull
       ;; their links back out via plain ipld/get-node (public) to build a
       ;; hand-constructed LEGACY-shaped {"indexed" .. "novelty" [...]} state
       ;; from them, exactly what an already-deployed pre-#16 chain has on
       ;; disk today.
       (let [{:keys [put! get-fn]} (mem-store)
             c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)
             {:keys [state]} (cd/head get-fn c1)
             walk-chain (fn walk-chain [head-link]
                          (loop [cid (some-> head-link ipld/link-cid) acc []]
                            (if (nil? cid)
                              acc
                              (let [{:strs [e rest]} (ipld/get-node get-fn cid)]
                                (recur (some-> rest ipld/link-cid) (conj acc e))))))
             tx-links (into (walk-chain (get state "novelty-front"))
                            (rseq (walk-chain (get state "novelty-back"))))
             legacy-state {"indexed" nil "novelty" tx-links}
             legacy-chain-cid (cd/commit! put! get-fn legacy-state nil)
             everything (constantly true)]
         (testing "reads work on the untouched legacy chain"
           (is (= 2 (eng/novelty-size get-fn legacy-chain-cid)))
           (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                    {:e "bob" :a "role" :v_edn "\"user\"" :added true}}
                  (set (eng/hot-datoms get-fn legacy-chain-cid everything test-blind-fn test-decrypt-fn)))))
         (testing "a new commit! migrates it to front/back, preserving chronological order and all data"
           (let [migrated (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] legacy-chain-cid test-encrypt-fn)]
             (is (= 3 (eng/novelty-size get-fn migrated)) "migration preserves the 2 legacy entries + the 1 new one")
             (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                      {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                      {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
                    (set (eng/hot-datoms get-fn migrated everything test-blind-fn test-decrypt-fn))))
             (testing "folding the migrated chain indexes everything, in the original chronological order"
               (let [folded (eng/fold! put! get-fn migrated test-blind-fn test-encrypt-fn test-decrypt-fn)]
                 (is (= 0 (eng/novelty-size get-fn folded)))
                 (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                          {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                          {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
                        (set (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                              test-blind-fn test-decrypt-fn))))))))))))

#?(:clj
   (deftest bounded-fold-across-front-exhaustion-preserves-chronological-retraction-order
     (testing "the counterexample from kotoba-lang/kotobase-peer#16's design-review
               comment, made concrete: assert X, retract X, assert X (3 writes,
               chronological order) -- if replayed out of order (e.g. newest-
               first), the final state would be WRONG (the newest assert would
               get incorrectly cancelled by the older retraction). This test
               forces the exact scenario a naive/incorrect design would get
               wrong: bound the first fold to 1 entry (drains front down to a
               state where a LATER bounded fold must reverse back into a fresh
               front), so the assert/retract/assert sequence straddles that
               front-exhaustion boundary, and asserts the final state is still
               X asserted (chronologically correct), not X retracted."
       (let [{:keys [put! get-fn]} (mem-store)
             c0 (eng/commit! put! get-fn [{:s "x" :p "flag" :o "on"}] nil test-encrypt-fn)               ; #1 assert X
             c1 (eng/commit! put! get-fn [[:db/retract "x" "flag" "on"]] c0 test-encrypt-fn)              ; #2 retract X
             c2 (eng/commit! put! get-fn [{:s "x" :p "flag" :o "on"}] c1 test-encrypt-fn)                 ; #3 assert X (newest)
             everything (constantly true)]
         (is (= 3 (eng/novelty-size get-fn c2)))
         (let [folded-1 (eng/fold! put! get-fn c2 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)]
           (is (= 2 (eng/novelty-size get-fn folded-1)) "bounded fold #1 (n=1) drains exactly the oldest entry")
           (let [folded-2 (eng/fold! put! get-fn folded-1 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)]
             (is (= 1 (eng/novelty-size get-fn folded-2)) "bounded fold #2 (n=1) drains the next oldest")
             (let [folded-3 (eng/fold! put! get-fn folded-2 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)]
               (is (= 0 (eng/novelty-size get-fn folded-3)) "bounded fold #3 (n=1) drains the last entry")
               (is (= #{{:e "x" :a "flag" :v_edn "\"on\"" :added true}}
                      (set (eng/hot-datoms get-fn folded-3 everything test-blind-fn test-decrypt-fn)))
                   "final state is X asserted (the chronologically newest write wins) -- NOT retracted, which is what a newest-first (incorrect) fold order would have produced"))))))))

#?(:clj
   (deftest push-after-take-oldest-refreshes-front-and-stays-chronologically-correct
        (testing "push (commit!) interleaved with a bounded take that exhausts
                  front and reverses back -- a fresh push after that reversal
                  must land in the (now-empty) back, and a later full read must
                  still return everything in correct chronological order"
          (let [{:keys [put! get-fn]} (mem-store)
                c0 (eng/commit! put! get-fn [{:s "a" :p "n" :o 1}] nil test-encrypt-fn)
                c1 (eng/commit! put! get-fn [{:s "b" :p "n" :o 2}] c0 test-encrypt-fn)
                c2 (eng/commit! put! get-fn [{:s "c" :p "n" :o 3}] c1 test-encrypt-fn)
                everything (constantly true)
                ;; bound=2 against 3 novelty entries: front (built at push-time,
                ;; all 3 in front since no fold happened yet) has exactly 3, so
                ;; take-oldest-novelty's ">= n" branch applies here, not the
                ;; front-exhaustion/reverse-back branch -- exercise THAT branch
                ;; by folding down to 1 remaining, then taking 1 more (draining
                ;; front to empty), THEN pushing again before reading everything.
                folded-1 (eng/fold! put! get-fn c2 ipld/link? 2 test-blind-fn test-encrypt-fn test-decrypt-fn)
                _ (is (= 1 (eng/novelty-size get-fn folded-1)))
                folded-2 (eng/fold! put! get-fn folded-1 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                _ (is (= 0 (eng/novelty-size get-fn folded-2)) "front now fully drained")
                c3 (eng/commit! put! get-fn [{:s "d" :p "n" :o 4}] folded-2 test-encrypt-fn)]
            (is (= 1 (eng/novelty-size get-fn c3)) "the post-drain push landed correctly")
            (is (= #{{:e "a" :a "n" :v_edn "\"1\"" :added true}
                     {:e "b" :a "n" :v_edn "\"2\"" :added true}
                     {:e "c" :a "n" :v_edn "\"3\"" :added true}
                     {:e "d" :a "n" :v_edn "\"4\"" :added true}}
                   (set (eng/hot-datoms get-fn c3 everything test-blind-fn test-decrypt-fn)))
                "hot-datoms after the post-drain push sees everything: a/b/c already folded (cold) PLUS d, the new novelty entry (hot), correctly merged")
            (let [final-folded (eng/fold! put! get-fn c3 test-blind-fn test-encrypt-fn test-decrypt-fn)]
              (is (= #{{:e "a" :a "n" :v_edn "\"1\"" :added true}
                       {:e "b" :a "n" :v_edn "\"2\"" :added true}
                       {:e "c" :a "n" :v_edn "\"3\"" :added true}
                       {:e "d" :a "n" :v_edn "\"4\"" :added true}}
                     (set (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn final-folded) nil everything
                                           test-blind-fn test-decrypt-fn)))
                  "everything pushed across the whole sequence is present after a final full fold"))))))

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
(deftest tx-map->datoms-single-entity-contract
  ;; entities->datoms (tested below) is a collection-flattening wrapper over
  ;; the same dc/eavt call -- this covers tx-map->datoms's own single-entity
  ;; contract directly, which had no direct test evidence.
  (testing "one entity tx-map -> its own [e a v] datoms, :db/id -> e"
    (is (= [["e1" :ns/a "v1"] ["e1" :ns/b "v2"]]
           (eng/tx-map->datoms {:db/id "e1" :ns/a "v1" :ns/b "v2"}))))
  (testing "an entity with only :db/id (no other attrs) -> no datoms"
    (is (= [] (eng/tx-map->datoms {:db/id "e1"}))))
  (testing "entities->datoms on a single-element seq == tx-map->datoms on that one entity"
    (let [ent {:db/id "e1" :ns/a "v1" :ns/b "v2"}]
      (is (= (eng/tx-map->datoms ent) (eng/entities->datoms [ent]))))))

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

(deftest datoms-vaet-index-reverse-reference-lookup
  ;; :vaet was previously an unhandled `case` branch with NO default -- any
  ;; caller actually sending it (kotobase-client's own `datoms` docstring
  ;; has always documented :vaet as a valid index) got an unhandled-case
  ;; exception instead of rows or a graceful error. Confirmed 2026-07-08,
  ;; fixed by wiring the already-existing `:ocp` index (arrangement's own
  ;; VAET-equivalent, populated for ref-valued quads -- see `refs`/`refs-to`).
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "knows" :o bob-link}
                           {:s "carol" :p "knows" :o bob-link}
                           {:s "alice" :p "name" :o "Alice"}])
        everything (constantly true)]
    (testing "no components -> full VAET scan, only ref-valued quads show up"
      (let [rows (eng/datoms db {:index :vaet} everything)]
        (is (= 2 (count rows)))
        (is (every? #(= "knows" (:a %)) rows))
        (is (= #{"alice" "carol"} (set (map :e rows))))))
    (testing "[value] narrows to that value's reverse refs"
      (is (= 2 (count (eng/datoms db {:index :vaet :components [bob-link]} everything)))))
    (testing "[value attr] point-looks-up the exact entities"
      (let [rows (eng/datoms db {:index :vaet :components [bob-link "knows"]} everything)]
        (is (= #{"alice" "carol"} (set (map :e rows))))
        (is (every? #(= "knows" (:a %)) rows))))
    (testing "a value never asserted as a ref has no VAET entries (a plain string, e.g., is never mistaken for a ref -- same convention `refs` already documents)"
      (is (= [] (eng/datoms db {:index :vaet :components ["Alice"]} everything))))))

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
         (testing "cold-datoms :vaet finds the reverse reference too -- ocp IS
                   persisted in every snapshot's index-roots already (arrangement's
                   own commit! writes all 4 roots unconditionally), this index-spec
                   entry was simply never added until 2026-07-08"
           (let [row (first (eng/cold-datoms get-fn snap {:index :vaet :components [bob-link]} (constantly true)
                                              test-blind-fn test-decrypt-fn))]
             (is (= "alice" (:e row)))
             (is (= "knows" (:a row)))))
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
   (deftest commit-serialized-effective-prunes-persisted-no-ops
     (async done
       (let [{:keys [put! get-fn store]} (mem-store)
             heads (atom {})
             cas-calls (atom 0)
             cas! (fn [head-key expected new]
                    (swap! cas-calls inc)
                    (if (= (get @heads head-key) expected)
                      (do (swap! heads assoc head-key new) new)
                      (get @heads head-key)))]
         (-> (eng/commit-serialized-effective!
              put! get-fn cas! "actor:alice" nil
              [[:db/add "alice" "role" "admin"]]
              test-encrypt-fn test-blind-fn test-decrypt-fn)
             (.then (fn [initial]
                      (let [blocks-before (count @store)
                            calls-before @cas-calls]
                        (-> (eng/commit-serialized-effective!
                             put! get-fn cas! "actor:alice" (:chain-cid-after initial)
                             [[:db/add "alice" "role" "admin"]
                              [:db/retract "alice" "missing" "value"]]
                             test-encrypt-fn test-blind-fn test-decrypt-fn)
                            (.then (fn [report]
                                     (is (false? (:committed? report)))
                                     (is (= (:chain-cid-before report)
                                            (:chain-cid-after report)))
                                     (is (= blocks-before (count @store)))
                                     (is (= calls-before @cas-calls))
                                     (done))))))))))))

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
                      (is (thrown? js/Error #_:clj-kondo/ignore (eng/hot-datoms get-fn c0))
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
   (deftest hot-datoms-and-fold-bang-correct-past-the-pmap-async-batch-size
     ;; ADR-2607110200 addendum: pmap-async batches novelty tx-block gets
     ;; (pmap-async-batch-size, currently 24) instead of firing them all via
     ;; one unbounded js/Promise.all -- a graph with hundreds of unfolded
     ;; novelty blocks was overrunning Workers' concurrent-subrequest ceiling
     ;; and blowing the CPU/wall-time budget (yoro-social-v2, 2026-07-10,
     ;; ~300 create-record calls none of which got folded in time). This
     ;; test commits MORE writes than one batch (30 > 24) so both
     ;; hot-datoms's and fold!'s novelty reads span at least two batches,
     ;; and asserts correctness (no dropped/duplicated/reordered rows) is
     ;; preserved across that batch boundary -- the actual regression this
     ;; guards against isn't "does batching happen" (opaque from outside)
     ;; but "does batching silently corrupt results at scale."
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)
             n 30
             commit-n! (fn commit-n! [i chain-cid]
                         (if (>= i n)
                           (js/Promise.resolve chain-cid)
                           (-> (eng/commit! put! get-fn
                                            [{:s (str "actor-" i) :p "role" :o "member"}]
                                            chain-cid test-encrypt-fn)
                               (.then (fn [next-cid] (commit-n! (inc i) next-cid))))))]
         (-> (commit-n! 0 nil)
             (.then (fn [c-final]
                      (is (= n (eng/novelty-size get-fn c-final))
                          "all n writes landed as novelty, none folded yet")
                      (-> (eng/hot-datoms get-fn c-final everything test-blind-fn test-decrypt-fn)
                          (.then (fn [before-rows]
                                   (is (= n (count before-rows))
                                       "hot-datoms sees all n rows across the batch boundary, none dropped/duplicated")
                                   (is (= n (count (set before-rows)))
                                       "no duplicate rows introduced by batching")
                                   (-> (eng/fold! put! get-fn c-final test-blind-fn test-encrypt-fn test-decrypt-fn)
                                       (.then (fn [folded]
                                                (is (= 0 (eng/novelty-size get-fn folded))
                                                    "fold across >1 batch still fully compacts novelty")
                                                (-> (eng/hot-datoms get-fn folded everything test-blind-fn test-decrypt-fn)
                                                    (.then (fn [after-rows]
                                                             (is (= (set before-rows) (set after-rows))
                                                                 "folding a >1-batch novelty tail loses/duplicates nothing")
                                                             (done)))))))))))))))))

#?(:cljs
   (deftest fold-bang-max-novelty-folds-a-bounded-prefix-and-leaves-the-rest
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "actor-0" :p "role" :o "member"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "actor-1" :p "role" :o "member"}] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/commit! put! get-fn [{:s "actor-2" :p "role" :o "member"}] c1 test-encrypt-fn)))
             (.then (fn [c2] (eng/commit! put! get-fn [{:s "actor-3" :p "role" :o "member"}] c2 test-encrypt-fn)))
             (.then (fn [c3]
                      (-> (eng/hot-datoms get-fn c3 everything test-blind-fn test-decrypt-fn)
                          (.then (fn [before-rows]
                                   (let [before (set before-rows)]
                                     (-> (eng/fold! put! get-fn c3 ipld/link? 2 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                         (.then (fn [folded]
                                                  (is (= 2 (eng/novelty-size get-fn folded))
                                                      "max-novelty=2 against 4 novelty entries folds only the oldest 2, leaves 2")
                                                  (is (some? (eng/latest-snapshot-cid get-fn folded)))
                                                  (-> (js/Promise.all
                                                       #js [(eng/hot-datoms get-fn folded everything test-blind-fn test-decrypt-fn)
                                                            (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                                                             test-blind-fn test-decrypt-fn)])
                                                      (.then (fn [results]
                                                               (let [[after-rows cold-rows] (vec results)]
                                                                 (is (= before (set after-rows))
                                                                     "hot-datoms still sees everything -- unfolded entries still merged in")
                                                                 (is (= #{{:e "actor-0" :a "role" :v_edn "\"member\"" :added true}
                                                                          {:e "actor-1" :a "role" :v_edn "\"member\"" :added true}}
                                                                        (set cold-rows))
                                                                     "only the 2 oldest actually got indexed; actor-2/actor-3 aren't cold yet")
                                                                 (-> (eng/fold! put! get-fn folded ipld/link? 2 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                                                     (.then (fn [folded-again]
                                                                              (is (= 0 (eng/novelty-size get-fn folded-again))
                                                                                  "a second bounded fold with the same budget clears the remaining tail")
                                                                              (-> (js/Promise.all
                                                                                   #js [(eng/hot-datoms get-fn folded-again everything test-blind-fn test-decrypt-fn)
                                                                                        (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded-again) nil everything
                                                                                                         test-blind-fn test-decrypt-fn)])
                                                                                  (.then (fn [results2]
                                                                                           (let [[after-rows2 cold-rows2] (vec results2)]
                                                                                             (is (= before (set after-rows2))
                                                                                                 "two bounded folds together lose/duplicate nothing vs. one unbounded fold")
                                                                                             (is (= before (set cold-rows2))
                                                                                                 "after both bounded folds, everything is indexed")
                                                                                             (done))))))))))))))))))))))))))

#?(:cljs
   (deftest fold-bang-max-novelty-nil-is-unbounded-exactly-like-the-default-arity
     ;; regression: explicit nil max-novelty (the 8-arity form) must behave
     ;; identically to the pre-existing 6-/7-arity unbounded calls.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn test-decrypt-fn)))
             (.then (fn [folded]
                      (is (= 0 (eng/novelty-size get-fn folded)) "nil max-novelty still fully compacts")
                      (done))))))))

;; ── ADR-2607120730 Part 1: memoized hydration (fold!'s cache-get/cache-put!) ──

#?(:cljs
   (deftest fold-bang-cache-get-cache-put-avoids-rehydrating-on-a-retry-against-the-same-snapshot
     ;; ADR-2607120730 Part 1 (memoized hydration): a fold attempt that
     ;; hydrates the same still-current indexed snapshot twice (e.g. two cron
     ;; retries against a chain nothing new has folded into since) should pay
     ;; the cold-datoms decrypt-and-scan cost only ONCE -- the second attempt
     ;; hits the cache instead of re-decrypting every cold entry.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             decrypt-calls (atom 0)
             counting-decrypt-fn (fn [blob] (swap! decrypt-calls inc) (test-decrypt-fn blob))
             cache (atom {})
             cache-get (fn [k] (js/Promise.resolve (get @cache k)))
             cache-put! (fn [k v] (js/Promise.resolve (swap! cache assoc k v)))]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/fold! put! get-fn c0 test-blind-fn test-encrypt-fn counting-decrypt-fn)))
             (.then (fn [c1]
                      (is (= 0 (eng/novelty-size get-fn c1)) "nothing pending after the warm-up fold")
                      (reset! decrypt-calls 0)
                      (-> (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn counting-decrypt-fn
                                     cache-get cache-put!)
                          (.then (fn [attempt1]
                                   (let [attempt1-calls @decrypt-calls]
                                     (is (pos? attempt1-calls) "attempt 1 (cache empty) actually decrypts the cold entry")
                                     (is (contains? @cache (eng/hydrate-cache-key (eng/latest-snapshot-cid get-fn c1)))
                                         "cache-put! populated the cache under the expected key")
                                     (reset! decrypt-calls 0)
                                     (-> (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn counting-decrypt-fn
                                                    cache-get cache-put!)
                                         (.then (fn [attempt2]
                                                  (is (zero? @decrypt-calls)
                                                      "attempt 2 (cache hit) decrypts nothing -- the cold scan was skipped entirely")
                                                  (is (= (eng/latest-snapshot-cid get-fn attempt1)
                                                         (eng/latest-snapshot-cid get-fn attempt2))
                                                      "cached and uncached hydration fold to the identical (content-addressed) snapshot CID")
                                                  (done)))))))))))))))

#?(:cljs
   (deftest fold-bang-no-cache-args-behaves-exactly-like-before
     ;; backward compat: the pre-existing 8-arity call (no cache-get/cache-put!)
     ;; must still work identically -- fold! delegates it to the new 10-arity
     ;; impl with cache-get=nil cache-put!=nil, which hydrate-db-cached treats
     ;; as "caching disabled", the same as calling plain hydrate-db.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/fold! put! get-fn c0 ipld/link? nil test-blind-fn test-encrypt-fn test-decrypt-fn)))
             (.then (fn [folded]
                      (is (= 0 (eng/novelty-size get-fn folded)))
                      (is (some? (eng/latest-snapshot-cid get-fn folded)))
                      (done))))))))

#?(:cljs
   (deftest fold-bang-cache-hit-preserves-link-values-not-just-plain-scalars
     ;; the cache serializes rows through the SAME v->edn/edn->link round trip
     ;; :v_edn already uses (not a raw pr-str of the whole row, which would
     ;; shred a Link the way v->edn's own docstring / ADR-2607051000's
     ;; follow-up already document for :v elsewhere in this codebase) --
     ;; specifically exercises a CACHE-HIT hydrate (not just a fresh one) to
     ;; prove that round trip is correct on the read-back path too.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             cache (atom {})
             cache-get (fn [k] (js/Promise.resolve (get @cache k)))
             cache-put! (fn [k v] (js/Promise.resolve (swap! cache assoc k v)))]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "knows" :o bob-link}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/fold! put! get-fn c0 test-blind-fn test-encrypt-fn test-decrypt-fn)))
             (.then (fn [c1]
                      ;; attempt 1: cache miss, populates the cache
                      (-> (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn test-decrypt-fn
                                     cache-get cache-put!)
                          (.then (fn [_attempt1]
                                   ;; attempt 2: cache HIT -- this is the path under test
                                   (-> (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn test-decrypt-fn
                                                  cache-get cache-put!)
                                       (.then (fn [attempt2]
                                                (-> (eng/hydrate-db get-fn (eng/latest-snapshot-cid get-fn attempt2)
                                                                    test-blind-fn test-decrypt-fn)
                                                    (.then (fn [db]
                                                             (is (= {"knows" #{"alice"}} (qs/refs-to db bob-link))
                                                                 "a cache-hit hydration still reconstructs a real Link, not a shredded seq")
                                                             (done)))))))))))))))))

#?(:cljs
   (deftest fold-bang-async-get-fn-produces-identical-results-to-the-sync-path
     ;; ADR-2607120730 follow-up: fold!'s optional async-get-fn (routed to
     ;; cold-datoms-async/scan-prefix-async instead of cold-datoms/scan-prefix)
     ;; is a pure performance path -- confirmed live against the real stuck
     ;; yoro-social-v2 backlog (5130 leaf entries: 806ms via scan-prefix-async
     ;; vs. a 300s CPU budget exceeded via the sync with-blocks-trampolined
     ;; path). This proves it produces the IDENTICAL folded result as the
     ;; sync path, including a Link-valued datom (not just plain scalars).
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             async-get-fn (fn [cid] (js/Promise.resolve (get-fn cid)))
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "alice" :p "knows" :o bob-link}] c0 test-encrypt-fn)))
             (.then (fn [c1]
                      (-> (eng/hot-datoms get-fn c1 everything test-blind-fn test-decrypt-fn)
                          (.then (fn [before-rows]
                                   (let [before (set before-rows)]
                                     (-> (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn test-decrypt-fn
                                                    nil nil async-get-fn)
                                         (.then (fn [folded]
                                                  (is (= 0 (eng/novelty-size get-fn folded))
                                                      "fold via async-get-fn still fully compacts")
                                                  (-> (js/Promise.all
                                                       #js [(eng/hot-datoms get-fn folded everything test-blind-fn test-decrypt-fn)
                                                            (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                                                             test-blind-fn test-decrypt-fn)
                                                            (eng/hydrate-db get-fn (eng/latest-snapshot-cid get-fn folded)
                                                                            test-blind-fn test-decrypt-fn)])
                                                      (.then (fn [results]
                                                               (let [[after-rows cold-rows db] (vec results)]
                                                                 (is (= before (set after-rows))
                                                                     "folding via the async-get-fn path loses/duplicates nothing")
                                                                 (is (= before (set cold-rows))
                                                                     "the async cold scan really did index the data")
                                                                 (is (= {"knows" #{"alice"}} (qs/refs-to db bob-link))
                                                                     "Link values survive the async cold-scan path too")
                                                                 (done)))))))))))))))))))

#?(:cljs
   (deftest hot-datoms-async-get-fn-produces-identical-results-to-the-sync-path
     ;; kotoba-lang/kotobase-peer#21: hot-datoms is THE regular per-request
     ;; read path (do-q/do-pull/any store bridge's hydrate!) -- unlike
     ;; fold!, it had no async-get-fn arity at all, so every read paid the
     ;; sync with-blocks trampoline's O(N^2) block-discovery cost on its
     ;; cold-snapshot half regardless of graph size. Covers BOTH halves of
     ;; hot-datoms's hot/cold split at once: a folded snapshot (cold half,
     ;; containing a Link-valued datom, routed through cold-datoms-async)
     ;; with MORE unfolded novelty (a plain scalar, routed through
     ;; read-tx-block-async) committed on top afterward -- proving neither
     ;; half silently drops or reshapes data through the new async path,
     ;; not just that the two halves' ROW COUNTS happen to match."
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             async-get-fn (fn [cid] (js/Promise.resolve (get-fn cid)))
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "alice" :p "knows" :o bob-link}] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)))
             (.then (fn [folded] (eng/commit! put! get-fn [{:s "dave" :p "role" :o "guest"}] folded test-encrypt-fn)))
             (.then (fn [c2]
                      (is (pos? (eng/novelty-size get-fn c2))
                          "test setup: c2 really does have a folded cold half PLUS unfolded novelty")
                      (-> (js/Promise.all
                           #js [(eng/hot-datoms get-fn c2 everything test-blind-fn test-decrypt-fn)
                                (eng/hot-datoms get-fn c2 nil everything test-blind-fn test-decrypt-fn async-get-fn)])
                          (.then (fn [results]
                                   (let [[sync-rows async-rows] (vec results)
                                         wire (fn [rows e a] (some #(when (and (= e (:e %)) (= a (:a %))) (:v_edn %)) rows))]
                                     (is (= 3 (count sync-rows) (count async-rows))
                                         "test setup: alice/role (cold), alice/knows (cold, Link), dave/role (novelty)")
                                     (is (= (set sync-rows) (set async-rows))
                                         "async-get-fn path returns byte-identical rows to the sync path, cold half + novelty half both included")
                                     (is (= "[\"ipld/link\" \"bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m\"]"
                                            (wire async-rows "alice" "knows"))
                                         "the folded (cold-half) Link's wire encoding survives cold-datoms-async intact, not shredded")
                                     (is (= "\"guest\"" (wire async-rows "dave" "role"))
                                         "the unfolded (novelty-half) scalar survives read-tx-block-async intact")
                                     (done))))))))))))

;; ── kotoba-lang/kotobase-peer#16: front/back persistent-queue novelty ──────

#?(:cljs
   (deftest legacy-flat-vector-novelty-state-reads-and-migrates-correctly
     ;; see the :clj version's docstring -- same scenario, promise-chained.
     ;; cd/head, cd/commit!, and every new push-novelty!/take-oldest-novelty
     ;; helper are synchronous on cljs too (no crypto involved, only cid/
     ;; link plumbing) -- only commit!/fold! themselves need .then.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)
             walk-chain (fn walk-chain [head-link]
                          (loop [cid (some-> head-link ipld/link-cid) acc []]
                            (if (nil? cid)
                              acc
                              (let [{:strs [e rest]} (ipld/get-node get-fn cid)]
                                (recur (some-> rest ipld/link-cid) (conj acc e))))))]
         (-> (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] c0 test-encrypt-fn)))
             (.then (fn [c1]
                      (let [{:keys [state]} (cd/head get-fn c1)
                            tx-links (into (walk-chain (get state "novelty-front"))
                                           (rseq (walk-chain (get state "novelty-back"))))
                            legacy-state {"indexed" nil "novelty" tx-links}
                            legacy-chain-cid (cd/commit! put! get-fn legacy-state nil)]
                        (is (= 2 (eng/novelty-size get-fn legacy-chain-cid)) "reads work on the untouched legacy chain")
                        (-> (eng/hot-datoms get-fn legacy-chain-cid everything test-blind-fn test-decrypt-fn)
                            (.then (fn [rows]
                                     (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                                              {:e "bob" :a "role" :v_edn "\"user\"" :added true}}
                                            (set rows)))
                                     (-> (eng/commit! put! get-fn [{:s "carol" :p "role" :o "guest"}] legacy-chain-cid test-encrypt-fn)
                                         (.then (fn [migrated]
                                                  (is (= 3 (eng/novelty-size get-fn migrated))
                                                      "migration preserves the 2 legacy entries + the 1 new one")
                                                  (-> (eng/hot-datoms get-fn migrated everything test-blind-fn test-decrypt-fn)
                                                      (.then (fn [rows2]
                                                               (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                                                                        {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                                                                        {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
                                                                      (set rows2)))
                                                               (-> (eng/fold! put! get-fn migrated test-blind-fn test-encrypt-fn test-decrypt-fn)
                                                                   (.then (fn [folded]
                                                                            (is (= 0 (eng/novelty-size get-fn folded)))
                                                                            (-> (eng/cold-datoms get-fn (eng/latest-snapshot-cid get-fn folded) nil everything
                                                                                                 test-blind-fn test-decrypt-fn)
                                                                                (.then (fn [cold-rows]
                                                                                         (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                                                                                                  {:e "bob" :a "role" :v_edn "\"user\"" :added true}
                                                                                                  {:e "carol" :a "role" :v_edn "\"guest\"" :added true}}
                                                                                                (set cold-rows))
                                                                                             "folding the migrated chain indexes everything, in the original chronological order")
                                                                                         (done))))))))))))))))))))))))

#?(:cljs
   (deftest bounded-fold-across-front-exhaustion-preserves-chronological-retraction-order
     ;; see the :clj version's docstring -- same counterexample scenario.
     (async done
       (let [{:keys [put! get-fn]} (mem-store)
             everything (constantly true)]
         (-> (eng/commit! put! get-fn [{:s "x" :p "flag" :o "on"}] nil test-encrypt-fn)
             (.then (fn [c0] (eng/commit! put! get-fn [[:db/retract "x" "flag" "on"]] c0 test-encrypt-fn)))
             (.then (fn [c1] (eng/commit! put! get-fn [{:s "x" :p "flag" :o "on"}] c1 test-encrypt-fn)))
             (.then (fn [c2]
                      (is (= 3 (eng/novelty-size get-fn c2)))
                      (-> (eng/fold! put! get-fn c2 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                          (.then (fn [folded-1]
                                   (is (= 2 (eng/novelty-size get-fn folded-1)))
                                   (-> (eng/fold! put! get-fn folded-1 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                       (.then (fn [folded-2]
                                                (is (= 1 (eng/novelty-size get-fn folded-2)))
                                                (-> (eng/fold! put! get-fn folded-2 ipld/link? 1 test-blind-fn test-encrypt-fn test-decrypt-fn)
                                                    (.then (fn [folded-3]
                                                             (is (= 0 (eng/novelty-size get-fn folded-3)))
                                                             (-> (eng/hot-datoms get-fn folded-3 everything test-blind-fn test-decrypt-fn)
                                                                 (.then (fn [rows]
                                                                          (is (= #{{:e "x" :a "flag" :v_edn "\"on\"" :added true}}
                                                                                 (set rows))
                                                                              "final state is X asserted (newest write wins) -- NOT retracted")
                                                                          (done))))))))))))))))))))

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
                                               (eng/cold-datoms get-fn snap nil (constantly true) test-blind-fn test-decrypt-fn)
                                               (eng/cold-datoms get-fn snap {:index :vaet :components [bob-link]} (constantly true)
                                                                 test-blind-fn test-decrypt-fn)])
                                         (.then (fn [results]
                                                  (let [[cold-rows db hot-rows cold-rows-2 vaet-rows] (vec results)
                                                        row (first cold-rows)
                                                        vaet-row (first vaet-rows)]
                                                    (testing "cold-datoms reconstructs the Link (not the raw edn-safe vector)"
                                                      (is (= "[\"ipld/link\" \"bafyreiaakutsdtndrl7e7emcmkp5hjsaaq2vu6prfelbgaglprvtdon63m\"]"
                                                             (:v_edn row))))
                                                    (testing "cold-datoms :vaet finds the reverse reference too -- ocp IS
                                                              persisted in every snapshot's index-roots already, this
                                                              index-spec entry was simply never added until 2026-07-08"
                                                      (is (= "alice" (:e vaet-row)))
                                                      (is (= "knows" (:a vaet-row))))
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

(deftest transact-effective-emits-only-state-transitions
  (let [db (eng/transact (eng/empty-db)
                         [["e1" "role" "admin"] ["e1" "name" "Alice"]])
        result (eng/transact-effective
                db
                [[:db/add "e1" "role" "admin"]
                 [:db/retract "missing" "role" "admin"]
                 [:db/retract "e1" "role" "admin"]
                 [:db/retract "e1" "role" "admin"]
                 [:db/add "e1" "role" "user"]])]
    (is (= [{:e "e1" :a "role" :v "admin" :op :retract}
            {:e "e1" :a "role" :v "user" :op :assert}]
           (:effective-deltas result)))
    (is (= #{"user"} (get (qs/entity-attrs (:db-after result) "e1") "role")))))

(deftest transact-effective-expands-entity-retraction-deterministically
  (let [db (eng/transact (eng/empty-db)
                         [["e1" "role" "admin"] ["e1" "name" "Alice"]])
        result (eng/transact-effective db [[:db/retractEntity "e1"]])]
    (is (= [{:e "e1" :a "name" :v "Alice" :op :retract}
            {:e "e1" :a "role" :v "admin" :op :retract}]
           (:effective-deltas result)))
    (is (empty? (qs/entity-attrs (:db-after result) "e1")))))

(deftest transact-with-statistics-refreshes-from-effective-deltas
  (let [db (eng/transact (eng/empty-db) [["e1" "role" "admin"]])
        statistics {:visibility-scope "tenant-a/public-v1" :epoch 4
                    :clauses [{:pattern [nil "role" nil] :rows 1}
                              {:pattern [nil "role" "admin"] :rows 1}]}
        result (eng/transact-with-statistics
                db
                [[:db/add "e1" "role" "admin"]
                 [:db/retract "missing" "role" "admin"]
                 [:db/retract "e1" "role" "admin"]
                 [:db/add "e2" "role" "user"]]
                statistics 5)]
    (is (= 5 (get-in result [:query-statistics :epoch])))
    (is (= [1 0] (mapv :rows (get-in result [:query-statistics :clauses]))))
    (is (= 2 (count (:effective-deltas result))))))

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

;; ── ADR-2607071610 Phase 2: as-of / :added false surfacing ──────────────────
;; `history` (the pre-existing hot-db-returning fn, `commit-serialized!`'s
;; neighbor above) documented "a datom retracted later still appears here"
;; from the start, but that was NOT actually true until this pass -- fixed
;; below (see `kotobase-peer.core/audit-replay`'s docstring for the confirmed
;; bug). `history-datoms` is new: the Datomic-`d/datoms`-shaped `:added`
;; EVENT LOG `history`'s own docstring named as "Phase 2 of that ADR."

#?(:clj
   (deftest history-now-actually-preserves-a-retracted-fact
     ;; The confirmed bug this pass fixes: before, `(eng/pull (eng/history ...)
     ;; "post/1")` returned `{}` for a fact that was asserted then retracted --
     ;; contradicting `history`'s own docstring. Now it doesn't.
     (let [{:keys [put! get-fn]} (mem-store)
           c1 (eng/commit! put! get-fn [["post/1" ":yoro.post/text" "hello"]] nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [[:db/retract "post/1" ":yoro.post/text" "hello"]]
                           c1 test-encrypt-fn)
           h (eng/history get-fn c2 test-blind-fn test-decrypt-fn)]
       (is (= #{"hello"} (get (eng/pull h "post/1") ":yoro.post/text"))
           "the retracted fact is STILL visible in the audit db"))))

#?(:clj
   (deftest history-datoms-shows-both-the-assert-and-the-retract
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn [["post/1" ":yoro.post/text" "hello"]] nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [[:db/retract "post/1" ":yoro.post/text" "hello"]]
                           c1 test-encrypt-fn)
           rows (eng/history-datoms get-fn c2 everything test-decrypt-fn)]
       (testing "current-state hot-datoms drops the retracted fact entirely (Phase 1 behavior, unchanged)"
         (is (empty? (eng/hot-datoms get-fn c2 everything test-blind-fn test-decrypt-fn))))
       (testing "history-datoms shows BOTH the original assert and the later retract, in order"
         (is (= [{:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added true}
                 {:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added false}]
                (vec rows)))))))

#?(:clj
   (deftest history-datoms-survives-a-fold-even-though-hot-datoms-does-not
     ;; The whole point of Phase 2: fold! physically removes a retracted fact
     ;; from the indexed snapshot (real GC, Phase 1) -- but the ORIGINAL tx
     ;; block recording the assert, and the one recording the retract, are
     ;; never deleted (content-addressed). history-datoms must still see
     ;; both after folding, by replaying every commit's own novelty from
     ;; genesis.
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn [["post/1" ":yoro.post/text" "hello"]] nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [[:db/retract "post/1" ":yoro.post/text" "hello"]]
                           c1 test-encrypt-fn)
           c3 (eng/fold! put! get-fn c2 test-blind-fn test-encrypt-fn test-decrypt-fn)]
       (testing "post-fold hot-datoms shows nothing for post/1 (Phase 1 GC)"
         (is (empty? (eng/hot-datoms get-fn c3 everything test-blind-fn test-decrypt-fn))))
       (testing "post-fold history-datoms STILL shows both events"
         (is (= [{:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added true}
                 {:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added false}]
                (vec (eng/history-datoms get-fn c3 everything test-decrypt-fn))))))))

#?(:clj
   (deftest history-datoms-retract-entity-expands-into-one-row-per-attribute
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn
                           [["post/1" ":yoro.post/text" "hello"]
                            ["post/1" ":yoro.post/author" "did:key:zA"]]
                           nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [[:db/retractEntity "post/1"]] c1 test-encrypt-fn)
           rows (eng/history-datoms get-fn c2 everything test-decrypt-fn)]
       (is (= #{{:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added true}
                {:e "post/1" :a ":yoro.post/author" :v_edn "\"did:key:zA\"" :added true}
                {:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added false}
                {:e "post/1" :a ":yoro.post/author" :v_edn "\"did:key:zA\"" :added false}}
              (set rows))))))

#?(:clj
   (deftest history-datoms-entity-filter-narrows-rows-but-not-correctness
     ;; retract-entity on post/1 must correctly consume post/1's OWN prior
     ;; state regardless of whether post/2's rows are being filtered out --
     ;; this proves the full replay runs unconditionally and only the
     ;; EMITTED rows are narrowed by the entity filter.
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn
                           [["post/1" ":yoro.post/text" "hello"]
                            ["post/2" ":yoro.post/text" "keep"]]
                           nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [[:db/retractEntity "post/1"]] c1 test-encrypt-fn)]
       (is (= #{{:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added true}
                {:e "post/1" :a ":yoro.post/text" :v_edn "\"hello\"" :added false}}
              (set (eng/history-datoms get-fn c2 "post/1" everything test-decrypt-fn))))
       (is (= [{:e "post/2" :a ":yoro.post/text" :v_edn "\"keep\"" :added true}]
              (vec (eng/history-datoms get-fn c2 "post/2" everything test-decrypt-fn)))
           "post/2 was only ever asserted, never retracted -- its own single event, correctly narrowed"))))

#?(:clj
   (deftest history-datoms-across-multiple-folds-neither-duplicates-nor-drops-events
     ;; A commit's own state["novelty"] is the CUMULATIVE not-yet-folded list,
     ;; not a per-commit delta -- newly-added-tx-cids already walks the delta
     ;; (new tx-cids since the previous commit), so history-datoms must not
     ;; re-replay the same tx block once per intervening commit before a fold.
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c1 (eng/commit! put! get-fn [["e" ":a/x" "v1"]] nil test-encrypt-fn)
           c2 (eng/commit! put! get-fn [["e" ":a/y" "v2"]] c1 test-encrypt-fn)
           cf (eng/fold! put! get-fn c2 test-blind-fn test-encrypt-fn test-decrypt-fn)
           c3 (eng/commit! put! get-fn [[:db/retract "e" ":a/x" "v1"]] cf test-encrypt-fn)
           c4 (eng/commit! put! get-fn [["e" ":a/z" "v3"]] c3 test-encrypt-fn)
           cf2 (eng/fold! put! get-fn c4 test-blind-fn test-encrypt-fn test-decrypt-fn)
           rows (eng/history-datoms get-fn cf2 everything test-decrypt-fn)]
       (is (= [{:e "e" :a ":a/x" :v_edn "\"v1\"" :added true}
               {:e "e" :a ":a/y" :v_edn "\"v2\"" :added true}
               {:e "e" :a ":a/x" :v_edn "\"v1\"" :added false}
               {:e "e" :a ":a/z" :v_edn "\"v3\"" :added true}]
              (vec rows))
           "every event exactly once, in commit order, across both folds")
       (testing "current-state hot-datoms agrees with history-datoms' net effect"
         (is (= #{["e" ":a/y"] ["e" ":a/z"]}
                (set (map (juxt :e :a) (eng/hot-datoms get-fn cf2 everything test-blind-fn test-decrypt-fn)))))))))

#?(:clj
   (deftest history-datoms-nil-chain-cid-is-empty
     (is (= [] (eng/history-datoms nil nil (constantly true) test-decrypt-fn)))))

;; ── materialized views (ADR-2607166600 IVM) ──────────────────────────────────

#?(:clj
   (deftest fold-bang-materializes-views-and-view-rows-stays-fresh
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}
                                        {:s "alice" :p "name" :o "Alice"}] nil test-encrypt-fn)
           folded (eng/fold! put! get-fn c0 ipld/link? nil test-blind-fn test-encrypt-fn
                             test-decrypt-fn nil nil nil {"roles" {"attrs" ["role"]}})
           v (eng/view-rows get-fn folded "roles" everything test-decrypt-fn)]
       (is (= {"attrs" ["role"]} (:spec v)))
       (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}}
              (set (:rows v)))
           "only spec attrs are materialized; the name row is excluded")
       (is (nil? (eng/view-rows get-fn folded "nope" everything test-decrypt-fn))
           "unknown view -> nil")
       ;; unfolded novelty is merged fresh on read, no new fold needed
       (let [c1 (eng/commit! put! get-fn [{:s "bob" :p "role" :o "user"}] folded test-encrypt-fn)
             v1 (eng/view-rows get-fn c1 "roles" everything test-decrypt-fn)]
         (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                  {:e "bob" :a "role" :v_edn "\"user\"" :added true}}
                (set (:rows v1)))
             "a novelty assertion matching the spec appears without a fold")
         ;; carry-forward: fold WITHOUT a views param keeps materializing the view
         (let [folded2 (eng/fold! put! get-fn c1 test-blind-fn test-encrypt-fn test-decrypt-fn)
               v2 (eng/view-rows get-fn folded2 "roles" everything test-decrypt-fn)]
           (is (= #{{:e "alice" :a "role" :v_edn "\"admin\"" :added true}
                    {:e "bob" :a "role" :v_edn "\"user\"" :added true}}
                  (set (:rows v2)))
               "a spec-less fold carries the stored view spec forward and re-materializes"))))))

#?(:clj
   (deftest view-rows-cancels-novelty-retractions-and-transact-preserves-views
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "alice" :p "role" :o "admin"}] nil test-encrypt-fn)
           folded (eng/fold! put! get-fn c0 ipld/link? nil test-blind-fn test-encrypt-fn
                             test-decrypt-fn nil nil nil {"roles" {"attrs" ["role"]}})
           ;; retract the materialized row via post-fold novelty
           c1 (eng/commit! put! get-fn [[:db/retract "alice" "role" "admin"]] folded test-encrypt-fn)
           v (eng/view-rows get-fn c1 "roles" everything test-decrypt-fn)]
       (is (= #{} (set (filter :added (:rows v))))
           "an unfolded retraction cancels the stored view row on read")
       ;; a plain transact (commit!) must not drop the "views" link from state
       (let [c2 (eng/commit! put! get-fn [{:s "carol" :p "name" :o "Carol"}] c1 test-encrypt-fn)
             v2 (eng/view-rows get-fn c2 "roles" everything test-decrypt-fn)]
         (is (some? v2) "commit! carries the views link forward")))))

#?(:clj
   (deftest view-removal-via-nil-spec
     (let [{:keys [put! get-fn]} (mem-store)
           everything (constantly true)
           c0 (eng/commit! put! get-fn [{:s "a" :p "x" :o "1"}] nil test-encrypt-fn)
           f1 (eng/fold! put! get-fn c0 ipld/link? nil test-blind-fn test-encrypt-fn
                         test-decrypt-fn nil nil nil {"xs" {"attrs" ["x"]}})
           c1 (eng/commit! put! get-fn [{:s "b" :p "x" :o "2"}] f1 test-encrypt-fn)
           f2 (eng/fold! put! get-fn c1 ipld/link? nil test-blind-fn test-encrypt-fn
                         test-decrypt-fn nil nil nil {"xs" nil})]
       (is (some? (eng/view-rows get-fn f1 "xs" everything test-decrypt-fn)))
       (is (nil? (eng/view-rows get-fn f2 "xs" everything test-decrypt-fn))
           "an explicit nil spec removes the view at the next fold"))))
