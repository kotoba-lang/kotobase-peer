(ns kotobase-peer.availability
  "Lightweight, salted-hash proof-of-retrievability for pinned blocks — a
   periodic challenge/response BETWEEN REPLICA-HOLDING PEERS, not a
   trustless prover-only scheme (see HONEST SCOPE below). Redesigned in
   cljc from the pre-Rust-deletion kotoba-dht/availability_proof.rs
   sketch (legacy ADR-2605252200 §5.2, 'lightweight PoSt'); reuses this
   codebase's own portable SHA-256 (multiformats.core/sha256, real
   JVM+cljs parity) instead of introducing blake3. See
   90-docs/adr/2607051410-net-kotobase-distributed-storage-mesh-design.md
   (com-junkawasaki/root) for the design this implements.

   HONEST SCOPE: this is NOT Filecoin's PoRep/PoSt. A verifier holding
   only a CID (a hash) cannot check hash(bytes ++ nonce) without already
   possessing bytes — hash functions don't let you derive H(x||y) from
   H(x) alone. So `verify` below requires the VERIFIER to independently
   resolve the same block through its own get-fn (another replica-
   holding peer, the tenant's own kept copy, or a rotating auditor from
   an L3 provider-list) — this proves ongoing possession/freshness
   between two parties who both once had the data, at the cost of one
   compact hash instead of re-transferring the whole block every epoch.
   Justified because kekkai gates mesh membership to known identities
   (not an anonymous adversarial market) and durable pins already
   require replication >= 2 for plain durability — so a verifying
   replica-holder already exists at the tiers this audit is offered on
   (see redundancy-tiers)."
  (:require [multiformats.core :as mf]))

;; Mirrors legacy ADR-2605252200 §5.1 pricing — only tiers with >=1
;; OTHER replica holder can run this audit at all.
(def redundancy-tiers
  {:volatile {:replicas 1 :availability-proof? false}   ; base price, unauditable
   :standard {:replicas 3 :availability-proof? false}   ; replicated, unaudited
   :sla      {:replicas 5 :availability-proof? true}})  ; replicated + audited, premium

(defn- bytes-concat
  "a ++ b in the platform's native byte-buffer type (byte[] on :clj,
   Uint8Array on :cljs) — the type multiformats.core/sha256 requires
   directly, since it does no seq-coercion of its own."
  [a b]
  #?(:clj
     (let [^bytes a (byte-array a)
           ^bytes b (byte-array b)
           out (byte-array (+ (alength a) (alength b)))]
       (System/arraycopy a 0 out 0 (alength a))
       (System/arraycopy b 0 out (alength a) (alength b))
       out)
     :cljs
     (let [a (js/Uint8Array. a)
           b (js/Uint8Array. b)
           out (js/Uint8Array. (+ (.-length a) (.-length b)))]
       (.set out a 0)
       (.set out b (.-length a))
       out)))

(defn- salted-hash [block-bytes nonce]
  (mf/hexify (mf/sha256 (bytes-concat block-bytes nonce))))

(defn challenge
  "Verifier -> one challenge for one (node, cid) pair. `nonce` is
   caller-supplied randomness (host chooses the source; this stays a
   pure fn of its inputs, matching this codebase's Date.now/random
   discipline elsewhere)."
  [cid nonce epoch]
  {:kotobase.availability/cid cid
   :kotobase.availability/nonce nonce
   :kotobase.availability/epoch epoch})

(defn prove
  "Storage node's response, via the SAME get-fn seam kotobase-peer.core/
   commit! and hot-datoms already use. Returns nil (not a fabricated
   proof) if the node lacks the block — fails closed, same discipline as
   kotobase-peer.core/verify-chain and quota-exceeded?."
  [get-fn {:kotobase.availability/keys [cid nonce]}]
  (when-let [block-bytes (get-fn cid)]
    {:kotobase.availability/cid cid
     :kotobase.availability/proof (salted-hash block-bytes nonce)}))

(defn verify
  "Verifier resolves `cid` through ITS OWN get-fn (its own replica) and
   recomputes the salted hash — only the compact `proof` crosses the
   wire from the node under audit, not the block itself. Never throws;
   fails closed on a missing local replica or a malformed/missing
   response."
  [verifier-get-fn {:kotobase.availability/keys [cid nonce]} node-response]
  (let [local-bytes (verifier-get-fn cid)]
    (cond
      (nil? local-bytes) :verifier-lacks-replica
      (nil? node-response) :missed
      (not= (:kotobase.availability/cid node-response) cid) :malformed
      (= (:kotobase.availability/proof node-response) (salted-hash local-bytes nonce)) :ok
      :else :failed)))

(defn audit-outcome
  "One epoch's result for one (node, cid) pair -> a pure decision record.
   Caller wires :ok into a storage economy's settle/credit path and
   :failed/:missed into kekkai membership standing — this ns holds no
   opinion on credits or admission, the same stance kotobase-peer.core
   already takes on CACAO auth (its own README: 'no auth opinion of its
   own')."
  [node cid epoch verdict]
  {:audit/node node :audit/cid cid :audit/epoch epoch :audit/verdict verdict})
