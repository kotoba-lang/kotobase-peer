(ns kotobase-peer.policy
  "Read-visibility policy — the concrete form of ADR-2607050500's
  'capability/purpose-scoped redaction' placeholder (Phase 3,
  ADR-2607174500).

  The policy is IN-GRAPH DATA, not worker configuration: datoms on the
  well-known entity `kotobase.policy/read` travel with the chain, so any
  peer that syncs the graph enforces the identical policy — the same
  distribution argument as the materialized views (ADR-2607166600).

  Model (deliberately minimal, additive later):
    {:db/id \"kotobase.policy/read\"
     :kotobase.policy/protected-prefixes \"[\\\":dm.\\\" \\\":secret.\\\"]\"}
  Rows whose :a starts with ANY protected prefix are visible only to a
  viewer whose verified CACAO carries `read-protected-capability` in its
  resources. A graph with NO policy entity is fully public — exactly
  today's behavior, so rollout is zero-regression by construction.

  `visible-for` returns the row post-filter every read producer in
  kotobase-peer already threads (datoms / cold-datoms / hot-datoms / q /
  view-rows — 'Query is a first-class effect'): the seam existed
  everywhere, this ns just decides the fn from (policy × viewer caps)."
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.string :as str]))

(def policy-entity "kotobase.policy/read")

(def read-protected-capability "kotoba://can/datom:read-protected")

(defn policy-of
  "Current-state rows ({:e :a :v_edn ...}) of the POLICY ENTITY →
  {:protected-prefixes [...]}, or nil when the graph declares no policy.
  Tolerant of extra rows (callers may pass a whole-entity read)."
  [rows]
  (let [attrs (reduce (fn [m {:keys [e a v_edn added]}]
                        (if (and (= e policy-entity) (not (false? added)))
                          (assoc m a v_edn)
                          m))
                      {} rows)
        ;; v_edn is the wire-EDN of the stored value; the stored value is
        ;; itself an EDN string of the prefix vector (the same
        ;; string-blob convention tx values use throughout this stack).
        read-vec (fn [k]
                   (try
                     (some-> (get attrs k)
                             edn/read-string
                             (as-> v (if (string? v) (edn/read-string v) v)))
                     (catch #?(:clj Exception :cljs :default) _ nil)))
        prefixes (read-vec ":kotobase.policy/protected-prefixes")
        owner-attrs (read-vec ":kotobase.policy/owner-attrs")]
    (when (and (sequential? prefixes) (seq prefixes)
               (every? string? prefixes))
      (cond-> {:protected-prefixes (vec prefixes)}
        ;; Phase 3c (ADR-2607174500 addendum 2): attrs whose VALUE is a DID
        ;; that owns the entity. A viewer whose verified CACAO issuer DID
        ;; matches ANY owner-attr value on an entity sees that entity's
        ;; protected rows even without the read-protected capability — the
        ;; owner reading their own data (e.g. a DM's :dm.message/author).
        (and (sequential? owner-attrs) (seq owner-attrs)
             (every? string? owner-attrs))
        (assoc :owner-attrs (vec owner-attrs))))))

(defn visible-for
  "(policy × viewer capability strings) → the `visible?` row fn.
  nil policy → (constantly true): a policy-less graph stays fully public.
  With a policy, rows under a protected prefix require
  `read-protected-capability` among the viewer's VERIFIED CACAO
  resources; everything else stays visible. The policy entity's own rows
  are always visible (a viewer may inspect what is being withheld —
  redaction, not stealth)."
  ([policy caps] (visible-for policy caps nil))
  ([policy caps owned-entities]
   (if (nil? policy)
     (constantly true)
     (let [prefixes (:protected-prefixes policy)
           allowed? (contains? (set caps) read-protected-capability)
           owned (set owned-entities)]
       (if allowed?
         (constantly true)
         ;; rows arrive in TWO shapes depending on the producer: datoms/
         ;; hot-datoms/view-rows emit {:e :a :v_edn}, arrangement.query (q)
         ;; emits {:s :p :o} — treat :a/:p and :e/:s as the same positions
         ;; (they are; see kotobase-peer.core/datoms' docstring).
         (fn [row]
           (let [ent (or (:e row) (:s row))
                 attr (or (:a row) (:p row))]
             (or (= ent policy-entity)
                 (contains? owned ent)       ; owner-based disclosure (Phase 3c)
                 (not (some #(str/starts-with? (or attr "") %) prefixes))))))))))
