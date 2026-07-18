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
            [clojure.string :as str]
            [ipld.core :as ipld]))

(def policy-entity "kotobase.policy/read")

(def read-protected-capability "kotoba://can/datom:read-protected")
(def read-capability "kotoba://can/datom:read")
(def decrypt-capability "kotoba://can/datom:decrypt")
(def transact-capability "kotoba://can/datom:transact")
(def policy-admin-capability "kotoba://can/datom:policy-admin")

(defn policy-cid
  "Stable content identity of the normalized policy decision input."
  [policy]
  (when policy
    (:cid (ipld/node->block {"type" "kotobase/read-policy/v1"
                             "policy" (pr-str policy)}))))

(defn visibility-evidence [visible?]
  (select-keys (meta visible?) [:policy-cid :policy-reason :policy-mode]))

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
        read-value (fn [k]
                     (try
                       (some-> (get attrs k) edn/read-string)
                       (catch #?(:clj Exception :cljs :default) _ nil)))
        prefixes (read-vec ":kotobase.policy/protected-prefixes")
        owner-attrs (read-vec ":kotobase.policy/owner-attrs")
        write-prefixes (read-vec ":kotobase.policy/write-prefixes")
        write-entity-prefixes (read-vec ":kotobase.policy/write-entity-prefixes")
        write-actions (read-vec ":kotobase.policy/write-actions")
        max-datoms (read-value ":kotobase.policy/max-datoms-per-tx")
        required-purpose (read-value ":kotobase.policy/required-purpose")]
    (when (and (sequential? prefixes) (seq prefixes)
               (every? string? prefixes))
      (cond-> {:protected-prefixes (vec prefixes)}
        (contains? #{:public :private :sealed}
                   (read-value ":kotobase.policy/security-mode"))
        (assoc :security-mode (read-value ":kotobase.policy/security-mode"))
        ;; Phase 3c (ADR-2607174500 addendum 2): attrs whose VALUE is a DID
        ;; that owns the entity. A viewer whose verified CACAO issuer DID
        ;; matches ANY owner-attr value on an entity sees that entity's
        ;; protected rows even without the read-protected capability — the
        ;; owner reading their own data (e.g. a DM's :dm.message/author).
        (and (sequential? owner-attrs) (seq owner-attrs)
             (every? string? owner-attrs))
        (assoc :owner-attrs (vec owner-attrs))
        (and (sequential? write-prefixes) (seq write-prefixes)
             (every? string? write-prefixes))
        (assoc :write-prefixes (vec write-prefixes))
        (and (sequential? write-entity-prefixes) (seq write-entity-prefixes)
             (every? string? write-entity-prefixes))
        (assoc :write-entity-prefixes (vec write-entity-prefixes))
        (and (sequential? write-actions) (seq write-actions)
             (every? #(contains? #{:assert :retract} %) write-actions))
        (assoc :write-actions (set write-actions))
        (and (integer? max-datoms) (pos? max-datoms))
        (assoc :max-datoms-per-tx max-datoms)
        (and (string? required-purpose) (seq required-purpose))
        (assoc :required-purpose required-purpose)))))

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

(defn visible-for-mode
  "Fail-closed visibility entry point for network services. MODE is the graph's
  creation-time mode when policy data is absent/unparseable. Private/sealed
  graphs deny data rows on policy failure; legacy-public is explicit migration
  compatibility, never an implicit default."
  [policy caps owned-entities mode]
  (let [mode (or (:security-mode policy) mode :legacy-public)
        caps (set caps)
        policy-row? (fn [row] (= policy-entity (or (:e row) (:s row))))
        [visible? reason]
        (cond
          (= mode :legacy-public) [(visible-for policy caps owned-entities) :legacy-public]
          (and (= mode :public) (nil? policy)) [policy-row? :deny/missing-public-policy]
          (nil? policy) [policy-row? :deny/missing-policy]
          (and (= mode :private) (not (contains? caps read-capability)))
          [policy-row? :deny/missing-read-capability]
          (= mode :sealed)
          (if (contains? caps decrypt-capability)
            [(visible-for policy caps owned-entities) :allow/decrypt-capability]
            [policy-row? :deny/missing-decrypt-capability])
          :else [(visible-for policy caps owned-entities) :policy-filtered])]
    (with-meta visible? {:policy-cid (policy-cid policy)
                         :policy-reason reason :policy-mode mode})))

(defn- quad-action [quad]
  (if (contains? #{:retract :retract-entity} (:op quad)) :retract :assert))

(defn write-decision
  "Authorize a complete transaction before any block is written. CONTEXT must
  contain normalized effective capabilities, never raw CACAO resources.
  Returns {:allowed? ... :denials [...]}; callers commit only when allowed.

  A valid policy grants ordinary writes only to :write-prefixes. Policy/schema/
  authority records require the separately trusted policy-admin capability.
  Missing policy is permitted only in explicit :legacy-public mode."
  [policy context quads mode]
  (let [caps (set (:effective-caps context))
        admin? (contains? caps policy-admin-capability)
        transact? (contains? caps transact-capability)
        mode (or (:security-mode policy) mode :legacy-public)
        prefixes (:write-prefixes policy)
        entity-prefixes (:write-entity-prefixes policy)
        allowed-actions (or (:write-actions policy) #{:assert :retract})
        max-datoms (:max-datoms-per-tx policy)
        required-purpose (:required-purpose policy)
        purpose (:purpose context)
        owner-attrs (set (:owner-attrs policy))
        protected-prefixes [":kotobase.policy/" ":kotobase.authority/"
                            ":kotobase.schema/" ":db/"]
        denial
        (fn [{:keys [s p] :as quad}]
          (let [p (or p "")
                action (quad-action quad)
                immutable-mode-change? (and policy
                                            (= p ":kotobase.policy/security-mode"))
                protected? (or (= s policy-entity)
                               (contains? owner-attrs p)
                               (some #(str/starts-with? p %) protected-prefixes))
                prefix-ok? (or (and (= mode :legacy-public) (nil? policy))
                               (some #(str/starts-with? p %) prefixes))
                entity-ok? (or (empty? entity-prefixes)
                               (some #(str/starts-with? (or s "") %) entity-prefixes))]
            (cond
              (not transact?) {:quad quad :reason :write/missing-transact-capability}
              (and required-purpose (not= required-purpose purpose))
              {:quad quad :reason :write/purpose-denied}
              (not (contains? allowed-actions action))
              {:quad quad :reason :write/action-denied}
              (not entity-ok?) {:quad quad :reason :write/entity-denied}
              immutable-mode-change?
              {:quad quad :reason :write/security-mode-immutable}
              (and protected? (not admin?))
              {:quad quad :reason :write/policy-admin-required}
              (and (not protected?) (not prefix-ok?))
              {:quad quad :reason :write/attribute-denied}
              :else nil)))
        denials (cond-> (vec (keep denial quads))
                  (and max-datoms (> (count quads) max-datoms))
                  (conj {:reason :write/quota-exceeded
                         :count (count quads) :max max-datoms}))]
    {:allowed? (empty? denials) :denials denials
     :policy-mode mode}))

(defn assert-write-authorized!
  [policy context quads mode]
  (let [decision (write-decision policy context quads mode)]
    (when-not (:allowed? decision)
      (throw (ex-info "kotobase write policy denied transaction"
                      {:type :kotobase.policy/write-denied
                       :decision decision})))
    decision))
