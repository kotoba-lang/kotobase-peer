(ns kotobase-peer.policy-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [kotobase-peer.policy :as policy]))

(def ^:private policy-rows
  [{:e "kotobase.policy/read" :a ":kotobase.policy/protected-prefixes"
    :v_edn (pr-str (pr-str [":dm." ":secret."])) :added true}])

(deftest no-policy-means-public
  (is (nil? (policy/policy-of [])))
  (is (nil? (policy/policy-of [{:e "other" :a ":x/y" :v_edn "\"v\"" :added true}])))
  (let [visible? (policy/visible-for nil [])]
    (is (visible? {:e "any" :a ":dm.message/text" :v_edn "\"x\""}))
    (is (visible? {:e "any" :a ":secret.key/blob" :v_edn "\"x\""}))))

(deftest policy-parses-prefixes
  (is (= {:protected-prefixes [":dm." ":secret."]}
         (policy/policy-of policy-rows))))

(deftest redacts-protected-prefixes-without-capability
  (let [visible? (policy/visible-for (policy/policy-of policy-rows) [])]
    (is (not (visible? {:e "m1" :a ":dm.message/text" :v_edn "\"hi\""})))
    (is (not (visible? {:e "k1" :a ":secret.key/blob" :v_edn "\"x\""})))
    (is (visible? {:e "p1" :a ":yoro.post/text" :v_edn "\"public\""})
        "non-protected attrs stay visible")
    (is (visible? {:e "kotobase.policy/read"
                   :a ":kotobase.policy/protected-prefixes" :v_edn "\"[]\""})
        "the policy itself is always inspectable — redaction, not stealth")))

(deftest capability-opens-protected-rows
  (let [visible? (policy/visible-for (policy/policy-of policy-rows)
                                     [policy/read-protected-capability
                                      "kotoba://can/other"])]
    (is (visible? {:e "m1" :a ":dm.message/text" :v_edn "\"hi\""}))
    (is (visible? {:e "p1" :a ":yoro.post/text" :v_edn "\"public\""}))))

(deftest retracted-policy-rows-are-ignored
  (is (nil? (policy/policy-of
             [{:e "kotobase.policy/read" :a ":kotobase.policy/protected-prefixes"
               :v_edn (pr-str (pr-str [":dm."])) :added false}]))))

(deftest malformed-policy-degrades-to-public
  (is (nil? (policy/policy-of
             [{:e "kotobase.policy/read" :a ":kotobase.policy/protected-prefixes"
               :v_edn "\"not-a-vector\"" :added true}]))))

(deftest quad-shaped-rows-are-filtered-too
  ;; arrangement.query (q) rows are {:s :p :o}, not {:e :a :v_edn}
  (let [visible? (policy/visible-for (policy/policy-of policy-rows) [])]
    (is (not (visible? {:s "m1" :p ":dm.message/text" :o "hi"})))
    (is (visible? {:s "p1" :p ":yoro.post/text" :o "public"}))))

;; ── owner-based disclosure (Phase 3c, ADR-2607174500 addendum 2) ──────────────

(def ^:private owner-policy-rows
  [{:e "kotobase.policy/read" :a ":kotobase.policy/protected-prefixes"
    :v_edn (pr-str (pr-str [":dm."])) :added true}
   {:e "kotobase.policy/read" :a ":kotobase.policy/owner-attrs"
    :v_edn (pr-str (pr-str [":dm.message/author"])) :added true}])

(deftest policy-parses-owner-attrs
  (is (= {:protected-prefixes [":dm."] :owner-attrs [":dm.message/author"]}
         (policy/policy-of owner-policy-rows)))
  (is (nil? (:owner-attrs (policy/policy-of policy-rows)))
      "a policy without owner-attrs has none"))

(deftest owner-sees-own-protected-rows-others-do-not
  (let [pol (policy/policy-of owner-policy-rows)
        ;; m1 owned by did:alice, m2 owned by did:bob (owner set resolved by
        ;; the handler via avet reads; here we pass it directly)
        alice-view (policy/visible-for pol [] #{"m1"})
        stranger-view (policy/visible-for pol [] #{})]
    (is (alice-view {:e "m1" :a ":dm.message/text" :v_edn "\"hi\""})
        "owner sees their own protected row")
    (is (not (alice-view {:e "m2" :a ":dm.message/text" :v_edn "\"hers\""}))
        "owner does NOT see someone else's protected row")
    (is (not (stranger-view {:e "m1" :a ":dm.message/text" :v_edn "\"hi\""}))
        "a non-owner with no capability sees nothing protected")
    (is (alice-view {:e "m1" :a ":yoro.post/text" :v_edn "\"public\""})
        "public rows always visible")))

(deftest capability-still-trumps-ownership
  (let [pol (policy/policy-of owner-policy-rows)
        cap-view (policy/visible-for pol ["kotoba://can/datom:read-protected"] #{})]
    (is (cap-view {:e "m2" :a ":dm.message/text" :v_edn "\"anything\""})
        "the read-protected capability opens everything regardless of ownership")))
