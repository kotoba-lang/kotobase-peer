(ns kotobase-peer.plan-cardinality-test
  "ADR-2608021000 §6-4-1: the planner already measures each clause's
  cardinality to choose an order, and used to throw those numbers away. It now
  hands them to the executor, which uses them to choose a STRATEGY per clause
  -- one broad scan plus a hash join when a clause's relation is small
  relative to the number of keyed scans a step would issue for it.

  What is asserted here is that the hint is emitted, is keyed the way the
  executor looks it up, and matches the estimates the plan itself used. That
  the hint cannot change an ANSWER is arrangement's own equivalence suite;
  this side only has to not lie about the numbers."
  (:require [clojure.test :refer [deftest testing is]]
            [kotobase-peer.core :as eng]))

(def ^:private everything (constantly true))

(defn- db []
  (eng/transact (eng/empty-db)
                (concat
                 (for [i (range 30)] {:s (str "m" i) :p "creator" :o (str "p" (mod i 5))})
                 (for [i (range 30)] {:s (str "m" i) :p "date" :o (str i)})
                 (for [i (range 5)] {:s (str "p" i) :p "role" :o "author"}))))

(deftest the-plan-hands-its-own-estimates-to-the-executor
  (let [query '{:find [?m ?d]
                :where [[?m "creator" ?p] [?m "date" ?d] [?p "role" "author"]]}
        plan (eng/datalog-query-plan (db) query everything)
        hint (get-in plan [:query :clause-cardinality])]
    (is (:optimized? plan))
    (testing "keyed by the clause itself -- how arrangement.datalog looks it up"
      (is (= (set (:where query)) (set (keys hint)))))
    (testing "the numbers are the ones the plan ordered on, not a second guess"
      (is (= (into {} (map (juxt :clause :estimated-rows)) (:plan plan)) hint)))
    (is (= 30 (get hint '[?m "creator" ?p])))
    (is (= 5 (get hint '[?p "role" "author"])))))

(deftest an-unoptimized-plan-emits-no-hint
  ;; Queries with functions/negation keep source order and are not estimated,
  ;; so there is nothing to hand over. A hint that is absent leaves the
  ;; executor on the keyed path, which is the safe default.
  (let [query '{:find [?s] :where [[?s "date" ?d] [(> ?d 18)]]}
        plan (eng/datalog-query-plan (db) query everything)]
    (is (false? (:optimized? plan)))
    (is (nil? (get-in plan [:query :clause-cardinality])))))

(deftest the-hint-does-not-change-what-query-returns
  ;; End to end through kotobase-peer.core/query, which is what actually
  ;; threads the hint into arrangement.
  (let [d (db)
        query '{:find [?m] :where [[?m "creator" ?p] [?p "role" "author"]]}]
    (is (= 30 (count (eng/query d query everything))))))
