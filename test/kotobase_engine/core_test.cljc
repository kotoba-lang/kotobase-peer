(ns kotobase-engine.core-test
  (:require #?(:clj [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing] :include-macros true])
            [clojure.set :as set]
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

(deftest q-routes-through-kqe
  (let [db (eng/transact (eng/empty-db)
                          [{:s "alice" :p "role" :o "admin"}
                           {:s "bob" :p "role" :o "user"}])]
    (is (= #{{:s "alice" :p "role" :o "admin"}} (eng/q db ["alice" nil nil])))
    (is (= #{{:s "alice" :p "role" :o "admin"}} (eng/q db [nil "role" "admin"])))
    (is (= 2 (count (eng/q db [nil nil nil]))))))

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
            namespace's commit! composition"
    (let [db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
          {:keys [put! get-fn]} (mem-store)
          {put2! :put! get-fn2 :get-fn} (mem-store)
          a (:state (eng/head get-fn (eng/commit! put! get-fn db nil)))
          b (:state (eng/head get-fn2 (eng/commit! put2! get-fn2 db nil)))]
      (is (= a b)))))

(deftest commit-dag-chain-tracks-multiple-snapshots
  (let [{:keys [put! get-fn]} (mem-store)
        db1 (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
        db2 (eng/transact db1 [{:s "bob" :p "role" :o "user"}])
        c0 (eng/commit! put! get-fn db1 nil)
        c1 (eng/commit! put! get-fn db2 c0)
        history (eng/chain get-fn c1)]
    (is (= 2 (count history)))
    (is (= [0 1] (map :seq history)))
    (is (not= (:state (first history)) (:state (second history)))
        "different db content -> different snapshot CID")
    (is (= (eng/latest-snapshot-cid get-fn c1) (:state (last history))))
    (is (true? (eng/verify-chain get-fn c1)))))

(deftest verify-chain-catches-a-store-that-lies
  (let [{:keys [put! get-fn store]} (mem-store)
        db (eng/transact (eng/empty-db) [{:s "alice" :p "role" :o "admin"}])
        c0 (eng/commit! put! get-fn db nil)
        db2 (eng/transact db [{:s "bob" :p "role" :o "user"}])
        c1 (eng/commit! put! get-fn db2 c0)
        other-cid (eng/commit! put! get-fn (eng/transact (eng/empty-db)
                                                          [{:s "mallory" :p "role" :o "evil"}])
                                nil)]
    (is (true? (eng/verify-chain get-fn c1)))
    ;; splice a DIFFERENT (but validly dag-cbor-encoded) commit's bytes under
    ;; c0's own cid key -- a dishonest/corrupted store, without changing c0
    ;; itself. verify-chain must catch this via CID re-derivation, not throw.
    (swap! store assoc c0 (get @store other-cid))
    (is (false? (eng/verify-chain get-fn c1)))))
