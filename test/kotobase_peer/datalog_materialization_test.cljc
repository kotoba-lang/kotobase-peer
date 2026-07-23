(ns kotobase-peer.datalog-materialization-test
  (:require [clojure.test :refer [deftest is testing]]
            [ipld.core :as ipld]
            [kotobase-peer.core :as peer]
            [kotobase-peer.datalog-materialization :as materialization]
            [kotobase-peer.materialized-view :as view]
            [kotobase-peer.merkle-lsm :as lsm]))

(def visible? (constantly true))

(defn db-with [datoms]
  (peer/transact (peer/empty-db) datoms))

(def people-query
  {:find '[?person ?name]
   :where '[[?person "role" "admin"]
            [?person "name" ?name]]})

(deftest positive-joins-use-differential-maintenance
  (let [before (db-with [["alice" "role" "admin"]
                         ["alice" "name" "Alice"]
                         ["bob" "name" "Bob"]])
        {:keys [db-after effective-deltas]}
        (peer/transact-effective before [["bob" "role" "admin"]])
        result (materialization/maintain-query-delta
                {:db-before before :db-after db-after :query people-query
                 :visible? visible? :current-result #{["alice" "Alice"]}
                 :effective-deltas effective-deltas})]
    (is (= :differential (:mode result)))
    (is (= #{["alice" "Alice"] ["bob" "Bob"]} (:result result)))
    (is (= [{:result ["bob" "Bob"] :op :assert}] (:changes result)))))

(deftest join-frontier-plans-bound-index-lookups
  (let [query {:find '[?team]
               :where '[[?team "member" ?person]
                        [?person "role" "admin"]]}
        seeds (materialization/change-frontier-seeds
               query [{:e "alice" :a "role" :v "admin"}])
        seed (first seeds)]
    (is (= #{'{?person "alice"}} (set seeds)))
    (is (= {:index :avet :components ["member" "alice"]}
           (materialization/clause-lookup
            '[?team "member" ?person] seed)))
    (is (= {:index :eavt :components ["ops" "member" "alice"]}
           (materialization/clause-lookup
            '[?team "member" ?person]
            '{?team "ops" ?person "alice"})))
    (is (= [{'?person "alice" '?team "ops"}]
           (materialization/frontier-next-bindings
            '[?team "member" ?person] seed
            [["ops" "member" "alice"]
             ["sales" "member" "bob"]])))
    (is (= 1 (:clause-index
              (materialization/frontier-step-plan query [0 1] [seed])))
        "the fully bound EAVT anchor is evaluated before an AVET join")))

(deftest join-frontier-refuses-an-unbounded-variable-clause
  (is (nil? (materialization/clause-lookup '[?e ?a ?v] {}))))

(deftest join-frontier-work-chain-is-byte-bounded-and-canonical
  (let [bindings (mapv (fn [n] {'?person (str "person-" n)
                                 '?team (str "team-" (mod n 3))})
                       (range 24))
        options {:snapshot :after :remaining [0 2]
                 :bindings bindings :max-bytes 240}
        chain-a (materialization/build-frontier-work-chain options)
        chain-b (materialization/build-frontier-work-chain options)
        by-cid (into {} (map (juxt :cid :node) (:nodes chain-a)))]
    (is (= (:head chain-a) (:head chain-b)))
    (is (= (mapv :cid (:nodes chain-a)) (mapv :cid (:nodes chain-b))))
    (is (< 1 (count (:nodes chain-a))))
    (is (every? #(<= #?(:clj (alength ^bytes (:bytes %))
                        :cljs (.-byteLength (:bytes %)))
                     240)
                (:nodes chain-a)))
    (loop [cid (:head chain-a) decoded []]
      (if cid
        (let [work (materialization/decode-frontier-work (get by-cid cid))]
          (is (= :after (:snapshot work)))
          (is (= [0 2] (:remaining work)))
          (recur (:next-work work) (into decoded (:bindings work))))
        (is (= bindings decoded))))))

(deftest join-frontier-work-can-prepend-an-existing-chain
  (let [tail (materialization/build-frontier-work-chain
              {:snapshot :before :remaining [1]
               :bindings [{'?person "tail"}] :max-bytes 512})
        head (materialization/build-frontier-work-chain
              {:snapshot :after :remaining [0]
               :bindings [{'?person "head"}]
               :next-work (:head tail) :max-bytes 512})
        decoded (materialization/decode-frontier-work
                 (get-in head [:nodes 0 :node]))]
    (is (= (:head tail) (:next-work decoded)))
    (is (= [{'?person "head"}] (:bindings decoded)))))

(deftest join-frontier-work-preserves-a-bounded-prefix-cursor
  (let [bindings (mapv (fn [n] {'?person (str "person-" n)}) (range 12))
        block {"format" "test/remainder" "rows" [["person-5"]]}
        scan {:clause-index 0 :index :avet :after "member|person-4"
              :block-remainder [{:cid (str (ipld/cid (ipld/encode block)))
                                 :block block}]}
        chain (materialization/build-frontier-work-chain
               {:snapshot :after :remaining [0 1]
                :bindings bindings :scan scan :max-bytes 400})
        by-cid (into {} (map (juxt :cid :node) (:nodes chain)))]
    (is (< 1 (count (:nodes chain))))
    (loop [cid (:head chain) decoded []]
      (if cid
        (let [work (materialization/decode-frontier-work (get by-cid cid))]
          (is (= scan (:scan work)))
          (recur (:next-work work) (into decoded (:bindings work))))
        (is (= bindings decoded))))))

(deftest join-frontier-work-rejects-malformed-prefix-cursors
  (doseq [scan [{:clause-index 0 :index :unknown :after "x"}
                {:clause-index 1 :index :avet :after "x"}
                {:clause-index 0 :index :avet :after ""}]]
    (is (thrown-with-msg?
         #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
         #"Invalid join frontier"
         (materialization/build-frontier-work-chain
          {:snapshot :after :remaining [0]
           :bindings [{'?person "alice"}]
           :scan scan
           :max-bytes 512})))))

(deftest join-frontier-single-oversized-binding-fails-closed
  (is (thrown-with-msg?
       #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
       #"exceeds work byte budget"
       (materialization/build-frontier-work-chain
        {:snapshot :after :remaining [0]
         :bindings [{'?value (apply str (repeat 1024 "x"))}]
         :max-bytes 128}))))

(deftest retraction-checks-for-an-alternative-derivation
  (let [query {:find '[?team]
               :where '[[?team "member" ?person]
                        [?person "role" "admin"]]}
        before (db-with [["ops" "member" "alice"]
                         ["ops" "member" "bob"]
                         ["alice" "role" "admin"]
                         ["bob" "role" "admin"]])
        {:keys [db-after effective-deltas]}
        (peer/transact-effective
         before [[:db/retract "alice" "role" "admin"]])
        result (materialization/maintain-query-delta
                {:db-before before :db-after db-after :query query
                 :visible? visible? :current-result #{["ops"]}
                 :effective-deltas effective-deltas})]
    (is (= #{["ops"]} (:result result)))
    (is (empty? (:changes result))
        "another join derivation keeps the set-valued result alive")))

(deftest differential-maintenance-respects-query-inputs
  (let [query {:find '[?person] :in '[?wanted]
               :where '[[?person "role" ?wanted]]}
        before (db-with [["alice" "role" "admin"]])
        {:keys [db-after effective-deltas]}
        (peer/transact-effective before [["bob" "role" "admin"]
                                         ["carol" "role" "user"]])
        result (materialization/maintain-query-delta
                {:db-before before :db-after db-after :query query
                 :visible? visible? :inputs ["admin"]
                 :current-result #{["alice"]}
                 :effective-deltas effective-deltas})]
    (is (= :differential (:mode result)))
    (is (= #{["alice"] ["bob"]} (:result result)))
    (is (= [{:result ["bob"] :op :assert}] (:changes result)))))

(deftest affected-result-membership-does-not-require-the-complete-view
  (let [query {:find '[?person ?name]
               :where '[[?person "name" ?name]]}
        before (db-with [["alice" "name" "Alice"]
                         ["unrelated" "name" "Unchanged"]])
        {:keys [db-after effective-deltas]}
        (peer/transact-effective
         before [[:db/retract "alice" "name" "Alice"]
                 ["alice" "name" "Alicia"]])
        affected (materialization/affected-query-results
                  {:db-before before :db-after db-after :query query
                   :visible? visible? :effective-deltas effective-deltas})
        result (materialization/maintain-query-delta
                {:db-before before :db-after db-after :query query
                 :visible? visible?
                 :current-result #{["alice" "Alice"]}
                 :current-result-complete? false
                 :effective-deltas effective-deltas})]
    (is (materialization/bounded-single-clause-query? query))
    (is (= #{["alice" "Alice"] ["alice" "Alicia"]} affected))
    (is (nil? (:result result)))
    (is (= [{:result ["alice" "Alice"] :op :retract}
            {:result ["alice" "Alicia"] :op :assert}]
           (:changes result)))))

(deftest full-datalog-forms-use-correct-recompute-fallback
  (let [query {:find '[?person]
               :where '[[?person "name" _]
                        (not [?person "disabled" "true"])]}
        before (db-with [["alice" "name" "Alice"]])
        {:keys [db-after effective-deltas]}
        (peer/transact-effective before [["alice" "disabled" true]])
        result (materialization/maintain-query-delta
                {:db-before before :db-after db-after :query query
                 :visible? visible? :current-result #{["alice"]}
                 :effective-deltas effective-deltas})]
    (is (= :recompute (:mode result)))
    (is (empty? (:result result)))
    (is (= [{:result ["alice"] :op :retract}] (:changes result)))))

(deftest one-refresh-plan-publishes-base-statistics-and-all-view-deltas
  (let [before (db-with [["alice" "role" "admin"]
                         ["alice" "name" "Alice"]
                         ["bob" "name" "Bob"]])
        old-manifest (lsm/build-manifest {:db-id "tenant-a" :epoch 0})
        old-view (view/build-view
                  {:view-id "admins" :epoch 0
                   :source-manifest (:cid old-manifest)
                   :entries [{:key (view/view-key ["alice" "Alice"])
                              :value ["alice" "Alice"]}]})
        refresh
        (materialization/refresh-plan
         {:db-before before
          :tx-data [["bob" "role" "admin"]]
          :db-id "tenant-a" :new-epoch 1
          :expected-head "old-publication"
          :previous-base-manifest (:cid old-manifest)
          :base-statistics {"catalog-directory" (ipld/link (:cid old-manifest))}
          :query-statistics
          {:visibility-scope "tenant-a/public" :epoch 0
           :clauses [{:pattern [nil "role" "admin"] :rows 1}
                     {:pattern [nil "name" nil] :rows 2}]}
          :registered-view-ids #{"admins"}
          :view-specs
          [{:view-id "admins" :query people-query :visible? visible?
            :current-result #{["alice" "Alice"]}
            :previous-epoch 0
            :previous-bundle (get-in old-view [:bundle :cid])}]})
        plan (:plan refresh)
        base-cid (get-in plan [:result :base-manifest])
        bundle (get-in refresh [:views 0 :view :bundle :node])
        effects (:effects plan)
        base-node (->> effects
                       (filter #(= base-cid (:cid %)))
                       first :bytes ipld/decode)]
    (is (= 2 (get-in refresh [:query-statistics :clauses 0 :rows])))
    (is (= (:cid old-manifest)
           (ipld/link-cid
            (get-in base-node ["statistics" "catalog-directory"]))))
    (is (= :differential (get-in refresh [:views 0 :maintenance :mode])))
    (is (= base-cid (ipld/link-cid (get bundle "source-manifest"))))
    (is (= (get-in old-view [:bundle :cid])
           (ipld/link-cid (get bundle "previous-bundle"))))
    (is (= 1 (count (filter #(= :head/cas (:effect/type %)) effects))))
    (is (= :head/cas (:effect/type (peek effects))))
    (is (= "old-publication" (:expected (peek effects))))))

(deftest refresh-rejects-noop-and-partial-registration
  (let [before (db-with [["alice" "name" "Alice"]])]
    (testing "no state transition cannot create a fresh epoch"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (materialization/refresh-plan
                    {:db-before before :tx-data [["alice" "name" "Alice"]]
                     :db-id "tenant-a" :new-epoch 1
                     :query-statistics {:visibility-scope "x" :epoch 0
                                        :clauses []}
                     :registered-view-ids #{"v"}
                     :view-specs [{:view-id "v" :query people-query
                                   :visible? visible? :current-result #{}
                                   :previous-epoch 0
                                   :previous-bundle "missing"}]}))))
    (testing "a publication cannot silently omit the registered view set"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (materialization/refresh-plan
                    {:db-before before :tx-data [["bob" "name" "Bob"]]
                     :db-id "tenant-a" :new-epoch 1
                     :query-statistics {:visibility-scope "x" :epoch 0
                                        :clauses []}
                     :registered-view-ids #{"v"}
                     :view-specs []}))))))
