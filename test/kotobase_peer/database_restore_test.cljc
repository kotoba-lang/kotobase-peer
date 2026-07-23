(ns kotobase-peer.database-restore-test
  #?(:clj (:require [clojure.test :refer [deftest is testing]]
                    [ipld.core :as ipld]
                    [kotobase-peer.database-restore :as restore])
     :cljs (:require [cljs.test :refer-macros [deftest is testing]]
                     [ipld.core :as ipld]
                     [kotobase-peer.database-restore :as restore])))

(defn cid [value] (ipld/cid (ipld/encode value)))

(deftest database-restore-page-checkpoints-fence-order-and-publication
  (let [head (cid {"head" 1})
        page-a (cid {"page" 0})
        page-b (cid {"page" 1})
        task (restore/restore-task
              {:inventory-cid (cid {"inventory" 1})
               :target-db-id "restored"
               :head-cid head
               :entry-count 3
               :page-count 2})
        initial (restore/initial-checkpoint
                 {:task task :token "attempt-token" :attempt 1})
        first-page (restore/advance-page
                    {:task task :checkpoint initial
                     :token "attempt-token" :attempt 1
                     :page-ordinal 0 :page-cid page-a
                     :entry-count 2 :restored 2 :already-present 0
                     :first-entry ["blocks" "cid-a"]
                     :last-entry ["blocks" "cid-b"]})
        second-page (restore/advance-page
                     {:task task :checkpoint first-page
                      :token "attempt-token" :attempt 1
                      :page-ordinal 1 :page-cid page-b
                      :entry-count 1 :restored 0 :already-present 1
                      :first-entry ["objects" "cid-c"]
                      :last-entry ["objects" "cid-c"]})
        reclaimed (restore/reclaim-checkpoint
                   {:task task :checkpoint second-page
                    :old-token "attempt-token" :old-attempt 1
                    :new-token "reclaimed-token" :new-attempt 2})
        ready (restore/ready-to-publish
               {:task task :checkpoint reclaimed
                :token "reclaimed-token" :attempt 2
                :verified-reachable 3})
        completed (restore/complete
                   {:task task :checkpoint ready
                    :token "reclaimed-token" :attempt 2
                    :observed-head (str head)})]
    (is (= "running" (get-in initial [:node "status"])))
    (is (= 1 (get-in first-page [:node "next-page"])))
    (is (= 3 (get-in second-page [:node "processed-entries"])))
    (is (= 2 (get-in second-page [:node "restored"])))
    (is (= 1 (get-in second-page [:node "already-present"])))
    (is (= 2 (get-in reclaimed [:node "attempt"])))
    (is (= 2 (get-in reclaimed [:node "next-page"])))
    (is (= "ready-to-publish" (get-in ready [:node "status"])))
    (is (= "completed" (get-in completed [:node "status"])))
    (testing "page replay, token mismatch, incomplete verification and wrong head fail closed"
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (restore/advance-page
                    {:task task :checkpoint first-page
                     :token "attempt-token" :attempt 1
                     :page-ordinal 0 :page-cid page-a
                     :entry-count 2 :restored 2 :already-present 0
                     :first-entry ["blocks" "cid-a"]
                     :last-entry ["blocks" "cid-b"]})))
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (restore/advance-page
                    {:task task :checkpoint first-page
                     :token "attempt-token" :attempt 1
                     :page-ordinal 1 :page-cid page-b
                     :entry-count 1 :restored 1 :already-present 0
                     :first-entry ["blocks" "cid-a"]
                     :last-entry ["objects" "cid-c"]})))
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (restore/ready-to-publish
                    {:task task :checkpoint second-page
                     :token "wrong" :attempt 1
                     :verified-reachable 3})))
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (restore/ready-to-publish
                    {:task task :checkpoint second-page
                     :token "attempt-token" :attempt 1
                     :verified-reachable 2})))
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (restore/reclaim-checkpoint
                    {:task task :checkpoint second-page
                     :old-token "attempt-token" :old-attempt 1
                     :new-token "reclaimed-token" :new-attempt 3})))
      (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                   (restore/complete
                    {:task task :checkpoint ready
                     :token "attempt-token" :attempt 1
                     :observed-head "different"}))))))

(deftest restore-task-identity-pins-inventory-target-and-bounds
  (let [base {:inventory-cid (cid {"inventory" 1})
              :target-db-id "restored"
              :head-cid (cid {"head" 1})
              :entry-count 10
              :page-count 2}]
    (is (= (:cid (restore/restore-task base))
           (:cid (restore/restore-task base))))
    (is (not= (:cid (restore/restore-task base))
              (:cid (restore/restore-task
                     (assoc base :target-db-id "other")))))
    (is (not= (:cid (restore/restore-task base))
              (:cid (restore/restore-task
                     (assoc base :entry-count 11)))))))
