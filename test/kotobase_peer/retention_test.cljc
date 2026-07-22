(ns kotobase-peer.retention-test
  (:require [clojure.test :refer [deftest is testing]]
            [kotobase-peer.retention :as retention]))

(deftest retention-root-validation-and-activity
  (let [reader (retention/root-node {:db-id "db-a" :kind :reader :id "query-1"
                                     :manifest-cid "m-reader" :epoch 7
                                     :expires-at 2000})
        replica (retention/root-node {:db-id "db-a" :kind :replication :id "replica-1"
                                      :manifest-cid "m-replica" :epoch 5
                                      :expires-at 900})
        hold (retention/root-node {:db-id "db-a" :kind :legal-hold :id "case-1"
                                   :manifest-cid "m-hold" :epoch 3})
        release (retention/root-node {:db-id "db-a" :kind :release :id "v1"
                                      :manifest-cid "m-release" :epoch 4})]
    (is (retention/active? reader 1000))
    (is (not (retention/active? replica 1000)))
    (is (retention/active? hold 1000))
    (is (retention/active? release 1000))
    (is (= 3 (retention/minimum-safe-epoch [reader replica hold release] 1000)))
    (is (not (retention/active? (retention/release-node hold 1100) 1100)))
    (testing "leased roots cannot silently become durable"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (retention/root-node {:db-id "db-a" :kind :reader :id "bad"
                                         :manifest-cid "m" :epoch 1}))))
    (testing "an empty registry has no artificial epoch-zero pin"
      (is (nil? (retention/minimum-safe-epoch [] 1000))))
    (testing "decoded registry values are revalidated before CAS"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (retention/validate-node (assoc reader "version" 99)))))))
