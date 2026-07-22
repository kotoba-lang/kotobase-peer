(ns kotobase-peer.atomic-publication-test
  (:require [clojure.test :refer [deftest is testing]]
            [ipld.core :as ipld]
            [kotobase-peer.atomic-publication :as publication]
            [kotobase-peer.materialized-view :as view]
            [kotobase-peer.merkle-lsm :as lsm]))

(def datoms
  [{:e "alice" :a "name" :v "Alice"}
   {:e "alice" :a "role" :v "admin"}])

(defn fixture []
  (let [base (lsm/flush-plan {:db-id "tenant-a" :epoch 7 :datoms datoms})
        base-cid (get-in base [:manifest :cid])
        cards (view/build-view
               {:view-id "people/cards" :epoch 7 :source-manifest base-cid
                :entries [{:key "alice" :value {"name" "Alice"}}]})]
    {:base base :cards cards
     :statistics {:epoch 7 :visibility-scope "tenant-a/public"
                  :clauses [{:pattern [nil "role" "admin"] :rows 1}]}}))

(deftest one-root-publishes-base-statistics-and-views
  (let [{:keys [base cards statistics]} (fixture)
        plan (publication/build-plan
              {:db-id "tenant-a" :expected nil :base-plan base
               :statistics statistics :views [cards]})
        effects (:effects plan)
        root (get-in plan [:publication :node])]
    (is (= "kotobase/epoch-publication" (get root "format")))
    (is (= 7 (get root "epoch")))
    (is (= (get-in base [:manifest :cid])
           (ipld/link-cid (get root "base-manifest"))))
    (is (= (get-in cards [:bundle :cid])
           (ipld/link-cid (get-in root ["views" "people/cards" "bundle"]))))
    (is (= :head/cas (:effect/type (peek effects))))
    (is (= 1 (count (filter #(= :head/cas (:effect/type %)) effects))))
    (is (= (get-in plan [:publication :cid]) (:next (peek effects))))
    (is (every? #{:block/put :object/put}
                (map :effect/type (pop effects))))))

(deftest plan-is-deterministic-and-migration-reader-is-explicit
  (let [{:keys [base cards statistics]} (fixture)
        second-view (view/build-view
                     {:view-id "people/by-role" :epoch 7
                      :source-manifest (get-in base [:manifest :cid])
                      :entries [{:key "admin/alice" :value "alice"}]})
        options {:db-id "tenant-a" :expected nil :base-plan base
                 :statistics statistics}
        a (publication/build-plan (assoc options :views [cards second-view]))
        b (publication/build-plan (assoc options :views [second-view cards]))]
    (is (= (get-in a [:publication :cid]) (get-in b [:publication :cid])))
    (is (= (get-in base [:manifest :cid])
           (publication/base-manifest-cid
            (get-in a [:publication :node]) (get-in a [:publication :cid]))))
    (is (= (get-in base [:manifest :cid])
           (publication/base-manifest-cid
            (get-in base [:manifest :node]) (get-in base [:manifest :cid]))))))

(deftest epoch-source-and-id-mismatches-fail-before-effects
  (let [{:keys [base cards statistics]} (fixture)]
    (testing "statistics cannot lag the base snapshot"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (publication/build-plan
                    {:db-id "tenant-a" :base-plan base
                     :statistics (assoc statistics :epoch 6) :views [cards]}))))
    (testing "a derived bundle cannot describe another base manifest"
      (let [wrong (view/build-view
                   {:view-id "wrong" :epoch 7
                    :source-manifest (ipld/cid (ipld/encode {"wrong" true}))
                    :entries []})]
        (is (thrown? #?(:clj Exception :cljs js/Error)
                     (publication/build-plan
                      {:db-id "tenant-a" :base-plan base
                       :statistics statistics :views [wrong]})))))
    (testing "view ids are unique within an epoch"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (publication/build-plan
                    {:db-id "tenant-a" :base-plan base
                     :statistics statistics :views [cards cards]}))))
    (testing "the constituent base plan cannot hide another publication"
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (publication/build-plan
                    {:db-id "tenant-a"
                     :base-plan (update base :effects conj
                                        {:effect/type :head/cas :db-id "other"
                                         :expected nil :next "wrong"})
                     :statistics statistics :views [cards]}))))))
