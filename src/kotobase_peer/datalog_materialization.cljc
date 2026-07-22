(ns kotobase-peer.datalog-materialization
  "Differential maintenance for Datalog-backed IPLD materialized views."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [kotobase-peer.atomic-publication :as publication]
            [kotobase-peer.core :as peer]
            [kotobase-peer.materialized-view :as view]
            [kotobase-peer.merkle-lsm :as lsm]
            [kotobase-peer.statistics :as statistics]))

(defn- datalog-var? [value]
  (and (symbol? value) (str/starts-with? (name value) "?")))

(defn- simple-query? [query]
  (and (not (:rules query))
       (every? #(and (vector? %) (= 3 (count %))) (:where query))
       (every? datalog-var? (:find query))))

(defn- change-bindings
  "Unify one effective datom delta with a positive triple clause."
  [clause {:keys [e a v]}]
  (reduce
   (fn [bindings [term actual]]
     (cond
       (= term '_) bindings
       (datalog-var? term)
       (if (and (contains? bindings term) (not= (get bindings term) actual))
         (reduced nil)
         (assoc bindings term actual))
       (= term actual) bindings
       :else (reduced nil)))
   {}
   (map vector clause [e a v])))

(defn- query-with-bindings
  [db query visible? inputs bindings]
  (let [input-vars (vec (remove #{'$} (or (:in query) [])))
        supplied (zipmap input-vars inputs)]
    (if (some (fn [[variable value]]
                (and (contains? supplied variable)
                     (not= (get supplied variable) value)))
              bindings)
      #{}
      (let [additional (->> (keys bindings)
                            (remove (set input-vars))
                            (sort-by name)
                            vec)
            bound-query (assoc query :in (vec (concat input-vars additional)))
            bound-inputs (vec (concat inputs (map bindings additional)))]
        (peer/query db bound-query visible? bound-inputs)))))

(defn- anchored-results
  [db query visible? inputs changes]
  (into #{}
        (mapcat
         (fn [change]
           (mapcat (fn [clause]
                     (if-let [bindings (change-bindings clause change)]
                       (query-with-bindings db query visible? inputs bindings)
                       []))
                   (:where query))))
        changes))

(defn- result-exists?
  [db query visible? inputs result]
  (contains? (query-with-bindings db query visible? inputs
                                  (zipmap (:find query) result))
             result))

(defn- result-changes [current next]
  (vec
   (concat
    (map (fn [result] {:result result :op :retract})
         (sort-by pr-str (set/difference current next)))
    (map (fn [result] {:result result :op :assert})
         (sort-by pr-str (set/difference next current))))))

(defn maintain-query-delta
  "Maintain one set-valued Datalog result after EFFECTIVE-DELTAS.

  Plain positive conjunctive queries use a differential path: each changed
  datom anchors every compatible clause, and only affected result tuples are
  checked against the after snapshot. Full Datalog grammar remains correct via
  deterministic recomputation for negation, aggregation, functions, or rules."
  [{:keys [db-before db-after query visible? inputs current-result
           effective-deltas]
    :or {inputs []}}]
  (when-not (fn? visible?)
    (throw (ex-info "Materialized query requires visible?" {})))
  (let [current (set current-result)]
    (if (simple-query? query)
      (let [assertions (filterv #(= :assert (:op %)) effective-deltas)
            retractions (filterv #(= :retract (:op %)) effective-deltas)
            candidates (set/union
                        (anchored-results db-after query visible? inputs assertions)
                        (anchored-results db-before query visible? inputs retractions))
            next (reduce (fn [result candidate]
                           (if (result-exists? db-after query visible? inputs candidate)
                             (conj result candidate)
                             (disj result candidate)))
                         current candidates)]
        {:mode :differential
         :candidate-count (count candidates)
         :result next
         :changes (result-changes current next)})
      (let [next (set (peer/query db-after query visible? inputs))]
        {:mode :recompute
         :candidate-count (count next)
         :result next
         :changes (result-changes current next)}))))

(defn- result->entry [{:keys [result op]}]
  {:key (view/view-key result) :value result :op op})

(defn refresh-plan
  "Apply TX-DATA and atomically publish its base flush, refreshed statistics,
  and every registered Datalog view delta at NEW-EPOCH.

  VIEW-SPECS require :view-id, :query, :visible?, :current-result,
  :previous-epoch, and :previous-bundle. REGISTERED-VIEW-IDS is the complete
  registry at the expected head; all registered views are refreshed together."
  [{:keys [db-before tx-data db-id tenant new-epoch safe-epoch expected-head
           previous-base-manifest base-statistics query-statistics view-specs
           registered-view-ids target-run-rows]
    :or {safe-epoch 0 target-run-rows 4096}}]
  (let [view-ids (mapv (comp str :view-id) view-specs)]
    (when-not (and (integer? new-epoch) (not (neg? new-epoch))
                   (seq registered-view-ids)
                   (= (set (map str registered-view-ids)) (set view-ids))
                   (= (count view-ids) (count (set view-ids))))
      (throw (ex-info "Refresh must include every registered view exactly once"
                      {:new-epoch new-epoch
                       :registered (set (map str registered-view-ids))
                       :supplied view-ids}))))
  (let [{:keys [db-after effective-deltas]}
        (peer/transact-effective db-before tx-data)]
    (when (empty? effective-deltas)
      (throw (ex-info "No effective transaction delta to publish" {})))
    (let [base-plan (lsm/flush-plan
                     {:db-id db-id :tenant tenant :epoch new-epoch
                      :safe-epoch safe-epoch :previous previous-base-manifest
                      :expected expected-head :datoms effective-deltas
                      :statistics (or base-statistics {})
                      :target-run-rows target-run-rows})
          base-cid (get-in base-plan [:manifest :cid])
          refreshed-statistics
          (statistics/refresh-query-statistics query-statistics
                                               effective-deltas new-epoch)
          maintained
          (mapv (fn [{:keys [view-id query visible? inputs current-result
                             previous-bundle previous-epoch block-rows plan-cid]
                      :as spec}]
                  (when-not (and (seq (str view-id)) previous-bundle
                                 (integer? previous-epoch)
                                 (< previous-epoch new-epoch))
                    (throw (ex-info "Each refresh view needs an older pinned bundle"
                                    {:view spec})))
                  (let [delta (maintain-query-delta
                               {:db-before db-before :db-after db-after
                                :query query :visible? visible? :inputs inputs
                                :current-result current-result
                                :effective-deltas effective-deltas})
                        built (view/build-view-delta
                               {:view-id view-id :epoch new-epoch
                                :changes (mapv result->entry (:changes delta))
                                :previous-bundle previous-bundle
                                :source-manifest base-cid :plan-cid plan-cid
                                :block-rows (or block-rows 512)})]
                    {:view-id view-id :maintenance delta :view built}))
                view-specs)
          atomic (publication/build-plan
                  {:db-id db-id :expected expected-head :base-plan base-plan
                   :statistics refreshed-statistics
                   :views (mapv :view maintained)})]
      {:db-after db-after
       :effective-deltas effective-deltas
       :query-statistics refreshed-statistics
       :views maintained
       :plan atomic})))
