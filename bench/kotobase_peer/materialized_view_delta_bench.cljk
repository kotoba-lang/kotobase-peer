(ns kotobase-peer.materialized-view-delta-bench
  (:require [kotobase-peer.materialized-view :as view]))

(defn- pad [i] (str (apply str (repeat (- 9 (count (str i))) "0")) i))
(defn- key-of [i] (str "tenant-a/" (pad i)))
(defn- timed [f]
  (let [start (System/nanoTime) value (f)]
    {:value value :ms (/ (- (System/nanoTime) start) 1e6)}))
(defn- percentile [xs p]
  (let [xs (vec (sort xs))]
    (nth xs (long (Math/floor (* p (dec (count xs))))))))

(defn measure [n change-count block-rows]
  (let [base-entries (mapv (fn [i] {:key (key-of i) :value {"id" i "version" 1}})
                           (range n))
        base (:value (timed #(view/build-view {:view-id :feed :epoch 1
                                               :entries base-entries :sorted? true
                                               :block-rows block-rows})))
        changed-ids (mapv #(mod (* % 7919) n) (range change-count))
        changes (mapv (fn [position i]
                        (if (odd? position)
                          {:key (key-of i) :value nil :op :retract}
                          {:key (key-of i) :value {"id" i "version" 2} :op :assert}))
                      (range) changed-ids)
        delta-timing
        (timed #(view/build-view-delta
                 {:view-id :feed :epoch 2 :changes changes :block-rows block-rows
                  :previous-bundle (get-in base [:bundle :cid])}))
        delta (:value delta-timing)
        generations [{:bundle (get-in delta [:bundle :node]) :pack-bytes (:pack-bytes delta)}
                     {:bundle (get-in base [:bundle :node]) :pack-bytes (:pack-bytes base)}]
        samples (mapv (fn [i]
                        (:ms (timed #(view/query-packed-chain
                                     generations {:lower (key-of i) :upper (key-of i)
                                                  :limit 1}))))
                      (take 500 changed-ids))
        sample-plan (:plan (view/query-packed-chain
                            generations {:lower (key-of (first changed-ids))
                                         :upper (key-of (first changed-ids)) :limit 1}))]
    {:base-rows n :changed-rows change-count :block-rows block-rows
     :delta-build-ms (:ms delta-timing)
     :delta-rows-per-sec (/ (* change-count 1000.0) (:ms delta-timing))
     :delta-pack-bytes (alength ^bytes (:pack-bytes delta))
     :delta-blocks (count (:blocks delta))
     :chain-point {:samples (count samples)
                   :p50-ms (percentile samples 0.50)
                   :p95-ms (percentile samples 0.95)
                   :p99-ms (percentile samples 0.99)
                   :estimated-requests (:estimated-requests sample-plan)
                   :estimated-bytes (:estimated-bytes sample-plan)}}))

(defn -main [& args]
  (prn (measure (parse-long (or (first args) "10000"))
                (parse-long (or (second args) "1000"))
                (parse-long (or (nth args 2 nil) "512"))))
  (shutdown-agents))
