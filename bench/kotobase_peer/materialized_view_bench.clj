(ns kotobase-peer.materialized-view-bench
  "Browser-without-local-disk benchmark for packed IPLD materialized views."
  (:require [kotobase-peer.materialized-view :as view]))

(defn- pad [i]
  (str (apply str (repeat (- 9 (count (str i))) "0")) i))

(defn- key-of [i] (str "tenant-a/" (pad i)))

(defn- entries [n]
  (mapv (fn [i]
          {:key (key-of i)
           :value {"id" i
                   "title" (str "Post " i)
                   "author" (str "person-" (mod i 10000))
                   "score" (mod (* i 2654435761) 100000)}})
        (range n)))

(defn- timed [f]
  (let [start (System/nanoTime)
        result (f)]
    {:value result :ms (/ (- (System/nanoTime) start) 1e6)}))

(defn- percentile [xs p]
  (let [xs (vec (sort xs))]
    (nth xs (min (dec (count xs))
                 (long (Math/floor (* p (dec (count xs)))))))))

(defn- stats [samples]
  {:samples (count samples)
   :p50-ms (percentile samples 0.50)
   :p95-ms (percentile samples 0.95)
   :p99-ms (percentile samples 0.99)
   :mean-ms (/ (reduce + samples) (double (count samples)))})

(defn- query-ms [bundle pack query]
  (:ms (timed #(view/query-packed bundle pack query))))

(defn measure [n block-rows]
  (let [source (entries n)
        built-timing (timed #(view/build-view {:view-id :post/recent-by-tenant
                                               :epoch 1
                                               :block-rows block-rows
                                               :sorted? true
                                               :entries source}))
        built (:value built-timing)
        bundle (get-in built [:bundle :node])
        pack (:pack-bytes built)
        ;; Deterministic keys span the entire view rather than measuring only
        ;; one hot block. Two warmups remove JVM class/JIT startup distortion.
        point-ids (mapv #(mod (* % 7919) n) (range 1002))
        _ (doseq [i (take 2 point-ids)]
            (view/query-packed bundle pack {:lower (key-of i)
                                            :upper (key-of i) :limit 1}))
        point-samples (mapv (fn [i]
                              (query-ms bundle pack {:lower (key-of i)
                                                     :upper (key-of i)
                                                     :limit 1}))
                            (drop 2 point-ids))
        range-starts (mapv #(mod (* % 1543) (- n 20)) (range 202))
        _ (doseq [i (take 2 range-starts)]
            (view/query-packed bundle pack {:lower (key-of i)
                                            :upper (key-of (+ i 19)) :limit 20}))
        range-samples (mapv (fn [i]
                              (query-ms bundle pack {:lower (key-of i)
                                                     :upper (key-of (+ i 19))
                                                     :limit 20}))
                            (drop 2 range-starts))
        scan-width (min 2000 (dec n))
        scan-starts (mapv #(mod (* % 1543) (- n scan-width)) (range 52))
        _ (doseq [i (take 2 scan-starts)]
            (view/query-packed bundle pack {:lower (key-of i)
                                            :upper (key-of (+ i scan-width))}))
        scan-samples (mapv (fn [i]
                             (query-ms bundle pack {:lower (key-of i)
                                                    :upper (key-of (+ i scan-width))}))
                           (drop 2 scan-starts))
        middle (quot n 2)
        sample-plan (:plan (view/query-packed bundle pack
                                              {:lower (key-of middle)
                                               :upper (key-of middle) :limit 1}))]
    {:datoms n
     :block-rows block-rows
     :build-ms (:ms built-timing)
     :build-rows-per-sec (/ (* n 1000.0) (:ms built-timing))
     :blocks (count (:blocks built))
     :pack-bytes (alength ^bytes pack)
     :bundle-bytes (alength ^bytes (get-in built [:bundle :bytes]))
     :point (assoc (stats point-samples)
                   :estimated-requests (:estimated-requests sample-plan)
                   :estimated-bytes (:estimated-bytes sample-plan))
     :range-20 (stats range-samples)
     :range-coalesced
     (let [i (first scan-starts)
           plan (:plan (view/query-packed bundle pack
                                          {:lower (key-of i)
                                           :upper (key-of (+ i scan-width))}))]
       (assoc (stats scan-samples)
              :rows (inc scan-width)
              :logical-blocks (count (:descriptors plan))
              :estimated-requests (:estimated-requests plan)
              :estimated-bytes (:estimated-bytes plan)))
     :correct? (= middle (get-in (view/query-packed
                                  bundle pack {:lower (key-of middle)
                                               :upper (key-of middle) :limit 1})
                                 [:values 0 "id"]))}))

(defn -main [& args]
  (let [n (parse-long (or (first args) "100000"))
        block-rows (parse-long (or (second args) "512"))
        result (measure n block-rows)]
    (prn result)
    (when-not (:correct? result)
      (throw (ex-info "Materialized view benchmark returned the wrong row" result)))
    (shutdown-agents)))
