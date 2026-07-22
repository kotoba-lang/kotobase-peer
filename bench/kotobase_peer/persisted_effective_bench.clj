(ns kotobase-peer.persisted-effective-bench
  (:require [kotobase-peer.core :as peer]))

(defn- percentile [samples p]
  (nth (vec (sort samples))
       (long (Math/floor (* p (dec (count samples)))))))

(defn- elapsed-ms [f]
  (let [start (System/nanoTime)]
    (f)
    (/ (- (System/nanoTime) start) 1e6)))

(defn measure [entities samples]
  (let [store (atom {})
        put! (fn [cid bytes] (swap! store assoc cid bytes))
        get-fn (fn [cid] (get @store cid))
        head (atom nil)
        cas-calls (atom 0)
        cas! (fn [_ expected new]
               (swap! cas-calls inc)
               (if (= @head expected) (do (reset! head new) new) @head))
        seed (mapv (fn [i] [(str "e" i) "role" "user"]) (range entities))
        _ (peer/commit-serialized-effective! put! get-fn cas! "bench" nil seed
                                             identity pr-str identity)
        blocks-before-no-op (count @store)
        calls-before-no-op @cas-calls
        no-op-times
        (mapv (fn [i]
                (elapsed-ms
                 #(peer/commit-serialized-effective!
                   put! get-fn cas! "bench" @head
                   [[(str "e" (mod i entities)) "role" "user"]]
                   identity pr-str identity)))
              (range samples))
        blocks-after-no-op (count @store)
        calls-after-no-op @cas-calls
        effective-times
        (mapv (fn [i]
                (elapsed-ms
                 #(peer/commit-serialized-effective!
                   put! get-fn cas! "bench" @head
                   [[(str "new" i) "role" "user"]]
                   identity pr-str identity)))
              (range samples))]
    {:environment {:runtime (System/getProperty "java.version")
                   :store :in-memory :crypto :identity}
     :entities entities :samples samples
     :no-op {:p50-ms (percentile no-op-times 0.50)
             :p95-ms (percentile no-op-times 0.95)
             :blocks-written (- blocks-after-no-op blocks-before-no-op)
             :cas-calls (- calls-after-no-op calls-before-no-op)}
     :effective {:p50-ms (percentile effective-times 0.50)
                 :p95-ms (percentile effective-times 0.95)
                 :commits samples}}))

(defn -main [& args]
  (prn (measure (parse-long (or (first args) "10000"))
                (parse-long (or (second args) "20"))))
  (shutdown-agents))
