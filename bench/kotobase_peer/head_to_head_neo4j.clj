(ns kotobase-peer.head-to-head-neo4j
  "ADR-2607250100 axis 1: direct same-workload, same-JVM comparison vs
  Neo4j embedded (community, calendar-versioned). Sibling of
  head-to-head-datomic -- identical workload shape, identical kotobase
  side, so the three receipts compose into one comparable table.

  Neo4j side uses the embedded native API for the point lookup
  (findNodes by indexed property -- its fastest path, index awaited
  online before measuring) and Cypher for the scan. Storage is a temp
  on-disk directory (embedded Neo4j has no pure in-memory mode) -- that
  favors kotobase on writes and is stated in the receipt, not averaged
  away; reads run against the page cache after warmup.

  Usage: clojure -M:h2h-neo4j"
  (:require [kotobase-peer.core :as eng])
  (:import [javax.crypto Cipher Mac]
           [javax.crypto.spec SecretKeySpec GCMParameterSpec]
           [java.util Base64]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeUnit]
           [org.neo4j.dbms.api DatabaseManagementServiceBuilder]
           [org.neo4j.graphdb Label]))

;; ── same crypto + kotobase harness as head_to_head_datomic.clj ──────────────
(def ^:private test-dek (SecretKeySpec. (byte-array (range 1 33)) "AES"))
(def ^:private test-blind-key (SecretKeySpec. (byte-array (range 33 65)) "HmacSHA256"))
(def ^:private test-nonce-key (SecretKeySpec. (byte-array (range 65 97)) "HmacSHA256"))

(defn- test-encrypt-fn [^bytes plaintext]
  (let [mac (doto (Mac/getInstance "HmacSHA256") (.init test-nonce-key))
        nonce (byte-array (take 12 (.doFinal mac plaintext)))
        cipher (Cipher/getInstance "AES/GCM/NoPadding")]
    (.init cipher Cipher/ENCRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
    (byte-array (concat nonce (.doFinal cipher plaintext)))))

(defn- test-decrypt-fn [^bytes blob]
  (let [nonce (byte-array (take 12 blob))
        ct (byte-array (drop 12 blob))
        cipher (Cipher/getInstance "AES/GCM/NoPadding")]
    (.init cipher Cipher/DECRYPT_MODE test-dek (GCMParameterSpec. 128 nonce))
    (.doFinal cipher ct)))

(defn- test-blind-fn [component]
  (let [mac (doto (Mac/getInstance "HmacSHA256") (.init test-blind-key))]
    (.encodeToString (Base64/getEncoder) (.doFinal mac (.getBytes (pr-str component) "UTF-8")))))

(def entities 3000)
(def batches 3)
(def point-samples 200)
(def scan-samples 20)

(defn- elapsed-ms [f]
  (let [start (System/nanoTime) result (f)]
    {:ms (/ (- (System/nanoTime) start) 1e6) :result result}))

(defn- percentile [xs q]
  (let [s (vec (sort xs))]
    (nth s (min (dec (count s)) (long (Math/floor (* q (count s))))))))

(defn- stats [xs]
  {:p50-ms (percentile xs 0.50) :p95-ms (percentile xs 0.95)
   :mean-ms (/ (reduce + xs) (double (count xs)))})

(defn- run-kotobase []
  (let [store (atom {})
        put! (fn [cid bytes] (swap! store assoc cid bytes))
        get-fn (fn [cid] (get @store cid))
        everything (constantly true)
        batch-size (quot entities batches)
        tx (elapsed-ms
            (fn []
              (reduce (fn [chain b]
                        (eng/commit! put! get-fn
                                     (mapv (fn [i] {:s (str "e" i) :p ":bench/v" :o (str i)})
                                           (range (* b batch-size) (* (inc b) batch-size)))
                                     chain test-encrypt-fn))
                      nil (range batches))))
        chain (:result tx)
        folded (eng/fold! put! get-fn chain test-blind-fn test-encrypt-fn test-decrypt-fn)
        hydrate (elapsed-ms #(eng/hydrate-chain get-fn folded test-blind-fn test-decrypt-fn))
        db (:result hydrate)
        points (mapv (fn [i] (:ms (elapsed-ms
                                   #(eng/q db [nil ":bench/v" (str (mod (* i 37) entities))] everything))))
                     (range point-samples))
        scans (mapv (fn [_] (:ms (elapsed-ms #(count (eng/q db [nil ":bench/v" nil] everything)))))
                    (range scan-samples))]
    {:transact-total-ms (:ms tx)
     :hydrate-once-ms (:ms hydrate)
     :point-by-value (stats points)
     :full-scan (stats scans)}))

(defn- run-neo4j []
  (let [dir (Files/createTempDirectory "h2h-neo4j" (make-array FileAttribute 0))
        dbms (-> (DatabaseManagementServiceBuilder. dir) (.build))
        db (.database dbms "neo4j")
        label (Label/label "Bench")
        batch-size (quot entities batches)
        tx-total (elapsed-ms
                  (fn []
                    (doseq [b (range batches)]
                      (with-open [tx (.beginTx db)]
                        (doseq [i (range (* b batch-size) (* (inc b) batch-size))]
                          (doto (.createNode tx (into-array Label [label]))
                            (.setProperty "v" (str i))))
                        (.commit tx)))))
        _ (with-open [tx (.beginTx db)]
            (-> (.schema tx) (.indexFor label) (.on "v") (.create))
            (.commit tx))
        index-wait (elapsed-ms
                    (fn [] (with-open [tx (.beginTx db)]
                             (-> (.schema tx) (.awaitIndexesOnline 120 TimeUnit/SECONDS)))))
        points (mapv (fn [i]
                       (:ms (elapsed-ms
                             (fn []
                               (with-open [tx (.beginTx db)]
                                 (with-open [it (.findNodes tx label "v" (str (mod (* i 37) entities)))]
                                   (loop [n 0] (if (.hasNext it) (do (.next it) (recur (inc n))) n))))))))
                     (range point-samples))
        scans (mapv (fn [_]
                      (:ms (elapsed-ms
                            (fn []
                              (with-open [tx (.beginTx db)]
                                (with-open [rs (.execute tx "MATCH (n:Bench) RETURN n.v")]
                                  (loop [n 0] (if (.hasNext rs) (do (.next rs) (recur (inc n))) n))))))))
                    (range scan-samples))]
    (.shutdown dbms)
    {:transact-total-ms (:ms tx-total)
     :index-build-await-ms (:ms index-wait)
     :point-by-indexed-property (stats points)
     :full-scan-cypher (stats scans)}))

(defn -main [& _]
  (run-kotobase) (run-neo4j)
  (let [k (run-kotobase)
        n (run-neo4j)]
    (prn {:receipt/type :head-to-head-neo4j
          :workload {:entities entities :batches batches
                     :point-samples point-samples :scan-samples scan-samples
                     :value-type "string; Neo4j property index awaited online before point lookups"}
          :kotobase-peer k
          :neo4j-embedded n})
    (shutdown-agents)))
