(ns kotobase-peer.head-to-head-datomic
  "ADR-2607250100 axis 1: the first DIRECT same-workload, same-JVM,
  same-machine comparison against a named peer database -- Datomic Pro
  (free since 2023), in-memory peer. Everything measured here is one
  process, one run, no published-number cross-citation.

  Workload (identical shape both sides, 1 datom per entity):
    - transact 3 batches x 1000 entities, value attribute only
    - point lookup BY VALUE (both engines resolve via their value index)
    - full attribute scan

  Honest asymmetries, stated up front rather than averaged away:
    - kotobase-peer encrypts every value (AES-256-GCM) and blinds every
      key (HMAC-SHA256) on write, and decrypts on hydrate -- that cost is
      the PRODUCT (zero-trust storage), not harness overhead; Datomic
      stores plaintext in-memory.
    - kotobase stores values as strings (wire representation); Datomic
      stores longs with a typed AVET index.
    - Datomic's transactor path here is the in-memory dev protocol -- its
      durable-storage write path would be slower; kotobase's numbers are
      its real engine write path (content-addressed blocks in an atom).
  Usage: clojure -M:h2h  (writes nothing; prints receipt EDN to stdout)"
  (:require [datomic.api :as d]
            [kotobase-peer.core :as eng])
  (:import [javax.crypto Cipher Mac]
           [javax.crypto.spec SecretKeySpec GCMParameterSpec]
           [java.util Base64]))

;; ── same real JVM crypto construction as load_test.clj / core_test.cljc ─────
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

;; ── harness ─────────────────────────────────────────────────────────────────
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

(defn- run-datomic []
  (let [uri (str "datomic:mem://h2h-" (System/nanoTime))
        _ (d/create-database uri)
        conn (d/connect uri)
        _ @(d/transact conn [{:db/ident :bench/v :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one :db/index true}])
        batch-size (quot entities batches)
        tx (elapsed-ms
            (fn []
              (doseq [b (range batches)]
                @(d/transact conn (mapv (fn [i] {:db/id (d/tempid :db.part/user)
                                                 :bench/v (str i)})
                                        (range (* b batch-size) (* (inc b) batch-size)))))))
        db (d/db conn)
        points (mapv (fn [i] (:ms (elapsed-ms
                                   #(d/q '[:find ?e :in $ ?v :where [?e :bench/v ?v]]
                                         db (str (mod (* i 37) entities))))))
                     (range point-samples))
        scans (mapv (fn [_] (:ms (elapsed-ms
                                  #(count (d/q '[:find ?e ?v :where [?e :bench/v ?v]] db)))))
                    (range scan-samples))]
    (d/delete-database uri)
    {:transact-total-ms (:ms tx)
     :point-by-value (stats points)
     :full-scan (stats scans)}))

(defn -main [& _]
  ;; warm both code paths once so JIT warmup isn't billed to either side
  (run-kotobase) (run-datomic)
  (let [k (run-kotobase)
        dtm (run-datomic)]
    (prn {:receipt/type :head-to-head
          :workload {:entities entities :batches batches
                     :point-samples point-samples :scan-samples scan-samples
                     :value-type "string (both sides; :db/index true on the Datomic attr)"}
          :kotobase-peer k
          :datomic-pro-in-mem dtm})
    (shutdown-agents)))
