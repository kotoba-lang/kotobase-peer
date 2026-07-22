(ns kotobase-peer.export-view-fixture
  (:require [clojure.string :as str]
            [kotobase-peer.materialized-view :as view])
  (:import [java.nio.file Files Paths OpenOption]
           [java.util Base64]
           [javax.crypto Cipher Mac]
           [javax.crypto.spec GCMParameterSpec SecretKeySpec]))

(defn- pad [i] (str (apply str (repeat (- 9 (count (str i))) "0")) i))

(def ^:private fixture-keys
  {"tenant-a/dek-v1" (byte-array (map unchecked-byte (range 1 33)))
   "tenant-a/dek-v2" (byte-array (map unchecked-byte (range 33 65)))})

(defn- aes-gcm-encrypt [fixture-key]
  (fn [{:keys [plaintext]}]
  (let [key (SecretKeySpec. fixture-key "AES")
        mac (Mac/getInstance "HmacSHA256")
        _ (.init mac (SecretKeySpec. fixture-key "HmacSHA256"))
        nonce (byte-array (take 12 (.doFinal mac ^bytes plaintext)))
        cipher (Cipher/getInstance "AES/GCM/NoPadding")]
    (.init cipher Cipher/ENCRYPT_MODE key (GCMParameterSpec. 128 nonce))
    {:bytes (.doFinal cipher ^bytes plaintext)
     :algorithm "AES-256-GCM"
     :nonce nonce})))

(defn -main [& [directory mode]]
  (let [directory (or directory "/tmp/kotobase-view-e2e")
        encrypted? (and mode (str/starts-with? mode "encrypted"))
        key-id (when encrypted?
                 (if (= mode "encrypted-v2") "tenant-a/dek-v2" "tenant-a/dek-v1"))
        fixture-key (get fixture-keys key-id)
        entries (mapv (fn [i]
                        {:key (str "tenant-a/" (pad i))
                         :value {"id" i "title" (str "Post " i)}})
                      (range 1000))
        built (view/build-view (cond-> {:view-id :browser/e2e :epoch 1
                                        :entries entries :sorted? true :block-rows 100}
                                 encrypted? (assoc :key-id key-id
                                                   :encrypt-block-fn
                                                   (aes-gcm-encrypt fixture-key))))
        dir (Paths/get directory (make-array String 0))]
    (Files/createDirectories dir (make-array java.nio.file.attribute.FileAttribute 0))
    (Files/write (.resolve dir "view-pack.bin") ^bytes (:pack-bytes built)
                 (make-array OpenOption 0))
    (Files/write (.resolve dir "query-bundle.cbor") ^bytes (get-in built [:bundle :bytes])
                 (make-array OpenOption 0))
    (prn {:directory directory
          :pack-cid (str (:pack-cid built))
          :pack-bytes (alength ^bytes (:pack-bytes built))
          :bundle-cid (str (get-in built [:bundle :cid]))
          :bundle-bytes (alength ^bytes (get-in built [:bundle :bytes]))
          :query-key (str "tenant-a/" (pad 500))
          :encrypted? encrypted?
          :key-id key-id
          :key-b64 (when encrypted?
                     (.encodeToString (Base64/getEncoder) fixture-key))})))
