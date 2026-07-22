(ns kotobase-peer.export-view-fixture
  (:require [kotobase-peer.materialized-view :as view])
  (:import [java.nio.file Files Paths OpenOption]
           [java.util Base64]
           [javax.crypto Cipher Mac]
           [javax.crypto.spec GCMParameterSpec SecretKeySpec]))

(defn- pad [i] (str (apply str (repeat (- 9 (count (str i))) "0")) i))

(def ^:private fixture-key (byte-array (map unchecked-byte (range 1 33))))

(defn- aes-gcm-encrypt [{:keys [plaintext]}]
  (let [key (SecretKeySpec. fixture-key "AES")
        mac (Mac/getInstance "HmacSHA256")
        _ (.init mac (SecretKeySpec. fixture-key "HmacSHA256"))
        nonce (byte-array (take 12 (.doFinal mac ^bytes plaintext)))
        cipher (Cipher/getInstance "AES/GCM/NoPadding")]
    (.init cipher Cipher/ENCRYPT_MODE key (GCMParameterSpec. 128 nonce))
    {:bytes (.doFinal cipher ^bytes plaintext)
     :algorithm "AES-256-GCM"
     :nonce nonce}))

(defn -main [& [directory mode]]
  (let [directory (or directory "/tmp/kotobase-view-e2e")
        encrypted? (= mode "encrypted")
        entries (mapv (fn [i]
                        {:key (str "tenant-a/" (pad i))
                         :value {"id" i "title" (str "Post " i)}})
                      (range 1000))
        built (view/build-view (cond-> {:view-id :browser/e2e :epoch 1
                                        :entries entries :sorted? true :block-rows 100}
                                 encrypted? (assoc :key-id "tenant-a/dek-v1"
                                                   :encrypt-block-fn aes-gcm-encrypt)))
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
          :key-id (when encrypted? "tenant-a/dek-v1")
          :key-b64 (when encrypted?
                     (.encodeToString (Base64/getEncoder) fixture-key))})))
