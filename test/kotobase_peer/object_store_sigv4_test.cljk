(ns kotobase-peer.object-store-sigv4-test
  "What this worker actually signs.

  The object store's SigV4 path had no test at all while it carried its own
  copy of the signer — 2278 lines of object-store tests and not one assertion
  about an Authorization header. That is the shape of failure this whole
  consolidation is about: a signer nobody exercises drifts, and the only
  symptom is an opaque 403 from B2 at some later date.

  So rather than pin a signature string (which would just restate the library's
  own tests), these run the signed request back through `sigv4.verify` — the
  independent recomputation a real S3 endpoint performs. If canonicalization,
  the header set, or the payload hash were wrong, verification would fail here
  exactly as B2 would fail it in production. No clock injection needed: the
  check is self-consistent at whatever instant it runs."
  (:require [cljs.test :refer [deftest is testing async]]
            [kotobase-peer.object-store.worker :as worker]
            [sigv4.core :as v4]
            [sigv4.crypto :as crypto]
            [sigv4.verify :as verify]))

(def c (crypto/crypto))

(def config
  {:endpoint "https://s3.us-west-004.backblazeb2.com"
   :bucket "kotobase-blocks"
   :region "us-west-004"
   :access-key "004abcdef0123456789"
   :secret-key "K004sUpErSeCrEtAppLicAtionKey"})

(defn- verifies?
  "Recompute the signature the way the endpoint would. → Promise<boolean>."
  [signed key]
  (let [parsed (verify/parse-authorization (get-in signed [:headers "authorization"]))]
    (-> (verify/expected-signature
         c {:secret-key (:secret-key config)
            :parsed parsed
            :amz-date (get-in signed [:headers "x-amz-date"])
            :payload-hash (get-in signed [:headers "x-amz-content-sha256"])
            :request {:method (:method signed)
                      :path (v4/object-path (:bucket config) key)
                      :query nil
                      :headers (:headers signed)}})
        (.then #(verify/constant-time-eq? (:signature parsed) %)))))

(deftest a-signed-put-verifies
  (async done
    (let [key "blocks/bafy123"]
      (-> (worker/signed-s3-request config "PUT" key {:body "block bytes"})
          (.then (fn [signed]
                   (is (= "https://s3.us-west-004.backblazeb2.com/kotobase-blocks/blocks/bafy123"
                          (:url signed)))
                   (is (= "s3.us-west-004.backblazeb2.com" (get-in signed [:headers "host"])))
                   (testing "the payload hash covers the body, not the empty string"
                     (is (not= v4/empty-payload-sha256
                               (get-in signed [:headers "x-amz-content-sha256"]))))
                   (verifies? signed key)))
          (.then (fn [ok?] (is (true? ok?) "B2 must be able to verify what we sign") (done)))
          (.catch (fn [e] (is false (str e)) (done)))))))

(deftest a-signed-get-verifies
  (async done
    (let [key "blocks/bafy456"]
      (-> (worker/signed-s3-request config "GET" key {})
          (.then (fn [signed]
                   (is (= v4/empty-payload-sha256
                          (get-in signed [:headers "x-amz-content-sha256"])))
                   (verifies? signed key)))
          (.then (fn [ok?] (is (true? ok?)) (done)))
          (.catch (fn [e] (is false (str e)) (done)))))))

(deftest keys-with-reserved-and-non-ascii-characters-verify
  (testing "the copy this replaced used encodeURIComponent with a manual !'()*
            fixup and split keys without a trailing-empty limit; both are ways
            to sign a key that is not the key you are addressing"
    (async done
      (let [key "blocks/日本語 (1)!'()*.bin"]
        (-> (worker/signed-s3-request config "PUT" key {:body "x"})
            (.then #(verifies? % key))
            (.then (fn [ok?] (is (true? ok?)) (done)))
            (.catch (fn [e] (is false (str e)) (done))))))))

(deftest extra-headers-are-signed
  (async done
    (let [key "blocks/typed"]
      (-> (worker/signed-s3-request config "PUT" key
                                    {:body "x" :headers {"content-type" "application/octet-stream"}})
          (.then (fn [signed]
                   (is (re-find #"SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date"
                                (get-in signed [:headers "authorization"])))
                   (verifies? signed key)))
          (.then (fn [ok?] (is (true? ok?)) (done)))
          (.catch (fn [e] (is false (str e)) (done)))))))
