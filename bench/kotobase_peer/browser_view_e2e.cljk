(ns kotobase-peer.browser-view-e2e
  (:require [goog.object :as gobj]
            [ipld.core :as ipld]
            [kotobase-peer.materialized-view :as view]
            [kotobase-peer.statistics :as stats]))

(defn- now [] (.now js/performance))

(defn- bearer-token []
  (.get (js/URLSearchParams. (subs (.-hash js/location) 1)) "token"))

(defn- auth-headers
  ([] (auth-headers nil))
  ([headers]
   (let [token (bearer-token)
         result (or headers #js {})]
     (when token
       (gobj/set result "Authorization" (str "Bearer " token)))
     result)))

(defn- fetch-bytes [url options]
  (-> (js/fetch url options)
      (.then (fn [response]
               (when-not (.-ok response)
                 (throw (js/Error. (str url " returned " (.-status response)))))
               (.arrayBuffer response)))
      (.then #(js/Uint8Array. %))))

(defn- percentile [values p]
  (let [sorted (vec (sort values))]
    (nth sorted (js/Math.floor (* p (dec (count sorted)))))))

(defn- summary [values]
  {:samples (count values)
   :p50-ms (percentile values 0.50)
   :p95-ms (percentile values 0.95)
   :p99-ms (percentile values 0.99)
   :min-ms (apply min values)
   :max-ms (apply max values)})

(defn- range-header [{:keys [offset length]}]
  (str "bytes=" offset "-" (dec (+ offset length))))

(defn- decrypt-block [keyring descriptor ciphertext]
  (if-let [encryption (get descriptor "encryption")]
    (let [key-id (get encryption "key-id")
          crypto-key (get keyring key-id)]
      (when-not crypto-key
        (throw (js/Error. (str "missing view key " key-id))))
      (when-not (= "AES-256-GCM" (get encryption "algorithm"))
        (throw (js/Error. "unsupported view block encryption")))
      (-> (.decrypt (.-subtle js/crypto)
                    #js {:name "AES-GCM"
                         :iv (get encryption "nonce")
                         :tagLength 128}
                    crypto-key ciphertext)
          (.then #(js/Uint8Array. %))))
    (js/Promise.resolve ciphertext)))

(defn- decrypt-ranges [keyring plan range-arrays]
  (let [promises
        (mapcat
         (fn [fetch range-bytes]
           (map (fn [descriptor]
                  (let [start (- (get descriptor "offset") (:offset fetch))
                        ciphertext (.slice range-bytes start
                                           (+ start (get descriptor "length")))]
                    (decrypt-block keyring descriptor ciphertext)))
                (:descriptors fetch)))
         (:fetches plan) range-arrays)]
    (-> (js/Promise.all (clj->js (vec promises)))
        (.then #(vec (array-seq %))))))

(defn- query-once [bundle keyring
                   {:keys [lower upper limit expected-count cache nonce]}]
  (let [plan (view/range-query-plan
              {:bundle bundle :lower lower :upper upper :limit limit})
        started (now)
        requests
        (mapv (fn [effect]
                (fetch-bytes (if (= cache "reload")
                               (str "/e2e/object?sample=" nonce)
                               "/e2e/object")
                             #js {:cache cache
                                  :headers (auth-headers
                                            #js {"Range" (range-header effect)})}))
              (:need plan))]
    (-> (js/Promise.all (clj->js requests))
        (.then
         (fn [range-arrays]
           (decrypt-ranges keyring plan (vec (array-seq range-arrays)))))
        (.then
         (fn [plaintext-blocks]
           (let [values (view/finish-logical-blocks-query plan plaintext-blocks)
                 elapsed (- (now) started)]
             (when-not (= expected-count (count values))
               (throw (js/Error. (str "expected " expected-count
                                      " rows, got " (count values)))))
             {:ms elapsed
              :requests (:estimated-requests plan)
              :range-bytes (:estimated-bytes plan)
              :count (count values)
              :values values
              :first (first values)
              :last (last values)}))))))

(defn- batch-query-once [bundle keyring keys sample]
  (let [plan (view/batch-point-query-plan {:bundle bundle :keys keys})
        started (now)
        requests
        (mapv (fn [effect]
                (fetch-bytes (str "/e2e/object?sample=batch-" sample "-" (now))
                             #js {:cache "reload"
                                  :headers (auth-headers
                                            #js {"Range" (range-header effect)})}))
              (:need plan))]
    (-> (js/Promise.all (clj->js requests))
        (.then #(decrypt-ranges keyring plan (vec (array-seq %))))
        (.then
         (fn [plaintext-blocks]
           (let [values (view/finish-logical-blocks-batch-query plan plaintext-blocks)]
             (when-not (= (count keys) (count values))
               (throw (js/Error. (str "batch lookup expected " (count keys)
                                      " values, got " (count values)))))
             {:ms (- (now) started)
              :requests (:estimated-requests plan)
              :range-bytes (:estimated-bytes plan)
              :values values}))))))

(defn- join-once [bundle keyring sample]
  (let [started (now)
        outer-query {:lower "tenant-a/post-author/000000450"
                     :upper "tenant-a/post-author/000000469"
                     :limit 20 :expected-count 20 :cache "reload"
                     :nonce (str "join-outer-" sample "-" (now))}]
    (-> (query-once bundle keyring outer-query)
        (.then
         (fn [outer]
           (let [edges (:values outer)
                 author-ids (vec (sort (distinct (map #(get % "author-id") edges))))
                 author-key #(str "tenant-a/author/" (.padStart (str %) 9 "0"))
                 author-keys (mapv author-key author-ids)]
             (.then
              (batch-query-once bundle keyring author-keys sample)
              (fn [inner]
                (let [authors (into {} (map (fn [[_ author]]
                                              [(get author "id") author])
                                            (:values inner)))
                      post-keys (mapv #(str "tenant-a/"
                                            (.padStart (str (get % "post-id")) 9 "0"))
                                      edges)]
                  (.then
                   (batch-query-once bundle keyring post-keys (str sample "-posts"))
                   (fn [posts]
                     (let [joined (mapv (fn [edge]
                                          (let [post-id (get edge "post-id")]
                                            {:post (get (:values posts)
                                                        (str "tenant-a/"
                                                             (.padStart (str post-id) 9 "0")))
                                             :author (get authors (get edge "author-id"))}))
                                        edges)]
                       {:ms (- (now) started)
                        :rows (count joined)
                        :requests (+ (:requests outer) (:requests inner) (:requests posts))
                        :range-bytes (+ (:range-bytes outer) (:range-bytes inner)
                                        (:range-bytes posts))
                        :deduplicated-keys (+ (count author-ids) (count post-keys))
                        :first (first joined)}))))))))))))

(defn- join-samples [bundle keyring count]
  (reduce
   (fn [promise sample]
     (.then promise
            (fn [results]
              (.then (join-once bundle keyring sample)
                     #(conj results %)))))
   (js/Promise.resolve [])
   (range count)))

(defn- sequential-samples [bundle keyring query count cache label]
  (reduce
   (fn [promise i]
     (.then promise
            (fn [results]
              (.then (query-once bundle keyring
                                 (assoc query :cache cache
                                              :nonce (str label "-" i "-" (now))))
                     (fn [result] (conj results result))))))
   (js/Promise.resolve [])
   (range count)))

(defn- concurrent-samples [bundle keyring query batches concurrency]
  (reduce
   (fn [promise batch]
     (.then
      promise
      (fn [results]
        (let [started (now)
              requests (mapv #(query-once bundle keyring
                                          (assoc query :cache "reload"
                                                       :nonce (str "c-" batch "-" % "-" (now))))
                             (range concurrency))]
          (.then (js/Promise.all (clj->js requests))
                 (fn [batch-results]
                   (conj results
                         {:wall-ms (- (now) started)
                          :queries (vec (array-seq batch-results))})))))))
   (js/Promise.resolve [])
   (range batches)))

(defn- execute-harness [config bundle-bytes keyring result started]
  (let [actual (str (ipld/cid bundle-bytes))
        expected-cid (gobj/get config "bundleCid")]
    (when-not (= expected-cid actual)
      (throw (js/Error. "query bundle CID mismatch")))
    (let [bundle (ipld/decode bundle-bytes)
          point-key (gobj/get config "queryKey")
          point {:lower point-key :upper point-key :limit 1 :expected-count 1}
          range-query {:lower "tenant-a/000000450"
                       :upper "tenant-a/000000649"
                       :limit 200 :expected-count 200}]
      (-> (sequential-samples bundle keyring point 10 "reload" "point-cold")
          (.then
           (fn [point-cold]
             (.then (sequential-samples bundle keyring point 20 "force-cache" "point-warm")
                    (fn [point-warm] {:point-cold point-cold
                                      :point-warm point-warm}))))
          (.then
           (fn [output]
             (.then (sequential-samples bundle keyring range-query 10 "reload" "range-cold")
                    #(assoc output :range-cold %))))
          (.then
           (fn [output]
             (.then (concurrent-samples bundle keyring point 5 8)
                    #(assoc output :concurrent %))))
          (.then
           (fn [output]
             (.then (join-samples bundle keyring 10)
                    #(assoc output :joins %))))
          (.then
           (fn [{:keys [point-cold point-warm range-cold concurrent joins]}]
             (let [point-template (first point-cold)
                   range-template (first range-cold)
                   concurrency-latencies (mapcat #(map :ms (:queries %)) concurrent)
                   output {:bundle-cid expected-cid
                           :point {:cold (summary (map :ms point-cold))
                                   :warm (summary (map :ms point-warm))
                                   :requests (:requests point-template)
                                   :range-bytes (:range-bytes point-template)
                                   :value (:first point-template)}
                           :range {:cold (summary (map :ms range-cold))
                                   :requests (:requests range-template)
                                   :range-bytes (:range-bytes range-template)
                                   :rows (:count range-template)
                                   :first (:first range-template)
                                   :last (:last range-template)}
                           :concurrent {:batches 5 :concurrency 8
                                        :latency (summary concurrency-latencies)
                                        :batch-wall (summary (map :wall-ms concurrent))}
                           :join {:kind "materialized-statistics-three-clause"
                                  :statistics-scope (get-in bundle ["query-statistics"
                                                                    "visibility-scope"])
                                  :statistics-epoch (get-in bundle ["query-statistics" "epoch"])
                                  :order (mapv :id
                                               (stats/plan-clause-order
                                                (mapv (fn [statistic]
                                                        (let [attribute (get-in statistic ["pattern" 1])]
                                                          {:id (get {"post/title" :posts
                                                                     "author/name" :authors
                                                                     "post-author/window" :edges}
                                                                    attribute)
                                                           :estimated-rows (get statistic "rows")
                                                           :vars (get {"post/title" #{'?post}
                                                                      "author/name" #{'?author}
                                                                      "post-author/window" #{'?post '?author}}
                                                                     attribute)}))
                                                      (get-in bundle ["query-statistics" "clauses"]))))
                                  :cold (summary (map :ms joins))
                                  :rows (:rows (first joins))
                                  :deduplicated-keys (:deduplicated-keys (first joins))
                                  :requests (:requests (first joins))
                                  :range-bytes (:range-bytes (first joins))
                                  :first (:first (first joins))}
                           :encryption "AES-256-GCM"
                           :key-ids (vec (sort (keys keyring)))
                           :total-harness-ms (- (now) started)
                           :verified? true}]
               (set! (.. result -dataset -status) "done")
               (set! (.-textContent result)
                     (.stringify js/JSON (clj->js output) nil 2)))))))))

(defn- import-dek [key-b64]
  (let [binary (js/atob key-b64)
        bytes (js/Uint8Array. (count binary))]
    (dotimes [i (count binary)]
      (aset bytes i (.charCodeAt binary i)))
    (.importKey (.-subtle js/crypto) "raw" bytes #js {:name "AES-GCM"}
                false #js ["decrypt"])))

(defn- load-keyring [bundle-bytes]
  (let [bundle (ipld/decode bundle-bytes)
        key-ids (->> (get bundle "blocks")
                     (keep #(get-in % ["encryption" "key-id"]))
                     distinct vec)
        promises
        (mapv (fn [key-id]
                (-> (js/fetch (str "/e2e/key?keyId=" (js/encodeURIComponent key-id))
                              #js {:cache "no-store" :headers (auth-headers)})
                    (.then (fn [response]
                             (when-not (.-ok response)
                               (throw (js/Error. (str "key unavailable: " key-id))))
                             (.json response)))
                    (.then (fn [payload]
                             (.then (import-dek (gobj/get payload "key"))
                                    (fn [crypto-key] [key-id crypto-key]))))))
              key-ids)]
    (-> (js/Promise.all (clj->js promises))
        (.then #(into {} (array-seq %))))))

(defn- run-authorized []
  (let [result (.querySelector js/document "#result")
        started (now)
        config-promise (js/fetch "/e2e/config"
                                 #js {:cache "no-store"
                                      :headers (auth-headers)})
        decoded-config (.then config-promise (fn [response] (.json response)))
        query-promise
        (.then decoded-config
               (fn [config]
                 (let [bundle-promise
                       (fetch-bytes "/e2e/bundle"
                                    #js {:cache "reload" :headers (auth-headers)})
                       execution
                       (.then bundle-promise
                              (fn [bundle-bytes]
                                (.then (load-keyring bundle-bytes)
                                       (fn [keyring]
                                         (execute-harness config bundle-bytes keyring
                                                          result started)))))]
                   execution)))]
    (.catch query-promise
            (fn [error]
              (set! (.. result -dataset -status) "error")
              (set! (.-textContent result) (str error))
              (.error js/console error)))))

(defn ^:export run []
  (if (bearer-token)
    (run-authorized)
    (let [result (.querySelector js/document "#result")]
      (set! (.. result -dataset -status) "awaiting-token")
      (set! (.-textContent result) "awaiting bearer capability")
      (.addEventListener js/window "hashchange" (fn [_] (run-authorized))
                         #js {:once true}))))
