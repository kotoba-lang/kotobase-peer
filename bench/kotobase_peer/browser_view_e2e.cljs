(ns kotobase-peer.browser-view-e2e
  (:require [goog.object :as gobj]
            [ipld.core :as ipld]
            [kotobase-peer.materialized-view :as view]))

(defn- fetch-bytes [url options]
  (-> (js/fetch url options)
      (.then (fn [response]
               (when-not (.-ok response)
                 (throw (js/Error. (str url " returned " (.-status response)))))
               (.arrayBuffer response)))
      (.then #(js/Uint8Array. %))))

(defn- now [] (.now js/performance))

(defn- execute-query [config bundle-bytes result started bundle-start]
  (let [actual (str (ipld/cid bundle-bytes))]
    (when-not (= (gobj/get config "bundleCid") actual)
      (throw (js/Error. "query bundle CID mismatch")))
    (let [bundle (ipld/decode bundle-bytes)
          key (gobj/get config "queryKey")
          plan (view/range-query-plan
                {:bundle bundle :lower key :upper key :limit 1})
          effect (first (:need plan))
          range-header (str "bytes=" (:offset effect) "-"
                            (dec (+ (:offset effect) (:length effect))))
          range-start (now)]
      (-> (fetch-bytes "/e2e/object"
                       #js {:cache "force-cache"
                            :headers #js {"Range" range-header}})
          (.then
           (fn [range-bytes]
             (let [values (view/finish-range-query plan [range-bytes])
                   finished (now)
                   output {:bundle-ms (- range-start bundle-start)
                           :range-and-query-ms (- finished range-start)
                           :total-ms (- finished started)
                           :requests (:estimated-requests plan)
                           :range-bytes (:estimated-bytes plan)
                           :value (first values)
                           :verified? (= 500 (get (first values) "id"))}]
               (set! (.. result -dataset -status) "done")
               (set! (.-textContent result)
                     (.stringify js/JSON (clj->js output) nil 2)))))))))

(defn ^:export run []
  (let [result (.querySelector js/document "#result")
        started (now)
        config-promise (js/fetch "/e2e/config" #js {:cache "no-store"})
        decoded-config (.then config-promise (fn [response] (.json response)))
        query-promise
        (.then decoded-config
               (fn [config]
                 (let [bundle-start (now)]
                   (.then (fetch-bytes "/e2e/bundle" #js {:cache "force-cache"})
                          (fn [bundle-bytes]
                            (execute-query config bundle-bytes result started bundle-start))))))]
    (.catch query-promise
            (fn [error]
              (set! (.. result -dataset -status) "error")
              (set! (.-textContent result) (str error))
              (.error js/console error)))))
