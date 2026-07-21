(ns kotobase-peer.export-view-fixture
  (:require [kotobase-peer.materialized-view :as view])
  (:import [java.nio.file Files Paths OpenOption]))

(defn- pad [i] (str (apply str (repeat (- 9 (count (str i))) "0")) i))

(defn -main [& [directory]]
  (let [directory (or directory "/tmp/kotobase-view-e2e")
        entries (mapv (fn [i]
                        {:key (str "tenant-a/" (pad i))
                         :value {"id" i "title" (str "Post " i)}})
                      (range 1000))
        built (view/build-view {:view-id :browser/e2e :epoch 1
                                :entries entries :sorted? true :block-rows 100})
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
          :query-key (str "tenant-a/" (pad 500))})))
