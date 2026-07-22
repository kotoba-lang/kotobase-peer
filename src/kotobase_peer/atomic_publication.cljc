(ns kotobase-peer.atomic-publication
  "Atomic epoch root for base Merkle-LSM state and derived query artifacts."
  (:require [ipld.core :as ipld]))

(def format-version 1)

(defn- encoded [node]
  (let [bytes (ipld/encode node)]
    {:node node :bytes bytes :cid (ipld/cid bytes)}))

(defn- block-put [{:keys [cid bytes]}]
  {:effect/type :block/put :cid cid :bytes bytes})

(defn- head-cas [db-id expected next]
  {:effect/type :head/cas :db-id (str db-id) :expected expected :next next})

(defn- without-publication [effects]
  (vec (remove #(= :head/cas (:effect/type %)) effects)))

(defn- view-descriptor [base-cid epoch view]
  (let [bundle (get-in view [:bundle :node])
        bundle-cid (get-in view [:bundle :cid])
        source (some-> (get bundle "source-manifest") ipld/link-cid)]
    (when-not (= epoch (:epoch view))
      (throw (ex-info "Derived view epoch must equal base epoch"
                      {:base-epoch epoch :view-epoch (:epoch view)
                       :view-id (:view-id view)})))
    (when-not (= base-cid source)
      (throw (ex-info "Derived view must pin the base manifest"
                      {:base-manifest base-cid :source-manifest source
                       :view-id (:view-id view)})))
    [(:view-id view)
     (cond-> {"bundle" (ipld/link bundle-cid)
              "count" (:count view)}
       (:pack-cid view) (assoc "pack" (ipld/link (:pack-cid view))))]))

(defn build-plan
  "Combine an unexecuted Merkle-LSM BASE-PLAN, scoped STATISTICS, and built
  materialized VIEWS into one immutable epoch root and one final HeadCAS.
  Constituent HeadCAS effects are removed; all block/object puts precede the
  publication CAS. Query bundles must pin the exact base manifest CID."
  [{:keys [db-id expected base-plan statistics views previous-publication]}]
  (let [manifest (:manifest base-plan)
        base-cid (:cid manifest)
        epoch (:epoch manifest)
        views (vec views)
        base-heads (filterv #(= :head/cas (:effect/type %))
                            (:effects base-plan))]
    (when-not (and manifest (= epoch (get-in base-plan [:result :epoch])))
      (throw (ex-info "Atomic publication requires a valid base flush plan" {})))
    (when-not (and (= 1 (count base-heads))
                   (= (peek (:effects base-plan)) (first base-heads))
                   (= expected (:expected (first base-heads)))
                   (= base-cid (:next (first base-heads))))
      (throw (ex-info "Base plan must end in its one expected manifest HeadCAS"
                      {:expected expected :head-effects base-heads
                       :base-manifest base-cid})))
    (when-not (= epoch (:epoch statistics))
      (throw (ex-info "Statistics epoch must equal base epoch"
                      {:base-epoch epoch :statistics-epoch (:epoch statistics)})))
    (when-not (= (count views) (count (set (map :view-id views))))
      (throw (ex-info "Atomic publication view ids must be unique"
                      {:view-ids (mapv :view-id views)})))
    (let [view-directory (into (sorted-map)
                               (map #(view-descriptor base-cid epoch %)) views)
          statistics-block
          (encoded {"format" "kotobase/query-statistics"
                    "version" format-version
                    "db-id" (str db-id)
                    "epoch" epoch
                    "value" statistics})
          root-node
          (cond-> {"format" "kotobase/epoch-publication"
                   "version" format-version
                   "db-id" (str db-id)
                   "epoch" epoch
                   "base-manifest" (ipld/link base-cid)
                   "statistics" (ipld/link (:cid statistics-block))
                   "views" view-directory}
            previous-publication
            (assoc "previous-publication" (ipld/link previous-publication)))
          root (encoded root-node)
          effects (vec (concat (without-publication (:effects base-plan))
                               [(block-put statistics-block)]
                               (mapcat :effects views)
                               [(block-put root)
                                (head-cas db-id expected (:cid root))]))]
      {:result {:db-id (str db-id) :epoch epoch
                :publication (:cid root) :base-manifest base-cid
                :statistics (:cid statistics-block)
                :views (into {} (map (fn [[id descriptor]]
                                       [id (ipld/link-cid (get descriptor "bundle"))])
                                     view-directory))}
       :publication root
       :statistics statistics-block
       :effects effects})))

(defn publication-node? [node]
  (and (= "kotobase/epoch-publication" (get node "format"))
       (= format-version (get node "version"))))

(defn base-manifest-cid
  "Resolve the base manifest from an EpochPublication node. Direct version
  manifests remain readable during migration and return SELF-CID."
  [node self-cid]
  (cond
    (publication-node? node) (ipld/link-cid (get node "base-manifest"))
    (= "kotobase/version-manifest" (get node "format")) self-cid
    :else (throw (ex-info "Unknown database head format"
                          {:format (get node "format")}))))

(defn rebase-plan
  "Replace an EpochPublication's physical base manifest after semantics-
  preserving compaction. Statistics and derived view links remain pinned to
  the same logical epoch; the root block precedes one final HeadCAS."
  [{:keys [db-id expected publication-node base-manifest view-bundle-nodes]}]
  (let [manifest-node (:node base-manifest)
        manifest-cid (:cid base-manifest)]
    (when-not (publication-node? publication-node)
      (throw (ex-info "Rebase requires an EpochPublication" {})))
    (when-not (and (= "kotobase/version-manifest" (get manifest-node "format"))
                   (= (str db-id) (get publication-node "db-id"))
                   (= (str db-id) (get manifest-node "db-id"))
                   (= (get publication-node "epoch") (get manifest-node "epoch")))
      (throw (ex-info "Compacted base must preserve publication database and epoch"
                      {:db-id db-id
                       :publication-epoch (get publication-node "epoch")
                       :manifest-epoch (get manifest-node "epoch")})))
    (let [view-directory (get publication-node "views")
          bundle-ids (set (keys view-directory))]
      (when-not (= bundle-ids (set (keys view-bundle-nodes)))
        (throw (ex-info "Rebase requires every publication view bundle"
                        {:expected bundle-ids
                         :actual (set (keys view-bundle-nodes))})))
      (let [bundles
            (into (sorted-map)
                  (map (fn [[view-id bundle-node]]
                         (when-not (and (= "kotobase/query-bundle"
                                           (get bundle-node "format"))
                                        (= view-id (get bundle-node "view-id"))
                                        (= (get publication-node "epoch")
                                           (get bundle-node "epoch")))
                           (throw (ex-info "Invalid publication view bundle"
                                           {:view-id view-id})))
                         [view-id
                          (encoded (assoc bundle-node
                                          "source-manifest"
                                          (ipld/link manifest-cid)))]))
                  view-bundle-nodes)
            rebased-directory
            (into (sorted-map)
                  (map (fn [[view-id descriptor]]
                         [view-id (assoc descriptor "bundle"
                                         (ipld/link (get-in bundles
                                                            [view-id :cid])))]))
                  view-directory)
            root (encoded (assoc publication-node
                                 "base-manifest" (ipld/link manifest-cid)
                                 "views" rebased-directory))]
      {:result {:db-id (str db-id)
                :epoch (get publication-node "epoch")
                :publication (:cid root)
                :base-manifest manifest-cid}
       :publication root
       :view-bundles bundles
       :effects (vec (concat (map (comp block-put val) bundles)
                             [(block-put root)
                              (head-cas db-id expected (:cid root))]))}))))
