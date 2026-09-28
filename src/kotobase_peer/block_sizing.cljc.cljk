(ns kotobase-peer.block-sizing
  "Pure, bounded controller for choosing the next Merkle run block size.

  The controller never jumps more than one declared size class per decision.
  It requires a qualified current cohort, applies explicit cost weights, and
  changes class only after a hysteresis margin. Hosts persist the returned
  decision in the same immutable manifest epoch as the selected size.")

(def size-classes [16384 32768 65536 131072])

(def default-policy
  {:minimum-samples 3
   :hysteresis-ratio 0.10
   :request-ms-equivalent 0.0
   :mib-ms-equivalent 0.0
   :cpu-weight 0.0
   :cache-miss-ms-equivalent 0.0})

(defn- finite-non-negative? [value]
  (and (number? value)
       #?(:clj (Double/isFinite (double value))
          :cljs (js/Number.isFinite value))
       (not (neg? value))))

(defn- valid-observation? [observation]
  (and (map? observation)
       (contains? (set size-classes) (:block-bytes observation))
       (pos-int? (:samples observation))
       (every? finite-non-negative?
               (map observation
                    [:wall-p95-ms :cpu-ms :fetched-blocks
                     :fetched-bytes :cache-hit-ratio]))
       (<= (:cache-hit-ratio observation) 1)))

(defn- normalized-policy [policy]
  (let [policy (merge default-policy policy)]
    (when-not
     (and (pos-int? (:minimum-samples policy))
          (finite-non-negative? (:hysteresis-ratio policy))
          (< (:hysteresis-ratio policy) 1)
          (every? finite-non-negative?
                  (map policy
                       [:request-ms-equivalent :mib-ms-equivalent
                        :cpu-weight :cache-miss-ms-equivalent])))
      (throw (ex-info "Invalid block sizing policy" {:policy policy})))
    policy))

(defn cohort-score
  "Return the explicit wall/resource score for one validated observation."
  [observation policy]
  (when-not (valid-observation? observation)
    (throw (ex-info "Invalid block sizing observation"
                    {:observation observation})))
  (let [{:keys [request-ms-equivalent mib-ms-equivalent cpu-weight
                cache-miss-ms-equivalent]}
        (normalized-policy policy)]
    (+ (:wall-p95-ms observation)
       (* cpu-weight (:cpu-ms observation))
       (* request-ms-equivalent (:fetched-blocks observation))
       (* mib-ms-equivalent (/ (:fetched-bytes observation) 1048576.0))
       (* cache-miss-ms-equivalent (- 1 (:cache-hit-ratio observation))))))

(defn- adjacent-classes [current]
  (let [index (first (keep-indexed
                      (fn [index size]
                        (when (= current size) index))
                      size-classes))]
    (set (subvec size-classes (max 0 (dec index))
                 (min (count size-classes) (+ index 2))))))

(defn select-next
  "Choose a next block size from qualified current/adjacent cohorts.

  INPUT is {:current bytes :observations [...] :policy {...}}. Duplicate
  cohorts, unknown classes, malformed metrics, and an invalid current class
  fail closed. Missing qualified evidence holds the current class. The result
  includes deterministic scores and the exact reason for audit persistence."
  [{:keys [current observations policy]}]
  (let [classes (set size-classes)
        policy (normalized-policy policy)
        observations (vec observations)
        observed-classes (mapv :block-bytes observations)]
    (when-not (contains? classes current)
      (throw (ex-info "Current block size is not an allowed class"
                      {:current current :size-classes size-classes})))
    (when-not (and (every? valid-observation? observations)
                   (= (count observed-classes)
                      (count (set observed-classes))))
      (throw (ex-info "Block sizing cohorts must be valid and unique"
                      {:observations observations})))
    (let [qualified
          (->> observations
               (filter #(<= (:minimum-samples policy) (:samples %)))
               (filter #(contains? (adjacent-classes current)
                                   (:block-bytes %)))
               (map (fn [observation]
                      [(:block-bytes observation)
                       (cohort-score observation policy)]))
               (into (sorted-map)))
          current-score (get qualified current)]
      (if-not current-score
        {:current current :selected current :changed? false
         :reason :insufficient-current-evidence
         :qualified-scores qualified :policy policy}
        (let [[best-size best-score]
              (first (sort-by (juxt second first) qualified))
              improvement (if (zero? current-score)
                            0.0
                            (/ (- current-score best-score) current-score))
              change? (and (not= current best-size)
                           (<= (:hysteresis-ratio policy) improvement))]
          {:current current
           :selected (if change? best-size current)
           :changed? change?
           :reason (cond
                     (= current best-size) :current-best
                     change? :hysteresis-passed
                     :else :hysteresis-held)
           :improvement-ratio improvement
           :qualified-scores qualified
           :policy policy})))))
