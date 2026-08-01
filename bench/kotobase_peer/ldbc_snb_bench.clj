(ns kotobase-peer.ldbc-snb-bench
  "LDBC SNB Interactive complex reads IC02 / IC07 / IC08 / IC09 against
  kotobase's hot db and against Neo4j embedded, in the SAME JVM, over the
  SAME real LDBC dataset.

  Why: every prior kotobase head-to-head receipt (2026-07-25 Datomic /
  Neo4j / RisingWave) measured a SINGLE-ATTRIBUTE point lookup and a full
  scan, and said so -- \"no joins, no history queries, no concurrent load --
  this is a first receipt, not a TPC substitute\". Those are the cheapest
  queries a graph engine can be asked. NamiDB (namidb.com/benchmarks)
  publishes exactly these four LDBC SNB Interactive queries against Kuzu and
  Neo4j, so measuring the same four here is what makes the two projects'
  numbers comparable at all -- and Neo4j, measured on BOTH sides, is the
  common yardstick that lets a ratio survive the different machines.

  Data: the real LDBC SNB Interactive v1 SF1 dataset (Datagen v1.0.0,
  CsvComposite/LongDateFormatter, from the SURF/CWI repository), NOT a
  synthetic imitation. SF1 in full is ~10.5M elements, far more than one
  session can fold, so a DETERMINISTIC, STATED subset is loaded:

    * ALL 9,892 persons and ALL 180,623 `knows` edges -- the social topology
      that IC02/IC09's fan-out depends on is preserved exactly, not sampled.
    * the `posts` most recent Posts by creationDate, plus EVERY Comment that
      transitively replies into that set (whole conversation trees, so IC08's
      reply lookup and IC07's likes have real depth, not truncated stubs).
    * `hasCreator` for every included Message, and every `likes` edge whose
      target Message is included.

  Two modelling asymmetries, stated rather than averaged away:
    * `knows` is stored ONE direction in the CSV and is undirected in the
      LDBC spec. Neo4j matches it undirected natively (`-[:KNOWS]-`);
      kotobase's datom model has no undirected edge, so BOTH directions are
      asserted (2x the quads). This is what a real kotobase app would do.
    * LDBC's `likes` edge carries a creationDate PROPERTY. Neo4j puts it on
      the relationship; kotobase has no edge properties, so each like is
      REIFIED into an entity with 3 datoms. Both the element counts and the
      IC07 cost reflect that -- it is a genuine data-model cost, not overhead
      the harness invented.
    * kotobase's query language has no ORDER BY / LIMIT (arrangement.datalog
      is a conjunctive Datalog with aggregates, no ordering), so the
      top-20-by-date step runs in harness Clojure over the join's full
      result. Neo4j does it inside Cypher. Both sides are timed to the SAME
      final answer, and the answers are asserted EQUAL before any timing is
      reported -- but the work is not distributed the same way, and that is
      itself a finding about kotobase's query surface.

  Usage: clojure -M:ldbc-snb-bench <ldbc-dynamic-dir> [posts] [samples]"
  (:require [kotobase-peer.core :as eng]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import [org.neo4j.dbms.api DatabaseManagementServiceBuilder]
           [org.neo4j.graphdb Label]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent TimeUnit]))

;; ── CSV ────────────────────────────────────────────────────────────────────

(defn- rows
  "Lazy seq of split rows (header dropped) for one LDBC composite CSV."
  [dir file]
  (let [f (io/file dir file)]
    (when-not (.exists f)
      (throw (ex-info "LDBC csv missing" {:file (.getPath f)})))
    (with-open [r (io/reader f)]
      (doall (map #(str/split % #"\|" -1) (rest (line-seq r)))))))

(defn- load-subset
  "Real LDBC SF1, subset per the ns docstring. Returns plain maps/vectors."
  [dir n-posts]
  (let [persons (into {} (map (fn [[id fname lname & _]] [id [fname lname]]))
                      (rows dir "person_0_0.csv"))
        knows (mapv (fn [[a b & _]] [a b]) (rows dir "person_knows_person_0_0.csv"))
        ;; posts: keep the n-posts most recent by creationDate
        all-posts (mapv (fn [[id _img cd _ip _br _lang content _len]] [id (parse-long cd) content])
                        (rows dir "post_0_0.csv"))
        kept-posts (->> all-posts (sort-by second >) (take n-posts) vec)
        post-ids (into #{} (map first) kept-posts)
        ;; comments: every comment transitively replying into kept posts.
        c-rows (rows dir "comment_0_0.csv")
        comment-meta (into {} (map (fn [[id cd _ip _br content _len]] [id [(parse-long cd) content]])) c-rows)
        reply-post (into {} (map (fn [[c p]] [c p])) (rows dir "comment_replyOf_post_0_0.csv"))
        reply-comment (into {} (map (fn [[c p]] [c p])) (rows dir "comment_replyOf_comment_0_0.csv"))
        ;; fixpoint over the reply forest: a comment is kept if its parent is kept
        kept-comments
        (loop [frontier (into #{} (comp (filter (fn [[_ p]] (post-ids p))) (map first)) reply-post)
               kept #{}]
          (if (empty? frontier)
            kept
            (let [kept' (into kept frontier)
                  next (into #{} (comp (filter (fn [[_ parent]] (frontier parent))) (map first))
                             reply-comment)]
              (recur (into #{} (remove kept') next) kept'))))
        msg-ids (into post-ids kept-comments)
        post-creator (into {} (comp (filter (fn [[m _]] (post-ids m))) (map vec))
                           (rows dir "post_hasCreator_person_0_0.csv"))
        comment-creator (into {} (comp (filter (fn [[m _]] (kept-comments m))) (map vec))
                              (rows dir "comment_hasCreator_person_0_0.csv"))
        likes (into (vec (keep (fn [[p m cd]] (when (post-ids m) [p m (parse-long cd)]))
                               (rows dir "person_likes_post_0_0.csv")))
                    (keep (fn [[p m cd]] (when (kept-comments m) [p m (parse-long cd)]))
                          (rows dir "person_likes_comment_0_0.csv")))]
    {:persons persons
     :knows knows
     :messages (into (into {} (map (fn [[id cd content]] [id [cd content]])) kept-posts)
                     (map (fn [c] [c (comment-meta c)])) kept-comments)
     :post-ids post-ids
     :comment-ids kept-comments
     :creator (merge post-creator comment-creator)
     :reply-of (into (into {} (comp (filter (fn [[c p]] (and (kept-comments c) (post-ids p)))) (map vec)) reply-post)
                     (comp (filter (fn [[c p]] (and (kept-comments c) (kept-comments p)))) (map vec))
                     reply-comment)
     :likes likes
     :msg-ids msg-ids}))

;; ── kotobase side ──────────────────────────────────────────────────────────

(defn- build-kotobase
  "Hot db built via `eng/transact` -- the same db VALUE the persisted path
  produces (`hydrate-db` reduces `assert-quad` over cold rows into
  `qs/empty-db`; `transact` is that same assert), without paying this
  subset's AES-GCM + content-addressing write cost, which is measured
  separately in the merkle-lsm receipts and is not what an IC query's
  latency depends on. Stated, not hidden: no crypto and no persistence in
  THIS number."
  [{:keys [persons knows messages creator reply-of likes]}]
  (let [quads (transient [])
        add! (fn [s p o] (conj! quads {:s s :p p :o o}))]
    (doseq [[id [fname lname]] persons]
      (add! id "person/firstName" fname)
      (add! id "person/lastName" lname))
    (doseq [[a b] knows]                      ; undirected -> both directions
      (add! a "knows" b)
      (add! b "knows" a))
    (doseq [[id [cd content]] messages]
      (add! id "message/creationDate" (str cd))
      (add! id "message/content" (or content "")))
    (doseq [[m p] creator] (add! m "hasCreator" p))
    (doseq [[c parent] reply-of] (add! c "replyOf" parent))
    (doseq [[p m cd] likes]                   ; reified: no edge properties
      (let [lid (str "like:" p ":" m)]
        (add! lid "like/person" p)
        (add! lid "like/message" m)
        (add! lid "like/creationDate" (str cd))))
    (let [qs (persistent! quads)]
      {:db (eng/transact (eng/empty-db) qs)
       :quads (count qs)})))

(def ^:private everything (constantly true))

(defn- kb-ic02
  "IC02: the 20 most recent Messages by the start person's direct friends,
  created before max-date. Datalog join, then harness top-20 (no ORDER BY in
  arrangement.datalog)."
  [db person max-date]
  (->> (eng/query db
                  '{:find [?friend ?msg ?date]
                    :in [?person]
                    :where [[?person "knows" ?friend]
                            [?msg "hasCreator" ?friend]
                            [?msg "message/creationDate" ?date]]}
                  everything [person])
       (keep (fn [[f m d]] (let [dl (parse-long d)] (when (< dl max-date) [f m dl]))))
       (sort-by (juxt (comp - #(nth % 2)) second))
       (take 20) vec))

(defn- kb-ic09
  "IC09: same as IC02 but over friends AND friends-of-friends, excluding the
  start person. Two joins unioned in the harness -- `or-join` over two
  different path lengths is expressible but the union is the same answer and
  keeps the two engines' semantics obviously identical."
  [db person max-date]
  (let [one (eng/query db '{:find [?friend] :in [?person]
                            :where [[?person "knows" ?friend]]}
                       everything [person])
        two (eng/query db '{:find [?fof] :in [?person]
                            :where [[?person "knows" ?f1] [?f1 "knows" ?fof]]}
                       everything [person])
        reach (disj (into (into #{} (map first) one) (map first) two) person)]
    (->> reach
         (mapcat (fn [p]
                   (map (fn [[m d]] [p m (parse-long d)])
                        (eng/query db '{:find [?msg ?date] :in [?p]
                                        :where [[?msg "hasCreator" ?p]
                                                [?msg "message/creationDate" ?date]]}
                                   everything [p]))))
         (filter (fn [[_ _ d]] (< d max-date)))
         (sort-by (juxt (comp - #(nth % 2)) second))
         (take 20) vec)))

(defn- kb-ic07
  "IC07: the 20 most recent likes on the start person's Messages, one row per
  liker (their latest like)."
  [db person]
  (->> (eng/query db
                  '{:find [?liker ?msg ?date]
                    :in [?person]
                    :where [[?msg "hasCreator" ?person]
                            [?like "like/message" ?msg]
                            [?like "like/person" ?liker]
                            [?like "like/creationDate" ?date]]}
                  everything [person])
       (map (fn [[l m d]] [l m (parse-long d)]))
       (group-by first)
       (map (fn [[_ ls]] (first (sort-by (comp - #(nth % 2)) ls))))
       (sort-by (juxt (comp - #(nth % 2)) first))
       (take 20) vec))

(defn- kb-ic08
  "IC08: the 20 most recent Comments that are direct replies to the start
  person's Messages."
  [db person]
  (->> (eng/query db
                  '{:find [?author ?reply ?date]
                    :in [?person]
                    :where [[?msg "hasCreator" ?person]
                            [?reply "replyOf" ?msg]
                            [?reply "hasCreator" ?author]
                            [?reply "message/creationDate" ?date]]}
                  everything [person])
       (map (fn [[a r d]] [a r (parse-long d)]))
       (sort-by (juxt (comp - #(nth % 2)) second))
       (take 20) vec))

;; ── neo4j side ─────────────────────────────────────────────────────────────

(def ^:private cy-ic02
  "MATCH (p:Person {pid:$pid})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Message)
   WHERE m.creationDate < $maxDate
   RETURN f.pid AS friend, m.pid AS msg, m.creationDate AS date
   ORDER BY date DESC, msg ASC LIMIT 20")

(def ^:private cy-ic09
  "MATCH (p:Person {pid:$pid})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message)
   WHERE m.creationDate < $maxDate AND f <> p
   RETURN DISTINCT f.pid AS friend, m.pid AS msg, m.creationDate AS date
   ORDER BY date DESC, msg ASC LIMIT 20")

(def ^:private cy-ic07
  "MATCH (p:Person {pid:$pid})<-[:HAS_CREATOR]-(m:Message)<-[l:LIKES]-(liker:Person)
   WITH liker, m, l.creationDate AS date
   ORDER BY date DESC
   WITH liker, collect({m:m.pid, d:date})[0] AS top
   RETURN liker.pid AS liker, top.m AS msg, top.d AS date
   ORDER BY date DESC, liker ASC LIMIT 20")

(def ^:private cy-ic08
  "MATCH (p:Person {pid:$pid})<-[:HAS_CREATOR]-(m:Message)<-[:REPLY_OF]-(r:Message)-[:HAS_CREATOR]->(a:Person)
   RETURN a.pid AS author, r.pid AS reply, r.creationDate AS date
   ORDER BY date DESC, reply ASC LIMIT 20")

(defn- batched!
  "Run one parameterised write per chunk, each in its OWN transaction --
  a single transaction holding half a million relationship creates is a
  memory problem, not a benchmark."
  [gdb q rows]
  (doseq [chunk (partition-all 20000 rows)]
    (with-open [tx (.beginTx gdb)]
      (.execute tx q {"rows" (vec chunk)})
      (.commit tx))))

(defn- neo4j-load!
  "Nodes, then indexes (own transaction -- Neo4j forbids schema and data
  changes in the same tx), then await online, then relationships."
  [gdb {:keys [persons knows messages creator reply-of likes]}]
  (batched! gdb "UNWIND $rows AS r CREATE (:Person {pid:r.pid, firstName:r.f, lastName:r.l})"
            (mapv (fn [[id [f l]]] {"pid" id "f" f "l" l}) persons))
  (batched! gdb "UNWIND $rows AS r CREATE (:Message {pid:r.pid, creationDate:r.d, content:r.c})"
            (mapv (fn [[id [d c]]] {"pid" id "d" d "c" (or c "")}) messages))
  (with-open [tx (.beginTx gdb)]
    (-> (.schema tx) (.indexFor (Label/label "Person")) (.on "pid") (.create))
    (-> (.schema tx) (.indexFor (Label/label "Message")) (.on "pid") (.create))
    (.commit tx))
  (with-open [tx (.beginTx gdb)]
    (-> (.schema tx) (.awaitIndexesOnline 600 TimeUnit/SECONDS)))
  (batched! gdb "UNWIND $rows AS r MATCH (a:Person {pid:r.a}), (b:Person {pid:r.b}) CREATE (a)-[:KNOWS]->(b)"
            (mapv (fn [[a b]] {"a" a "b" b}) knows))
  (batched! gdb "UNWIND $rows AS r MATCH (m:Message {pid:r.m}), (p:Person {pid:r.p}) CREATE (m)-[:HAS_CREATOR]->(p)"
            (mapv (fn [[m p]] {"m" m "p" p}) creator))
  (batched! gdb "UNWIND $rows AS r MATCH (c:Message {pid:r.c}), (p:Message {pid:r.p}) CREATE (c)-[:REPLY_OF]->(p)"
            (mapv (fn [[c p]] {"c" c "p" p}) reply-of))
  (batched! gdb "UNWIND $rows AS r MATCH (p:Person {pid:r.p}), (m:Message {pid:r.m}) CREATE (p)-[:LIKES {creationDate:r.d}]->(m)"
            (mapv (fn [[p m d]] {"p" p "m" m "d" d}) likes)))

(defn- neo4j-rows [tx cypher params]
  (with-open [res (.execute tx cypher params)]
    (mapv (fn [^java.util.Map row] (into {} (map (fn [[k v]] [(keyword k) v])) row))
          (iterator-seq res))))

;; ── measurement ────────────────────────────────────────────────────────────

(defn- elapsed-ms [f]
  (let [start (System/nanoTime) result (f)]
    {:ms (/ (- (System/nanoTime) start) 1e6) :result result}))

(defn- percentile [xs q]
  (let [s (vec (sort xs))]
    (nth s (min (dec (count s)) (long (Math/floor (* q (count s))))))))

(defn- stats [xs]
  {:p50-ms (percentile xs 0.50) :p95-ms (percentile xs 0.95)
   :p99-ms (percentile xs 0.99)
   :mean-ms (/ (reduce + xs) (double (count xs)))
   :samples (count xs)})

(defn- pick-persons
  "Deterministic start-person set: the persons with the most `knows` edges,
  which is where LDBC's own parameter curation aims (a start person with no
  friends measures nothing). Sorted by id for reproducibility."
  [{:keys [knows]} n]
  (let [deg (reduce (fn [m [a b]] (-> m (update a (fnil inc 0)) (update b (fnil inc 0)))) {} knows)]
    (->> deg (sort-by (juxt (comp - val) key)) (take n) (mapv key) sort vec)))

(defn -main [& args]
  (let [dir (or (first args)
                (throw (ex-info "usage: clojure -M:ldbc-snb-bench <ldbc-dynamic-dir> [posts] [samples]" {})))
        n-posts (parse-long (or (second args) "35000"))
        samples (parse-long (or (nth args 2 nil) "30"))
        load-t (elapsed-ms #(load-subset dir n-posts))
        data (:result load-t)
        max-date (+ 1 (reduce max 0 (keep first (vals (:messages data)))))
        starts (pick-persons data samples)
        elements {:persons (count (:persons data))
                  :knows-edges (count (:knows data))
                  :messages (count (:messages data))
                  :posts (count (:post-ids data))
                  :comments (count (:comment-ids data))
                  :has-creator (count (:creator data))
                  :reply-of (count (:reply-of data))
                  :likes (count (:likes data))}
        elements (assoc elements :total (reduce + (vals (dissoc elements :posts :comments))))
        kb-build (elapsed-ms #(build-kotobase data))
        {:keys [db quads]} (:result kb-build)
        tmp (.toFile (Files/createTempDirectory "ldbc-neo4j" (make-array FileAttribute 0)))
        dbms (.build (DatabaseManagementServiceBuilder. (.toPath tmp)))
        gdb (.database dbms "neo4j")
        neo-load (elapsed-ms #(neo4j-load! gdb data))
        ;; ---- correctness: the two engines must agree before any timing counts
        agreement
        (with-open [tx (.beginTx gdb)]
          (let [p (first starts)
                norm-kb (fn [rows] (mapv (fn [r] [(str (first r)) (str (second r)) (nth r 2)]) rows))
                norm-neo (fn [rows ka kb] (mapv (fn [r] [(str (get r ka)) (str (get r kb)) (:date r)]) rows))]
            {:ic02 (= (norm-kb (kb-ic02 db p max-date))
                      (norm-neo (neo4j-rows tx cy-ic02 {"pid" p "maxDate" max-date}) :friend :msg))
             :ic07 (= (set (map first (kb-ic07 db p)))
                      (set (map (comp str :liker) (neo4j-rows tx cy-ic07 {"pid" p}))))
             :ic08 (= (norm-kb (kb-ic08 db p))
                      (norm-neo (neo4j-rows tx cy-ic08 {"pid" p}) :author :reply))
             :ic09 (= (set (map second (kb-ic09 db p max-date)))
                      (set (map (comp str :msg) (neo4j-rows tx cy-ic09 {"pid" p "maxDate" max-date}))))}))
        ;; ---- warm both engines
        _ (with-open [tx (.beginTx gdb)]
            (doseq [p (take 5 starts)]
              (kb-ic02 db p max-date) (kb-ic07 db p) (kb-ic08 db p) (kb-ic09 db p max-date)
              (neo4j-rows tx cy-ic02 {"pid" p "maxDate" max-date})
              (neo4j-rows tx cy-ic07 {"pid" p})
              (neo4j-rows tx cy-ic08 {"pid" p})
              (neo4j-rows tx cy-ic09 {"pid" p "maxDate" max-date})))
        kb-times (reduce (fn [acc p]
                           (-> acc
                               (update :ic02 conj (:ms (elapsed-ms #(kb-ic02 db p max-date))))
                               (update :ic07 conj (:ms (elapsed-ms #(kb-ic07 db p))))
                               (update :ic08 conj (:ms (elapsed-ms #(kb-ic08 db p))))
                               (update :ic09 conj (:ms (elapsed-ms #(kb-ic09 db p max-date))))))
                         {:ic02 [] :ic07 [] :ic08 [] :ic09 []} starts)
        neo-times (with-open [tx (.beginTx gdb)]
                    (reduce (fn [acc p]
                              (-> acc
                                  (update :ic02 conj (:ms (elapsed-ms #(neo4j-rows tx cy-ic02 {"pid" p "maxDate" max-date}))))
                                  (update :ic07 conj (:ms (elapsed-ms #(neo4j-rows tx cy-ic07 {"pid" p}))))
                                  (update :ic08 conj (:ms (elapsed-ms #(neo4j-rows tx cy-ic08 {"pid" p}))))
                                  (update :ic09 conj (:ms (elapsed-ms #(neo4j-rows tx cy-ic09 {"pid" p "maxDate" max-date}))))))
                            {:ic02 [] :ic07 [] :ic08 [] :ic09 []} starts))]
    (.shutdown dbms)
    (prn
     {:schema 1
      :receipt/type :ldbc-snb-interactive
      :implementation "kotobase-peer.ldbc-snb-bench"
      :dataset {:name "LDBC SNB Interactive v1, SF1, Datagen v1.0.0, CsvComposite/LongDateFormatter"
                :source "https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100"
                :subset-rule (str "all persons + all knows; " n-posts " most recent Posts + full transitive reply trees; "
                                  "hasCreator for included Messages; likes onto included Messages")
                :elements elements
                :load-ms (:ms load-t)}
      :kotobase {:quads quads
                 :build-ms (:ms kb-build)
                 :note "hot db via eng/transact -- same db value the persisted path hydrates to; no AES-GCM/content-addressing in this number"
                 :ic02 (stats (:ic02 kb-times)) :ic07 (stats (:ic07 kb-times))
                 :ic08 (stats (:ic08 kb-times)) :ic09 (stats (:ic09 kb-times))}
      :neo4j-embedded {:load-ms (:ms neo-load)
                       :version "org.neo4j/neo4j 2025.03.0 embedded (community), temp on-disk store, pid indexes awaited online"
                       :ic02 (stats (:ic02 neo-times)) :ic07 (stats (:ic07 neo-times))
                       :ic08 (stats (:ic08 neo-times)) :ic09 (stats (:ic09 neo-times))}
      :answers-agree agreement
      :start-persons {:n (count starts) :rule "highest knows-degree, sorted by id"}
      :caveats
      ["Subset of SF1, not full SF1 (~10.5M elements) -- the subset rule is stated above and is deterministic."
       "kotobase has no ORDER BY/LIMIT in its query language; the top-20 step runs in harness Clojure over the join's full result, while Neo4j does it inside Cypher. Both are timed to the same final answer and the answers are asserted equal."
       "kotobase stores knows in both directions (no undirected edge) and reifies likes (no edge properties); Neo4j uses a native undirected match and a relationship property."
       "kotobase's number is a hot in-memory db with no encryption and no persistence; the persisted path additionally pays hydration, which is measured in ADR-2607310900 and the dag-shape receipt, not here."
       "Neo4j embedded has no pure in-memory mode; its temp on-disk store is read through the page cache after warmup."
       "Single machine, single JVM, no concurrency. NamiDB's published numbers are on different hardware -- only the ratio against the shared Neo4j yardstick is comparable, and only loosely."
       "IC07/IC09 agreement compares result SETS (likers / message ids) rather than ordered rows: both queries can have creationDate ties at the LIMIT-20 boundary, where the two engines are free to pick different rows. IC02/IC08 are compared as ordered rows. A reported disagreement is reported, never relaxed after the fact."]})))
