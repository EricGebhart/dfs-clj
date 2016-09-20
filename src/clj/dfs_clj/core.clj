(ns dfs-clj.core
  "Defines dfs-clj core functionality."
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as a :refer (>!! <!! <! >! go-loop)])
  (:import (com.backtype.hadoop.pail Pail PailSpec PailStructure)))



;;; Pail Specs and Structures.

(defn ^PailSpec spec
  "Builds a PailSpec from a PailStructure."
  [^PailStructure structure]
  (PailSpec. structure))

(defn ^Pail pail
  "Opens an existing Pail."
  ([path]
   (Pail. path))
  ([fs path]
   (Pail. fs path)))

(defn pail-structure
  "Given a pail return the PailStructure."
  [pail]
  (-> pail (.getSpec) (.getStructure)))

(defn get-structure
  "Given a PailStructure or a Pail return a PailStructure"
  [pail-or-structure]
  (if (instance? PailStructure pail-or-structure)
        pail-or-structure
        (pail-structure pail-or-structure)))

(defn get-schema-or-type
  [pail-struct]
  (if-let [res (.getSchema pail-struct)]
    res
    (.getType pail-struct)))

;;; Basic functionality of things you want to do with pails.

(defn check-pail-path
  "See if there is already a pail at this dataset's path."
  [path]
  (.isDirectory (io/file path)))

(defn get-subdirs-at-dir
  ([pail]
   (get-subdirs-at-dir pail ""))
  ([pail subdir]
   (.getAttrsAtDir pail subdir)))

(defn get-metadata-filenames
  ([pail]
   (get-metadata-filenames pail ""))
  ([pail subdir]
   (.getMetadataFileNames pail subdir)))

(defn cumulative-tree-seq
  "Returns a lazy sequence of the nodes in a tree as a file path,
  via a depth-first walk. branch? must be a fn of one arg that returns
  true if passed a node that can have children (but may not).  children
  must be a fn of one arg that returns a sequence of the children. Will
  only be called on nodes for which branch? returns true. Root is the
  root node of the tree."
  {:added "1.0"
   :static true}
  [branch? children root]
  (let [walk (fn walk [node]
               (lazy-seq
                (cons node
                      (when (branch? node)
                        (map #(str node "/" %)
                             (mapcat walk (children node)))))))]
    (walk root)))

(defn pail-file-seq
  "A tree seq on pail partitions"
  [pail dir]
  (cumulative-tree-seq
   (fn [f] true)
   (fn [d] (seq (get-subdirs-at-dir pail d)))
   dir))


(defn move-append
  "Move contents of pail and append to another pail.
   Rename if necessary. "
  [source-pail dest-pail]
  (.moveAppend dest-pail source-pail 1))

(defn copy-append
  "Move contents of pail and append to another pail. Rename
   if necessary. "
  [source-pail dest-pail]
  (.copyAppend dest-pail source-pail 1))

(defn absorb
  "Absorb one pail into another. Rename if necessary."
  [dest-pail source-pail]
  (.absorb dest-pail source-pail 1))

(defn consolidate
  "consolidate pail"
  [pail]
  (.consolidate pail))

(defn snapshot
  "Create snapshot of pail at path."
  [pail path]
  (.snapshot pail path))

(defn delete-snapshot
  "delete the snapshot of a pail."
  [pail snapshot]
  (.deleteSnapshot pail snapshot))

(defn pail-is-empty?
  "check pail for emptiness"
  [pail]
  (.isEmpty pail))

(defn pail-exists?
  "check to see if the pail path exists."
  [path]
  (.isDirectory (io/file path)))

(defn delete
  "delete pail path recursively"
  [path]
  (.delete path true))

(defn get-root [pail]
  (.getInstanceRoot pail))

(defn writer [pail]
  (.openWrite pail))

(defn close [writer]
  (.close writer))

(defn write [writer o]
  (.writeObject writer o))

;;; Opening and creating pails.

;; these can go away when the clj-pail clojars is updated with the pull request.
(defn ^Pail create
  "Creates a Pail from a PailSpec at `path`."
  [spec-or-structure path & {:keys [filesystem fail-on-exists]
                             :or {fail-on-exists true}
                             :as opts}]
  (if (instance? PailStructure spec-or-structure)
    (apply create (spec spec-or-structure) path (mapcat identity opts))
    (if filesystem
      (Pail/create filesystem path spec-or-structure fail-on-exists)
      (Pail/create path spec-or-structure fail-on-exists))))

(defn find-or-create [pstruct path & {:as create-key-args}]
  "Get a pail from a path, or create one if not found"
  (try (pail path)
       (catch Exception e
         (apply create pstruct path (mapcat identity create-key-args)))))


;;;; writing and reading from a pail..

(defn write-objects
  "Write a list of objects to a pail"
  [pail objects]
  (with-open [writer (.openWrite pail)]
    (doseq [o objects]
      (.writeObject writer o))))

(defn object-seq
  "Returns a sequence of objects read from the Pail."
  [^Pail pail]
  (iterator-seq (.iterator pail)))

(defn write<
  "read objects from a channel and write them to a pail."
  [pail from-ch done-ch data-key & {:keys [log-fn limit fini-fn]}]
  (let [writer (writer pail)]
    ;; Start reading from the channel and writing to the pail.
    (go-loop [total 0]
      (when log-fn
        (log-fn total))
      (let [dus (<! from-ch)]
        (if-not (or (nil? dus) (= total limit))
          (do
            (doseq [o dus]
              (write writer o))
            (recur (inc total)))
          (do
            (>! done-ch {data-key {:completed total}})

                                        ;(info "Import finished: " data-key " - " total)
            (when fini-fn
              (fini-fn total))
            (close writer)))))))

;;;; TODO
(defn validate
  "Validate that a pail connection matches a pail structure. This is basically an implementation
   of the validation code in dfs-datastores pail create(). The specs are only compared if
   .getName is not nil. Otherwise it's just a check to make sure the PailStructure types match."
  [pail-connection structure]
  (let [conn-spec (.getSpec pail-connection)
        conn-struct (.getStructure conn-spec)
        struct-spec (spec structure)]
    (cond (and (.getName struct-spec) (not (.equals conn-spec struct-spec))) false
          (not (= (type structure) (type conn-struct))) false
          :else true)))



(defmacro with-snapshot
  "Automatically deletes Pail snapshots after successfuly executing a block of code. The snapshots
  should be created with `Pail.snapshot()` and will only be deleted if the body finishes
  successfully. If the body throws an exception, the snapshot will not be deleted.

  This is intended to be used to safely delete data that was successfully processed while not
  deleting data that fails to process.

  The first argument should be the original Pail that the snapshots are derived from. If the body
  finishes correctly, this is the Pail from which the data will be deleted.

  The second argument is a vector of bindings. The bindings should specify snapshots that can be
  deleted from the original Pail with `Pail.deleteSnapshot()`.

  Example:

    (with-snapshot original-pail [snapshot-pail (.snapshot origin-pail \"/path/to/snapshot\")]
      ...)"
  [pail bindings & body]
  (cond
    (empty? bindings)
    `(do ~@body)

    (symbol? (first bindings))
    (let [[this-binding rest-bindings] (split-at 2 bindings)
          snapshot (first this-binding)]
      `(let ~(vec this-binding)
         (let [result# (with-snapshot ~pail ~rest-bindings ~@body)]
           (.deleteSnapshot ~pail ~snapshot)
           result#)))

    :else
    (throw (IllegalArgumentException. "with-snapshot only allows symbols in bindings"))))
