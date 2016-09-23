(ns dfs-clj.taps
  (:require [dfs-clj.core :as dfs])
  (:import (com.backtype.hadoop.pail Pail PailSpec PailStructure)
           (com.backtype.cascading.tap PailTap PailTap$PailTapOptions)))

;;; It's all about the paths to the taps...

(defn ^PailTap$PailTapOptions tap-options
  "Creates a `PailTapOptions` from a `PailSpec`. The `PailTapOptions` can then be used to create a
  `PailTap`. The `PailTap` will use the provided `PailSpec` to serialize, deserialize, and partition
  the data stored by the tap.

  When sourcing and sinking tuple, the tuple will have two fields. The first field is the path
  within the pail and is called `\"pail_root\"`. The second field will be named whatever string is
  given as the `:field-name` options, which defaults to `\"bytes\"`.

  The tap can be limited to a subset of the paths within the pail by giving a list of paths as the
  `:attributes` option. Each path must be a list of strings, each string corresponding to one level
  of the path. For example, to include data from the paths `\"foo/bar\"` and `\"baz\"`, the
  `:attributes` option would be `[[\"foo\" \"bar\"] [\"baz\"]]`. This option defaults to `nil`,
  which includes all paths within the pail.

  The last option is `:lister`, which attaches a `PailPathLister` to the `PailTap`."
  [spec & {:keys [field-name attributes lister]
           :or {field-name "bytes"}}]
  (PailTap$PailTapOptions.
   spec
   field-name
   (when attributes
     (if (every? string? attributes)
       (into-array [attributes])
       (into-array attributes)))
   lister))


(defn ^PailTap tap
  "Creates a `PailTap`. The tap will source and sink data from the path provided as the `path`
  parameter. The tap's behavior is specified by a `PailTapOptions` object, provided as the `options`
  parameter. The `options` parameter can be curried into a closure to create a factory-style
  function for creating taps with the same behavior at different paths."
  [options path]
  (PailTap. path options))


(defn pail->tap
  "Creates a `PailTap` from an existing `Pail`. The tap will source data from `pail` and can be
  customized with the same options as the `tap-options` function."
  [pail & opts]
  (let [spec (.getSpec pail)
        path (.getRoot pail)]
    (tap (apply tap-options spec opts) path)))

(defn get-sink [pstruct path]
  "Create sink according to the pail structure and path.
    As long as a pail doesn't already exist there."
  (when-not (dfs/check-pail-path path)
    (tap (tap-options (dfs/spec pstruct)) path)))

;;;; finding the paths to the taps.

(defn get-property-key
  "look in a pail and see what the first level of partitions is named."
  [pail]
  (keyword (first (dfs/get-subdirs-at-dir pail))))

;; ;;;; get-pail-keys are not used as far as I can tell...
;; (defn get-pail-keys-old
;;   "get a list of all the partitions in a pail."
;;   [pail]
;;   (let [paths (map #(clojure.string/split (str %) #"/")
;;                    (file-seq (clojure.java.io/file
;;                               (dfs/get-root pail))))
;;         root-count (inc (count (first paths)))
;;         properties (filter #(not (re-find #"pail" (last %))) paths)]
;;     properties))

;; (defn get-pail-keys
;;   [pail]
;;   (let [paths (map #(clojure.string/split % #"/")
;;                    (filter not-empty (dfs/pail-file-seq pail "")))]
;;     paths))

(defn get-tap-paths
  "create paths to properties by looking in the pail."
  [pail]
  (map #(vec (filter not-empty (clojure.string/split % #"/")))
       (filter not-empty (dfs/pail-file-seq pail))))

(defn get-tap-paths-old
  "create paths to properties by looking in the pail."
  [pail]
  (let [paths (map #(clojure.string/split (str %) #"/")
                   (file-seq (clojure.java.io/file
                              (dfs/get-root pail))))
        root-count (count (first paths))
        first-levels (filter identity (map #(take-last (- (count %) root-count) %) paths))]
    (map vec (filter #(not (re-find #"pail" (last %))) first-levels))))

(defn get-available-tap-map
  "Create a tap map of existing data in a pail."
  [pail]
  (let [paths (get-tap-paths pail)]
    (reduce #(conj %1  [(keyword (last %2)) %2]) '() paths)))

(defn filter-tap-map
  "filter a list of vectors by their first level property name.
  If no property to filter by, return what we were given."
  [m p]
  (if p
    (let [p (name p)]
      (into {} (filter #(= p (first (second %))) m)))
    m))

;;; tap-map should maybe just use 'get-available-tap-map instead of working through
;;; the schema.
;;; There could be many entities in the schema which have the same taps.
;;; The schema is not very helpful except for saying what are potential taps.
;;; the real taps can only be found by looking in the hadoop partition.
;;; We always get a property here, if we are not given one, we go look in the pail
;;; and grab the first one we find.
(defn tap-map
  "Get the tap map for a Pail or Pail Structure. When nil is given
  for property type return list of vectors for every possible tap.
  When no tapmapper is specified on the pail structure, look in the
  pail for actual tap paths."
  [pail-or-struct property-type]
  (let [pail-struct (dfs/get-structure pail-or-struct)
        tapmapper (.getTapMapper pail-struct)
        path-generator (.getPropertyPathGenerator pail-struct)
        type (dfs/get-schema-or-type pail-struct)]
    (filter-tap-map
     (if tapmapper
       (distinct (map tapmapper (path-generator type)))
       (get-available-tap-map pail-or-struct))

     property-type)))

(defn list-taps
  "Give a list of the Tap keys available for a Pail or Pail Structure"
  [pail-or-struct]
  (let [pail-struct (dfs/get-structure pail-or-struct)]
    (keys (tap-map pail-struct))))

(defn get-tap
  "Creates a `PailTap` from an existing vertically partitioned pail, by selecting an
   entry from the Pail's tap map. Takes a pail connection. returns nil if no tap found.
  if no property type is given, get the first one we find in the pail."

  ([pail tap-key]
   (get-tap pail tap-key nil))

  ([pail tap-key property-type]
   (let [property-type (or property-type
                           (get-property-key pail))]
     (when-let [attrs (tap-key
                       (tap-map pail property-type))]
       (pail->tap pail :field-name (name tap-key)
                  :attributes [attrs])))))

(defn get-taps [pail & tap-keys]
  "Get a map with a pail and some taps in it.
   Returns {:pail the-pail :tap-key tap :tap-key tap :tap-key tap ...} "
  (into {:pail pail}
        (reduce merge {}
                (map #(identity
                       {% (get-tap pail %)})
                     tap-keys))))

(defn get-compound-tap
  "Get a single tap for a group of pail partitions, a vector of tap-keys."
  ([pail tap-keys]
   (get-compound-tap pail tap-keys nil))

  ([pail tap-keys property-type]
   (let [property-type (or property-type
                           (get-property-key pail))]
     (pail->tap pail
                :attributes (map #(%1 %2)
                                 tap-keys
                                 (repeat (tap-map pail property-type)))))))
