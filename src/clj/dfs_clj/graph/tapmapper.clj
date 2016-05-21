(ns dfs-clj.graph.tapmapper
  "Defines Pail Tap mappers for vertically partitioned Pails."
  (:require [dfs-clj.graph.type :as type]
            [dfs-clj.thrift.type :as ttype]))

                                        ;---- Tap Mappers ------
                                        ;Takes a thrift property_path vector and creates a valid partition path for that property.
                                        ;   [ :location ["property" "location"]]
                                        ;Specify the tapmapper with :tapmapper in the PailStructure.
                                        ;
(defn nulltapmapper [path]
  "A null tapmapper returns no taps"
  {})

(defn null-tapmapper
  "return a null tapmapper"
  []
  nulltapmapper)

; Union taps - returns the top level field id's as a single directory path.
(defn union-taps [path]
  "Single level union partitioner which uses field ids as directory names."
  (let [res (:id (first path))
        fieldname (:name (first path))]
    (conj [(keyword fieldname)]  (vec [res]))))

(defn union-tap-mapper
  "returns a union-tap-mapper"
  []
  union-taps)


;union name taps - returns the top level field names as a single directory path.
(defn union-name-taps [path]
  "Single level union partitioner which uses field names as directory names."
  (let [res (:name (first path))
        fieldname (:name (first path))]
    (conj [(keyword fieldname)]  (vec [res]))))

(defn union-name-tap-mapper
  "returns a union-name-tap-mapper"
  []
  union-name-taps)


; union name property taps - returns top level names, and second level names for top level names ending in [Pp]roperty.
(defn union-name-property-taps [path]
  "fields ending in [Pp]roperty are partitioned further. ie. :first_name ['property' 'first_name']
   for partitioners where the field name is the directory name."
  (let [propregex #"^.*[Pp]roperty$"
        res (:name (first path))
        subunion (if (and (re-find propregex res) (> (count path) 2)) (nth path 2) nil)
        name (let [prefix (clojure.string/replace res propregex "-")]
               (if subunion
                 (if (= prefix "-")
                   (:name subunion)
                   (clojure.string/join prefix (:name subunion)))
                 res))]
    (conj [(keyword name)]  (vec (if subunion (conj [res] (:name subunion)) [res])))))

(defn union-name-property-tap-mapper
  "returns a union name property tap mapper"
  []
  union-name-property-taps)


; union property taps - returns top level field ids, and second level field ids for top level names ending in [Pp]roperty.
(defn union-property-taps [path]
  "fields ending in [Pp]roperty are partitioned further. ie. :first_name [1 1]. for a partitioner
   where directory names are field ids."
  (let [propregex #"^.*[Pp]roperty$"
        res (:id (first path))
        fieldname (:name (first path))
        subunion (if (and (re-find propregex fieldname) (> (count path) 2)) (nth path 2) nil)
        name (let [prefix (clojure.string/replace fieldname propregex "-")]
               (if subunion
                 (if (= prefix "-")
                   (:name subunion)
                   (clojure.string/join prefix (:name subunion)))
                 fieldname))]
    (conj [(keyword name)]  (vec (if subunion (conj [res] (:id subunion)) [res])))))

(defn union-property-tap-mapper
  "returns a union property tap mapper"
  []
  union-property-taps)
