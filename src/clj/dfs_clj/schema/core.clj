(ns dfs-clj.schema.core
  "Defines Pail-Schema core functionality.")

;;; basically this is a path generator for prismatic schema to put in
;;; your pail struct.

;;; I suspect that this is completely unnecessary as putting a nil
;;; in the pail struct for the tapmapper will short this out and it
;;; get-tap will still figure out what is going on and give you tap.
;;; By looking in the pail and coming back with the property paths it has.


(declare get-field-keys)

(defn- keys-for-either [schema]
  (mapcat get-field-keys (:schemas schema)))

(defn- key-keyword [keyish]
  (condp = (type keyish)
    schema.core.OptionalKey (:k keyish)
    keyish))

(defn- keys-for-map [schema]
  (map vector
       (map key-keyword (keys schema))
       (map get-field-keys (vals schema))))

(defn- get-field-keys
  "create a tree of properties in a schema."
  [schema]
  (condp = (type schema)
    schema.core.Either (keys-for-either schema)
    clojure.lang.PersistentArrayMap (keys-for-map schema)
    []))

(defn- node? [t]
  (and (keyword? (first t))
       (= 2 (count t))))

(defn- last-node? [t]
  (and (node? t)
       (= [] (last t))))

(defn- get-property-paths [tree]
  (cond
    (last-node? tree) [ [(first tree)] ]
    (node? tree)
    (let [children (get-property-paths (last tree))]
      (map #(conj % (first tree)) children))
    (seq? tree)
    (mapcat get-property-paths tree)))

(defn property-paths
  "Get a list of property paths for a prismatic schema data type.
   Each row consists of a set of field maps leading
   to a field, such that a path can be created for a property similar
   to the way a Pail Partitioner does. Using field ids or field names."
  [schema]
  (map reverse (get-property-paths (get-field-keys schema))))
