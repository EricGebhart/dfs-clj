(ns dfs-clj.graph.type
  "Functions for working with Graph Schema Thrift types."
  (:require [clojure.zip :as zip]
            [dfs-clj.thrift.type])
  (:import (org.apache.thrift TFieldIdEnum)))


(defn ordered-field-names
  "Returns a vector of names for the fields of a Thrift struct or union.
  The function's argument should be the class itself."
  [type]
    (mapv :name (field-meta-list type)))

(defn ordered-field-ids
  "Give an ordered vector of field ids for a struct or union."
  [type]
    (mapv :id (field-meta-list type)))

;; Get a list of property paths to make it easy to create
;; Cascalog taps for partitioned data.
(defn- get-type
  "get field type for a data type and field name if it's a struct or union
   field name vector is as created by field-ids-names [ id name ]."
  [parent field-name-vector]
  (let [field-key (keyword (:name field-name-vector))]
    (if (struct-field? parent field-key)
      (field-type parent field-key))))

(defn- get-field-tree
  "drill down through the fields of a thrift object and create
   a tree of paths consisting of field-maps.
   With each leaf node terminating in nil"
  [datatype & {:keys [parent] :or []}]
  (if datatype
    (mapv #(vec (conj parent %1 (get-field-tree (get-type %2 %1)
                                                :parent (vec (conj parent %1)))))
         (field-meta-list datatype) (repeat datatype))))

(defn- ptest
  "If it's a vector that ends with nil, it's a leaf node"
  [x]
    (and (vector? x) (nil? (last x))))

(defn- get-property-paths
  "Get the property leaf nodes out of the field tree"
  [tree]
    (loop [loc (zip/vector-zip tree)
           ps []]
      (if (zip/end? loc)
        ps
        (recur (zip/next loc)
               (if (ptest (zip/node loc))
                 (conj ps (keep identity (zip/node loc)))
                 ps)))))

(defn property-paths
  "Get a list of property paths for a thrift data type.
   Each row consists of a set of field maps leading
   to a field, such that a path can be created for a property similar
   to the way a Pail Partitioner does. Using field ids or field names."
  [type]
  (get-property-paths (mapv #(vec (reverse %)) (get-field-tree type))))
