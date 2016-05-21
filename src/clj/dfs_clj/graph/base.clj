(ns dfs-clj.graph.base
  (:require [dfs-clj.graph.type :as ttype]
            [dfs-clj.thrift.base :as base]
            [dfs-clj.thrift.type :as type]
            [dfs-clj.union :as tunion]))
(:import (java.nio ByteBuffer))

; simple data extraction
(defn property-value
  "get the named field value from the current structure in the top level union.
   Union -> struct :<field-name> - value."
  [object field-name]
  (base/value (tunion/current-value object) field-name))

(defn property-union-value
  "get a value from a union, inside a struct inside a union.
   name is the property name inside the struct.
   Union -> Struct :<field-name> -> Union - value."
  [object field-name]
  (tunion/current-value (property-value object field-name)))

(defn field-keys
  "Give back an ordered vector of field keys for a struct or union."
  [object]
    (mapv #(keyword (:name %)) (ttype/field-meta-list (type object))))
