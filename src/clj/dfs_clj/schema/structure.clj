(ns dfs-clj.schema.structure
  (:require [dfs-clj.structure :refer [gen-structure]]
            [dfs-clj.fressian.serializer :as s]
            [dfs-clj.schema.partitioner :as p]
            [dfs-clj.taps :as taps]
            [dfs-clj.schema.tapmapper :as t])
  (:gen-class))

;;; this is just an example.
;;; short-circuit what should actually be defined in the
;;; prismatic schema.
(def some-top-level-prismatic-schema-object nil)

(defn master-schema []
  some-top-level-prismatic-schema-object)

;;; end of fake schema...


(gen-structure schema.SchemaPailStructure
               :schema  (master-schema)
               :serializer  (s/fressian-serializer)
               :partitioner (p/property-name-partitioner (master-schema))
               :tapmapper   (t/property-name-tap-mapper)
               :property-path-generator taps/property-paths)
