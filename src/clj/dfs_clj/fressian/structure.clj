(ns dfs-clj.fressian.fressian-pail-structure
  (:require [dfs-clj.structure :refer [gen-structure]]
            [dfs-clj.fressian.serializer :as s]
            [dfs-clj.fressian.partitioner :as p]
            [dfs-clj.schema.core :as sc])
  (:gen-class))

(gen-structure pail-fressian.FressianPailStructure
               :type (type {})
               :serializer (s/fressian-serializer)
               ;:partitioner (p/fressian-partitioner)
               :partitioner (p/fressian-property-partitioner)
               :tapmapper   (t/null-tapmapper)
               :property-path-generator (sc/null-path-generator))
