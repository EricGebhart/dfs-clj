(ns dfs-clj.graph.data-unit-pail-structure
  (:require [dfs-clj.graph.structure :refer [gen-structure]]
            [dfs-clj.thrift.serializer :as s]
            [dfs-clj.thrift.partitioner :as pt]
            [dfs-clj.graph.partitioner :as pg]
            [dfs-clj.graph.tapmapper :as t])
  (:import [people DataUnit])
  (:gen-class))

(gen-structure dfs-clj.graph.DataUnitPailStructure
               :type DataUnit
               :serializer  (s/thrift-serializer DataUnit)
               ;:partitioner (pt/union-partitioner DataUnit)
               :partitioner (pg/union-name-property-partitioner DataUnit)
               :tapmapper   (t/union-name-property-tap-mapper))
