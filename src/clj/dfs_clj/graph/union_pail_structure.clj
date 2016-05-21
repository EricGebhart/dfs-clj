(ns dfs-clj.graph.union-pail-structure
  (:require [dfs-clj.graph.structure :refer [gen-structure]]
            [dfs-clj.thrift.serializer :as s]
            [dfs-clj.thrift.partitioner :as p]
            [dfs-clj.graph.tapmapper :as t])
  (:import [people DataUnit])
  (:gen-class))

(gen-structure pail-graph.UnionPailStructure
               :type DataUnit
               :serializer (s/thrift-serializer DataUnit)
               :partitioner (p/union-partitioner DataUnit)
               :tapmapper (t/union-tap-mapper))
