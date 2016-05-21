(ns dfs-clj.fakes.structure
  (:require [dfs-clj.structure :as structure]
            [dfs-clj.serializer :as s]
            [dfs-clj.partitioner :as p])
  (:use midje.open-protocols))


(defrecord-openly FakeSerializer []
  s/Serializer
  (s/serialize [this object] :fake)
  (s/deserialize [this buffer] :fake))


(defrecord-openly FakePartitioner []
  p/VerticalPartitioner
  (p/make-partition [this object] :fake)
  (p/validate [this dirs] :fake))


(defrecord UnserializableSerializer [object]
  s/Serializer
  (s/serialize [this object] (byte-array 0))
  (s/deserialize [this buffer] nil))

(defrecord UnserializablePartitioner [object]
  p/VerticalPartitioner
  (p/make-partition [this object] [])
  (p/validate [this dirs] true))


(structure/gen-structure dfs_clj.fakes.structure.DefaultPailStructure)


(structure/gen-structure dfs_clj.fakes.structure.FakePailStructure
                         :type Object
                         :prefix "fake-"
                         :serializer (FakeSerializer.)
                         :partitioner (FakePartitioner.))


; simulate a PailStructure that refers to unserializable implementations of Serializer and VerticalParitioner
(structure/gen-structure dfs_clj.fakes.structure.UnserializableStateStructure
                         :type Object
                         :prefix "unserializable-"
                         :serializer (UnserializableSerializer. *in*)
                         :partitioner (UnserializablePartitioner. *in*))
