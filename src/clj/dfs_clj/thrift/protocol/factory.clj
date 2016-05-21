(ns dfs-clj.thrift.protocol.factory
  "Functions for constructing Thrift protocol factories.
  Protocol factories are used to customize how Thrift objects are serialized. Thrift includes five
  standard protocols: binary, compact, JSON, simple JSON, and tuple. Any one of the protocols can be
  used when constructing a serializer or deserializer."
  (:import (org.apache.thrift.protocol TBinaryProtocol$Factory
                                       TCompactProtocol$Factory
                                       TJSONProtocol$Factory
                                       TSimpleJSONProtocol$Factory
                                       TTupleProtocol$Factory)))


(defn binary
  "Returns a `TBinaryProtocol.Factory`."
  [& {:keys [strict-read strict-write read-length]
      :or {strict-read false
           strict-write true
           read-length 0}}]
  (TBinaryProtocol$Factory. strict-read strict-write read-length))


(defn compact
  "Returns a `TCompactProtocol.Factory`."
  []
                                        ; TCompactProtocol.Factory receives a `maxNetworkBytes` parameter in 0.9, but supporting it would
                                        ; not compile with Thrift 0.8
  (TCompactProtocol$Factory.))


(defn json
  "Returns a `TJSONProtocol.Factory`."
  []
  (TJSONProtocol$Factory.))


(defn simple-json
  "Returns a `TSimpleJSONProtocol.Factory`."
  []
  (TSimpleJSONProtocol$Factory.))


(defn tuple
  "Returns a `TTupleProtocol.Factory`."
  []
  (TTupleProtocol$Factory.))
