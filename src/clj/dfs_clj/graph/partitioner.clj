(ns dfs-clj.graph.partitioner
  "Defines a Pail partitioner for graph schema thrift objects."
  (:require [dfs-clj.partitioner :as p]
            [dfs-clj.thrift.union :as union]
            [dfs-clj.thrift.base :as thrift]
            [dfs-clj.thrift.type :as type]))


(defrecord ^{:doc "A 2 level pail partitioner for Thrift unions. It requires a type, which must be a subtype
                  of `TUnion`. The partitioner will partition based on the union's set field name so that
                  all union values with the same field will be placed in the same partition. If a field's
                  name is property or ends in property or Property the partitioner will also partition
                  by the union found in the :property field of that structure.

                    Union PropertyValue {
                        1: name;
                        2: lastname;
                    }

                    Struct PersonProperty {
                        1: string id;
                        2: PropertyValue property;    /* <--- this name 'property' is required. */
                    }

                    Union DataUnit {
                        1: PersonProperty MyProperty;
                        2: string Things;
                    }

                  Partitioning DataUnit will result in \"/1/1\" (name), \"/1/2\" (lastname), and \"/2\" (things) as the partitions. "}

  UnionPropertyPartitioner
  [type]

  p/VerticalPartitioner
    (p/make-partition
     [this object]
     (let [res (vector (union/current-field-id object))]
       (if (re-find #"^.*[Pp]roperty$" (union/current-field-name object))
         (let [subunion (thrift/value (union/current-value object) :property)]
           (conj res (union/current-field-id subunion)))
       res)))

  (p/validate
    [this dirs]
    [(try
       (contains? (type/field-ids type)
                  (Integer/valueOf (first dirs)))
       (catch NumberFormatException e
         false))
     (rest dirs)]))

(defn union-property-partitioner
  [type]
  (UnionPropertyPartitioner. type))


(defrecord ^{:doc "A 2 level pail partitioner for Thrift unions. It requires a type, which must be a subtype
                  of `TUnion`. The partitioner will partition based on the union's set field name so that
                  all union values with the same field will be placed in the same partition. If a field's
                  name is property or ends in property or Property the partitioner will also partition
                  by the union found in the :property field of that structure.

                    Union PropertyValue {
                        1: name;
                        2: lastname;
                    }

                    Struct PersonProperty {
                        1: string id;
                        2: PropertyValue property;    /* <--- this name 'property' is required. */
                    }

                    Union DataUnit {
                        1: PersonProperty MyProperty;
                        2: string Things;
                    }

                  Partitioning DataUnit will result in \"/MyProperty/name\", \"/MyProperty/lastname\", and \"/Things\" as the partitions."}

  UnionNamePropertyPartitioner
  [type]


  p/VerticalPartitioner
  (p/make-partition
    [this object]
    (let [res (vector (union/current-field-name object))]
      (if (re-find #"^.*[Pp]roperty$" (first res))
        (let [subunion (thrift/value (union/current-value object) :property)]
          (conj res (union/current-field-name subunion)))
        res)))


  (p/validate
    [this dirs]
    [(contains? (type/field-names type)
                  (first dirs))
     (rest dirs)])
  )

(defn union-name-property-partitioner
  [type]
  (UnionNamePropertyPartitioner. type))
