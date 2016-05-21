(ns dfs-clj.schema.partitioner
  "Defines a Pail partitioner for Prismatic schema objects."
  (:require [dfs-clj.partitioner :as p]))

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

  PropertyNamePartitioner
  [type]


  p/VerticalPartitioner
  (p/make-partition
    [this object]
    (let [res (vector (name (first (keys object))))]
          (if (re-find #"^.*[Pp]roperty$" (first res))
            (let [subunion (:property ((first (keys object)) object))]
              (conj res (name (first (keys subunion)))))
            res)))

    ; anything is valid for now.
  (p/validate
    [this dirs]
    [true (rest dirs)]))

(defn property-name-partitioner
  [type]
  (PropertyNamePartitioner. type))
