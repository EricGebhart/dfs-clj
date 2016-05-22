
dfs-clj
============

A Clojure interface for [dfs datastores](https://github.com/nathanmarz/dfs-datastores) that allows for separate definitions of serialization, vertical partitioning, and `PailStructure` for use with Cascalog, Thrift, graph schema, fressian, and prismatic schema.

This library started as extensions to clj-pail, pail-cascalog and clj-thrift. The last few years
have seen the creation of other libraries, pail-graph, clj-pail-tap, pail-fressian and pail-schema.  
All of these libraries became rather messy to use.

This library is all of those libraries combined. 

The core of this project is clj-pail plus clj-pail-tap and then some. This extends the pail structure in
clj-pail to include a tapmapper, schema, and a path generator.  All of which are used when using prismatic schema as a schema.  Pail-cascalog and all additional tap and sink functionality is in taps.clj.

Beyond that, there are schema, fressian, thrift and graph namespaces which contain core functionality, 
serializers, partitioners and structures that are ready to use.  Graph simply extends the thrift
functionality to include graph schema data types.

Dfs-clj breaks up the [`PailStructure` interface](https://github.com/nathanmarz/dfs-datastores/blob/develop/dfs-datastores/src/main/java/com/backtype/hadoop/pail/PailStructure.java) into two separate Clojure protocols (`Serializer` and `VerticalPartitioner`) so that serialization and partitioning can be defined separately. Then it defines a macro (`gen-structure`) to generate a class that implements `PailStructure` by composing the two protocols.

A library to enable easier management and use of pails using, prismatic schema, thrift and fressian as desired..

Built on top of David Cuddeback's [clj-pail](https://github.com/dcuddeback/clj-pail).

## Usage

Add `dfs-clj` to your project's dependencies. If you're using Leiningen, your `project.clj` should look something like this:

~~~clojure
(defproject ...
  :dependencies [[dfs-clj VERSION]])
~~~

Where `VERSION` is the latest version on [Clojars](https://clojars.org/dfs-clj).

### Defining a `PailStructure`

You can generate classes that implement the `PailStructure` interface with the [`gen-structure` macro](src/main/clojure/clj_pail_tap/structure.clj) from the `clj-pail-tap.structure` namespace. The `PailStructure` interface is used by Pail to serialize, deserialize, and keep organized your data.

~~~clojure
(ns ...
  (:require [dfs-clj.structure :as s]))

(s/gen-structure com.example.pail.DefaultPailStructure)

~~~

`gen-structure` uses `gen-class`. So any namespace that uses `gen-structure` needs to be AOT-compiled. In Leiningen, add your namespace to the `:aot` key in `project.clj`:

~~~clojure
(defproject ...
  :aot [myproj.ns.that.uses.clj-pail.structure])
~~~

#### Options


`PailStructure` classes are defined with the `gen-structure` macro from `dfs-clj`. `dfs-clj.fressian`
, `dfs-clj.schema`, `dfs-clj.thrift` and `dfs-clj.graph` provide serializers, partitioners that can be used with the `gen-structure` macro.  Each namespace also provides example structures which you may use directly or as templates for creating your own.

By default, a `PailStructure` class generated with `gen-structure` will do nothing. It will be defined to handle `byte[]`; serialization and deserialization will do nothing (because your data is already a `byte` array); and no vertical partitioning will be defined.

These behaviors can be specified with options to `gen-structure`:

~~~clojure
(s/gen-structure com.example.pail.CustomPailStructure
                 :type DataUnit-A-Thrift-Class.
                 :schema DataUnit-Some-Prismatic-Schema-definition.
                 :serializer (CustomDateSerializer. DataUnit-A-Thrift-Class)
                 :partitioner (DailyDatePartitioner.)
                 :tapmapper (DataUnit-tapmapper)
                 :property-path-generator (DataUnit-property-paths)
                 :prefix "date-")
~~~

~~~clojure
(ns example.pail
  (:require [clj-pail.structure :refer [gen-structure]]
            [pail-fressian.serializer :as s]
            [pail-fressian.partitioner :as p])
  (:gen-class))

(gen-structure example.pail.PailStructure
               :serializer (s/fressian-serializer)
               :partitioner (p/fressian-partitioner))
~~~

In the above example, we define a `PailStructure` that can serialize any native data type using
Fressian read and write. The `PailStructure` will also be vertically
partitioned by the first field of each data object.


Type and schema are mutually exclusive. Type should be used for objects, whereas Schema is
for the situation when the serializer is Fressian instead of thrift and the schema is Prismatic schema
rather than graph schema.

Property-path-generator should be a function which takes whichever is being used, type or schema, and returns
a list of property path vectors.

Tap Mapper should be a function that can be mapped to output of the property-path-generator and provides property
path which correlates to the path the partitioner would create.

In addition to the core functionality, you will want to use some of the extras depending upon how you want
to use dfs datastores.  With thrift, native clojure data and a schema of some sort.
 
If you wish to use a thrift the thrift namespace has everything you need, if you wish to add graph schema
to your use of thrift, the graph namespace adds that additional functionality.

If you wish to use native clojure data the fressian namespace is all you need.

If you wish to use prismatic schema with native data then the schema namespace adds some more functionality.


#### Vertical Partitioning

A `PailStructure` is vertically partitioned according to the partitioner supplied as the
`:partitioner` keyword argument of `gen-structure`. `dfs-clj` provides somewhat generic partitioners, but
you will most likely want to create a partitioner specific to your application.

Generalized partitioners are defined in
[`dfs-clj.partitioner`](src/clj/dfs-clj/partitioner.clj). 
[`dfs-clj.fressian.partitioner`](src/clj/dfs-clj/fressian/partitioner.clj). 
[`dfs-clj.schema.partitioner`](src/clj/dfs-clj/schema/partitioner.clj). 
[`dfs-clj.graph.partitioner`](src/clj/dfs-clj/graph/partitioner.clj). 
[`dfs-clj.thrift.partitioner`](src/clj/dfs-clj/thrift/partitioner.clj). 

There are more than one partitioner in each. They are somewhat contrived, but may work perfectly
for you or at least provide inspiration for your own. The shape of data is a local problem, so
creating generic partitioners is somewhat difficult.

One Fressian partitioner uses the first property name of the data object to create a single level partitioning scheme.
The other Fressian partitioner uses the type name of the data object, while also
looking specifically for anything ending in [Pp]roperty, When found it looks for a possible second level of partitioning in
a :property field. Due to the open nature of a fressian pail these partitioners are truly only examples of how to proceed
in making your own application specific partitioner. Both partitioners short-circuit partition validation as almost anything
goes here, and there is no type to check.

If you have ideas about better generic partitioners, validation or improvements please fork me.

## License

Copyright © 2014 Eric Gebhart

Distributed under the [MIT License](LICENSE).

