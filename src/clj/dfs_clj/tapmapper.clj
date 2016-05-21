(ns dfs-clj.tapmapper
"Defines Null Pail Tap mappers and property path generators for vertically partitioned Pails.")

;------ Path generator ---------
; given a thrift data type or other schema object
; return a list of vectors representing the paths to each property in the tree.
; The thrift example in pail-graph generates a property path list like this.
; Although this is very specific to graph schema, where field-ids and names are given.

(comment
  (type/property-paths DataUnit)
  =>[({:id 1, :name "property"} {:id 1, :name "id"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 1, :name "first_name"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 2, :name "last_name"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 4, :name "location"} {:id 1, :name "address"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 4, :name "location"} {:id 2, :name "city"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 4, :name "location"} {:id 3, :name "county"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 4, :name "location"} {:id 4, :name "state"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 4, :name "location"} {:id 5, :name "country"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 4, :name "location"} {:id 6, :name "zip"})
   ({:id 1, :name "property"} {:id 2, :name "property"} {:id 5, :name "age"})
   ({:id 2, :name "friendshipedge"} {:id 1, :name "id1"})
   ({:id 2, :name "friendshipedge"} {:id 2, :name "id2"})])


(defn nullpathgenerator [path]
  "A null pathgenerator returns no paths"
  {})

(defn null-path-generator
  "return a null pathgenerator"
  []
  nullpathgenerator)


;---- Tap Mappers ------
;Takes a thrift property_path vector and creates a valid partition path for that property.
;   [ :location ["property" "location"]]
;Specify the tapmapper with :tapmapper in the PailStructure.
;
(defn nulltapmapper [path]
  "A null tapmapper returns no taps"
  {})

(defn null-tapmapper
  "return a null tapmapper"
  []
  nulltapmapper)
