(ns dfs-clj.schema.tapmapper
  "Defines Pail Tap mappers for vertically partitioned Pails.")

;; union name property taps - returns top level names, and second level names for top level
;;names ending in [Pp]roperty.
(defn property-name-taps [path]
  "fields ending in [Pp]roperty are partitioned further. ie. :first_name ['property' 'first_name']
   for partitioners where the field name is the directory name."
  (let [propregex #"^.*[Pp]roperty$"
        res (name (first path))
        subunion (if (and (re-find propregex res) (> (count path) 2)) (nth path 2) nil)
        pname (let [prefix (clojure.string/replace res propregex "-")]
               (if subunion
                 (if (= prefix "-")
                   (name subunion)
                   (clojure.string/join prefix (name subunion)))
                 res))]
    (conj [(keyword pname)]  (vec (if subunion (conj [res] (name subunion)) [res])))))

(defn property-name-tap-mapper
  "returns a union name property tap mapper"
  []
  property-name-taps)
