(ns datasplash.repl-utils
  (:require
   [clojure.java.io :as io]
   [clojure.walk :as walk]
   [datasplash.api :as ds]
   [clojure.edn :as edn]
   [superstring.core :as str]))

(defn read-edns-file
  "Reads a newline-separated seq of edn from `file`.
  Realizes it (not lazy).
  Note that this has nothing to do with dataflow and is only meant
  for internal use of this namespace."
  [file]
  (with-open [r (io/reader file)]
    (mapv edn/read-string
          (line-seq r))))

(defn write-map-edn
  "Uses `write-edn-file` to write individual pcolls in pcoll-map.
  pcoll-map should be a map of pcolls. keys will be turned to
  strings by `key->str`, which defaults to `name`, and they will
  be used for the filenames."
  ([dir pcoll-map]
   (write-map-edn dir name pcoll-map))
  ([dir key->str pcoll-map]
   (doseq [[k pcoll] pcoll-map]
     (ds/write-edn-file (str (java.io.File. dir (key->str k))) {:num-shards 1} pcoll))))

(defn read-map-edn
  "Reads each file in `dir`, uses the names for keys, and puts that in a map.
  `dir` will be turned to a java.io.File if it is a string. You can supply
  a `str-rename-fn` that will rename keys before they turn to keywords."
  ([dir]
   (read-map-edn dir identity))
  ([dir str-rename-fn]
   (into {}
         (comp (filter #(.isFile %)) ;; annoying necessary lambda
               (map (fn [f]
                      [(-> (.getName f) str-rename-fn keyword)
                       (read-edns-file f)])))
         (.listFiles (java.io.File. dir)))))

(defn remove-0s [s]
  (str/chop-suffix s "-00000-of-00001"))

(defn add0s [s]
  (str s " 00000-of-00001"))

(.isFile (java.io.File. "/home/marc"))
(read-map-edn "/home/marc/wme" remove-0s)

(defmacro direct
  "Inserts pipeline boilerplate.
  This is meant to be used at the repl, or in test namespaces.
  In the main code it makes more sense to write the boilerplate
  explicitely.

  Examples at the end of the docstring.

  The first argument is a bindings vector like in `let`.
  A `p (ds/make-pipeline [])` has been already been inserted.
  You could shadow it if you want (probably not), but you can't
  access a `p` from outer scope.
  The `side-inputs` symbol has a special meaning.
  If present, the corresponding map will be added
  as a `:side-inputs` key to the options argument
  of every `ds/map` (and similar) call in the main body.
  Furthermore, inside these calls, any symbol that has
  the same name as a key in the map gets expanded to
  the appropriate form that includes a call to
  `ds/side-inputs`. For example, if you write the
  bindings `side-inputs {:a a-view}`, then `a`
  inside a `ds/map` gets expanded to `(:a (ds/side-inputs))`
  See the full example below.

  The body forms then get wrapped into a (ds/->> p),
  (so in most cases you don't have `p` appear explicitely,
  but you stil have access to it if you need several input nodes)
  written to a temp file, then the pipeline is ran (blockingly),
  then the file is read and the contents returned.

  If your last form yields a PCollectionView, use the
  special `:is-view` keyword to indicate it, because
  we can't `ds/write-edn-file` a view, so we'll first
  go back to the underlying pcoll.

  WIP : make it work in the cases were your final form
  yields a map or tuple of pcolls.

  We do not care to remove the temp file afterwards,
  since it is the point of os-supplied temp files that
  they need not be explicitely deleted.

  Note : I think `datasplash.api` need to be required `:as`
  `ds`, as is the current convention. TODO:investigate this,
  maybe consider it a bug.

  Examples :

  (direct []
  (ds/generate-input [1 2 3])
  (ds/map inc))

  ;; => [4 2 3]
  ;; (of course, order of elements is not kept by BEAM)

  (direct []
  (ds/generate-input [1])
  (ds/view)
  :is-view)

  ;; => [1]
  ;; as you see, it would be better if we did it differenty
  ;; according to what type of view it is. Here in case
  ;; of a singleton view, we could unwrap it for instance.

  (direct [a 2]
  (ds/generate-input [1 2 3])
  (ds/map (partial + a)))

  ;; => [3 5 4]

  (direct [a-view (->> (ds/generate-input [2] p)
                     ds/view)
         side-inputs {:a a-view}]
  (ds/generate-input [1 2 3])
  ;; for some reason you can't use partial...
  (ds/map (fn [x] (+ x a))))

  ;; => [3 4 5]

  (direct [a-view (->> (ds/generate-input [2] p)
                     ds/view)
         side-inputs {:a a-view}]
  (ds/generate-input [1 2 3])
  (ds/map (fn [x] (+ a x))
          {:name :opt-map-already-present}))

  ;; => [5 3 4]"
  {:style/indent 1}
  [& forms]
  (let [_TODO :just-ignore-this-you-reader
        p-gen (gensym "p")
        tmp-dir (gensym "tmp_dir")
        user-pipeline (gensym "user-pipeline")
        forms (walk/postwalk (fn [form] (if (= 'p form) p-gen form))
                             forms)
        [binding-vec & forms] forms
        side-inputs (->> (partition 2 binding-vec)
                         (filter #(= 'side-inputs (first %)))
                         first
                         second)
        special-end (#{:is-map :is-view :is-tuple} (last forms))
        forms (if special-end (butlast forms) forms)
        forms (cond->> forms
                side-inputs
                (walk/postwalk
                 (fn [form]
                   (cond
                     (and (symbol? form)
                          (side-inputs (keyword form)))
                     `(~(keyword form) (datasplash.api/side-inputs))
                     (and (list? form)
                          (#{'ds/map
                             'ds/map-kv
                             'ds/mapcat
                             'ds/filter
                             'ds/keep}
                           (first form)))
                     ;; if the ds/map is not in a ->>, user needs put
                     ;; a (possibly empty) options map. Because
                     ;; we'll insert one if not present, but we don't
                     ;; know where because we don't know if inside ->>
                     ;; or not.
                     (let [ensured-opts-map (cond-> form
                                              (not (some map? form))
                                              (concat '({})))]
                       (map (fn [subform]
                              (cond-> subform
                                (map? subform)
                                (update :side-inputs
                                        (fn [old-si-form]
                                          ;; can't use backtick
                                          ;; because it would namespace side-inputs
                                          (list 'merge 'side-inputs old-si-form)))))
                            ensured-opts-map))
                     :else form))))
        body (concat `(->> ~p-gen ~@forms) (when (= special-end :is-view)
                                             '(.getPCollection)))
        writing (case special-end
                  :is-map
                  `(write-map-edn ~tmp-dir ~user-pipeline)
                  :is-tuple _TODO
                  `(ds/write-edn-file (str ~tmp-dir "/o")
                                      {:num-shards 1}
                                      ~user-pipeline))

        reading (case special-end
                  :is-map
                  `(read-map-edn ~tmp-dir remove-0s)

                  `(read-edns-file (str ~tmp-dir "/o-00000-of-00001")))

        ]
    `(let [~tmp-dir (java.nio.file.Files/createTempDirectory nil (make-array java.nio.file.attribute.FileAttribute 0))
           ~p-gen (ds/make-pipeline [])
           ~@binding-vec
           ~user-pipeline ~body

           ]
       ~writing
       (clojure.test/is (= :done
                           (ds/wait-pipeline-result
                            (ds/run-pipeline ~p-gen))))
       ~reading)))

(comment
  (direct []
    (ds/generate-input [1 2 3])
    (ds/map inc))

  (direct []
    (ds/generate-input [1])
    (ds/view)
    :is-view)

  (direct [a 2]
    (ds/generate-input [1 2 3])
    (ds/map (partial + a)))

  (direct [a-view (->> (ds/generate-input [2] p)
                       ds/view)
           side-inputs {:a a-view}]
    (ds/generate-input [1 2 3])
    ;; for some reason you can't use partial...
    (ds/map (fn [x] (+ x a))))

  (direct [a-view (->> (ds/generate-input [2] p)
                       ds/view)
           side-inputs {:a a-view}]
    (ds/generate-input [1 2 3])
    (ds/map (fn [x] (+ a x))
            {:name :opt-map-already-present}))

  (direct [a-view (->> (ds/generate-input [2] p)
                       ds/view)
           b-view (->> (ds/generate-input [4] p)
                       ds/view)
           side-inputs {:a a-view}]
    (ds/generate-input [1 2 3])
    (ds/map (fn [x] (+ a x))
            {:name :opt-map-already-present
             :side-inputs {:b b-view}})))


(comment ;; wip

  (let [{:keys [leaf-nodes ancestor-nodes]}]
   (direct [in tree]
     (eiffel.utils/slice-tree-nodes :categories)
     :is-map)))
