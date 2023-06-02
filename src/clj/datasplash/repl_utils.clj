(ns datasplash.repl-utils
  (:require
   [clojure.walk :as walk]
   [datasplash.api :as ds]))

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
                          (#{'ds/map 'ds/mapcat #_TODO:autres} (first form)))
                     ;; if the ds/map is not in a ->>, you need to have
                     ;; a (possibily empty) options map. Because
                     ;; we'll insert one if not present, but we don't
                     ;; know where because we don't know if inside ->>
                     ;; or not.
                     (let [ensured-opts-map (cond-> form
                                              (not (some map? form))
                                              (concat '({})))]
                       (map (fn [subform]
                              (cond-> subform
                                (map? subform)
                                ;; TODO merge instead
                                (assoc :side-inputs 'side-inputs)))
                            ensured-opts-map))
                     :else form))))
        body (concat `(->> ~p-gen ~@forms) (when (= special-end :is-view)
                                             '(.getPCollection)))
        writing (case special-end
                  :is-map _TODO
                  :is-tuple _TODO
                  `(ds/write-edn-file (str ~tmp-dir "/o")
                                      {:num-shards 1}
                                      ~user-pipeline))

        ;; TODO: this is not the way.
        reading `(clojure.edn/read-string (str "[ " (slurp (str ~tmp-dir "/o-00000-of-00001")) " ]"))

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
       (require '[clojure.edn])
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
            {:name :opt-map-already-present})))


(comment ;; wip

  (let [{:keys [leaf-nodes ancestor-nodes]}]
   (direct [in tree]
     (eiffel.utils/slice-tree-nodes :categories)
     :is-map)))
