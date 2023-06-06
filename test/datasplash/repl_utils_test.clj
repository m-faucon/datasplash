(ns datasplash.repl-utils-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datasplash.api :as ds]
   [datasplash.repl-utils :as sut]))

(deftest direct-test
  (testing "most basic usage"
    (is (= #{2 3 4}
           (set (sut/direct []
                            (ds/generate-input [1 2 3])
                            (ds/map inc))))))

  (testing "is-view"
    (is (= #{1}
           (set (sut/direct []
                            (ds/generate-input [1])
                            (ds/view)
                            :is-view)))))

  (testing "normal let bindings"
    (is (= #{3 4 5}
           (set (sut/direct [a 2]
                            (ds/generate-input [1 2 3])
                            (ds/map (partial + a)))))))

  (testing "side-inputs"
    (testing "basic usage"
      (is (= #{3 4 5}
             (set (sut/direct [a-view (->> (ds/generate-input [2] p)
                                           ds/view)
                               side-inputs {:a a-view}]
                              (ds/generate-input [1 2 3])
                              ;; for some reason you can't use partial...
                              (ds/map (fn [x] (+ x a))))))))

    (testing "opt map already present"
      (is (= #{3 4 5}
             (set (sut/direct [a-view (->> (ds/generate-input [2] p)
                                           ds/view)
                               side-inputs {:a a-view}]
                              (ds/generate-input [1 2 3])
                              (ds/map (fn [x] (+ a x))
                                      {:name :opt-map-already-present}))))))

    (testing "opt map already present, with side-inputs bindings"
      (is (= #{3 4 5}
             (set (sut/direct [a-view (->> (ds/generate-input [2] p)
                                           ds/view)
                               b-view (->> (ds/generate-input [4] p)
                                           ds/view)
                               side-inputs {:a a-view}]
                              (ds/generate-input [1 2 3])
                              (ds/map (fn [x] (+ a x))
                                      {:name :opt-map-already-present
                                       :side-inputs {:b b-view}}))))))))
