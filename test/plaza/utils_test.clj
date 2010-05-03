(ns plaza.utils-test
  (:use [plaza.utils] :reload-all)
  (:use [clojure.test]))

(deftest test-fold-list
  (is (= (fold-list [1 2 3 4])
         [[1 2] [3 4]])))

(deftest test-flatten-1
  (is (= (flatten-1 [:a [[:s :p :o] [:s :p :o]] :c [[:s :p :o]] :d])
         [:a [:s :p :o] [:s :p :o] :c [:s :p :o] :d])))
