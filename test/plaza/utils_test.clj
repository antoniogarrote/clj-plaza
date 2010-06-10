(ns plaza.utils-test
  (:use [plaza.utils] :reload-all)
  (:use [clojure.test]))

(deftest test-fold-list
  (is (= (fold-list [1 2 3 4])
         [[1 2] [3 4]])))

(deftest test-flatten-1
  (is (= (set (flatten-1 [:a [[:s :p :o] [:s :p :o]] :c [[:s :p :o]] :d]))
         (set [:a [:s :p :o] :c :d]))))

(deftest test-flatten-1-preserves-meta
  (let [to-flatten (with-meta [:a [[:s :p :o] [:s :p :o]] :c [[:s :p :o]] :d] {:flatten true})
        flattened (flatten-1 to-flatten)]
    (is (= (meta to-flatten) (meta flattened)))))

(deftest test-cmd-param-to-keywords
  (is (= "hola" (cmd-param-to-keyword "hola")))
  (is (= :hola (cmd-param-to-keyword :hola)))
  (is (= :hola (cmd-param-to-keyword "-hola"))))

(deftest test-check-default-values
  (let [optsp (check-default-values {:password "foo"} {:password "bar" :username "guest"})]
    (is (= "foo" (:password optsp)))
    (is (= "guest" (:username optsp)))))
