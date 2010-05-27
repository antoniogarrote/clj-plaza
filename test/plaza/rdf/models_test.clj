(ns plaza.rdf.models-test
  (:use [plaza.rdf core sparql models] :reload-all)
  (:use [plaza.rdf.implementations jena] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)

(defonce *test-model* (def-rdfs-model ["http://something/" "Good"] :name "http://test.com/name" :price ["http://test.com/" :price] :number :number))

(deftest test-props
  (is (= "http://something/Good") (type-uri *test-model*)))

(deftest test-to-map
  (let [m (to-map *test-model* [[:test ["http://test.com/" :name] "name"] [:test ["http://test.com/" :price] (d 120)] [:test :number (d 10)]])]
    (is (= m {:name (rdf-resource "name") :price (d 120) :number (d 10)}))))

(deftest test-to-pattern
  (let [p (to-pattern *test-model* [:name :price])]
    (is (= 4 (count p)))
    (is (= 2 (count (filter #(:optional (meta %1)) p))))))

(deftest test-to-pattern-2
  (let [p (to-pattern *test-model* "http://test.com/Test" [:name :price])]
    (is (= 4 (count p)))
    (is (= 2 (count (filter #(:optional (meta %1)) p))))
    (doseq [[s _p _o] p] (is (= "http://test.com/Test" (resource-id s))))))
