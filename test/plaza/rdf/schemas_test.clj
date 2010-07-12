(ns plaza.rdf.schemas-test
  (:use [plaza.rdf core sparql schemas] :reload-all)
  (:use [plaza.rdf.implementations jena] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)
(use 'plaza.rdf.vocabularies.foaf)
(init-vocabularies)

(defonce *test-model* (make-rdfs-schema ["http://something/" "Good"]
                                        :name   {:uri "http://test.com/name"      :range :string}
                                        :price  {:uri ["http://test.com/" :price] :range :float}
                                        :number {:uri :number                     :range :int}))

(deftest test-props
  (is (= "http://something/Good" (str (type-uri *test-model*)))))

(deftest test-add-remove-prop
  (do (let [modelp (-> *test-model*
                       (add-property :wadus "http://test.com/wadus" :float)
                       (add-property :foo "http://test.com/foo" "http://test.com/ranges/foo"))
            modelpp (-> modelp
                        (remove-property-by-uri "http://test.com/foo")
                        (remove-property-by-alias :wadus))]
        (= :foo (property-alias modelp "http://test.com/foo"))
        (= :wadus (property-alias modelp "http://test.com/wadus"))
        (is (nil? (property-alias modelpp :wadus)))
        (is (nil? (property-alias modelpp :foo))))))

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

(deftest test-property-uri
  (is (= "http://test.com/name" (str (property-uri *test-model* :name)))))

(deftest test-property-alias
  (is (= :name (property-alias *test-model* "http://test.com/name"))))

(deftest test-property-parse-value
  (is (= 2 (parse-prop-value *test-model* :number "2"))))

(deftest test-schema-to-triples
  (let [ts (to-rdf-triples foaf:Agent-schema)]
    (is (= 37 (count ts)))))
