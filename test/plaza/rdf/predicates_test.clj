(ns plaza.rdf.predicates-test
  (:use [plaza.rdf core predicates] :reload-all)
  (:use [plaza.rdf.implementations jena] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)

;; rdf/xml used in the tests
(def *test-xml* "<rdf:RDF
    xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"
    xmlns:test=\"http://plaza.org/ontologies/\" >
  <rdf:Description rdf:about=\"http://plaza.org/ontologies/a\">
    <test:c rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">3</test:c>
    <test:b rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">2</test:b>
  </rdf:Description>
  <rdf:Description rdf:about=\"http://plaza.org/ontologies/d\">
    <test:e rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">3</test:e>
  </rdf:Description>
</rdf:RDF>")

(deftest test-check
  (is (triple-check-apply (is-literal?) (l "test"))))

(deftest test-filters-1
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple-or?
                        (not? (object-and? (is-literal?)))
                        (object-and? (is-literal?)))
                       tps)]
    (is (= 4 (count result)))))

(deftest test-filters-2
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple-and?
                        (not? (object-and? (is-literal?)))
                        (object-and? (is-literal?)))
                       tps)]
    (is (= 0 (count result)))))

(deftest test-filters-3
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple-check (not? (object-and? (is-literal?))))
                       tps)]
    (is (= 2 (count result)))))

(deftest test-filters-4
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple-check (not? (object? (is-literal?))))
                       tps)]
    (is (= 2 (count result)))))

(deftest test-filters-5
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple-check (object? (is-resource?)))
                       tps)]
    (is (= 2 (count result)))))

(deftest test-filters-6
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result-1 (filter (triple-check (object? (literal? "hola")))
                         tps)
        result-2 (filter (triple-check (object? (literal? "adios")))
                         tps)]
    (is (= 1 (count result-1)))
    (is (= 0 (count result-2)))))

(deftest test-filters-7
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola" "es")] [:g [:h :i :j :k]]])
        result-1 (filter (triple-check (object? (literal? "hola" "es")))
                         tps)
        result-2 (filter (triple-check (object? (literal? "hola" "en")))
                         tps)]
    (is (= 1 (count result-1)))
    (is (= 0 (count result-2)))))

(deftest test-filters-8
  (is (triple-check-apply (qname-prefix? :rdf) (triple-subject rdf:Property)))
  (is (not (triple-check-apply (qname-prefix? :rdf) (triple-subject rdfs:Class)))))

(deftest test-filters-9
  (let [tps (make-triples [[(b :a) :p (b) ] [:d :e (l "hola" "es")]])
        result-1 (filter (triple-check (subject? (blank-node? :a)))
                         tps)
        result-2 (filter (triple-check (object? (is-blank-node?)))
                         tps)]
    (is (= 1 (count result-1)))
    (is (= 1 (count result-2)))))

(deftest test-tc-1
  (let [tps (make-triples [[(b :a) :p (b) ] [:d :e (l "hola" "es")]])
        result-1 (filter (tc (subject? (blank-node? :a)))
                         tps)
        result-2 (filter (tc (object? (is-blank-node?)))
                         tps)]
    (is (= 1 (count result-1)))
    (is (= 1 (count result-2)))))


(deftest test-predicate-2
  (is (triple-check-apply (datatype? :int) (d 1)))
  (is (not (triple-check-apply (datatype? :int) (d 2.0))))
  (is (triple-check-apply (datatype? "http://www.w3.org/2001/XMLSchema#int") (d 1))))

(deftest test-predicate-3
  (is (= true (triple-check-apply (literal-fn? (fn[l] true )) (l "cat"))))
  (is (= false (triple-check-apply (literal-fn? (fn [l] false)) (l "cat")))))
