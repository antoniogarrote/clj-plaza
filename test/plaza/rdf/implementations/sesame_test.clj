(ns plaza.rdf.implementations.sesame-test
  (:use [plaza.rdf core] :reload-all)
  (:use [plaza.rdf.implementations sesame] :reload-all)
  (:use [clojure.test]))

;; we'll test with jena
(alter-root-model (build-model :sesame))


(deftest test-build-model-1
  (is (instance? plaza.rdf.implementations.sesame.SesameModel (build-model :sesame))))

(deftest test-build-model-2
  (is (instance? plaza.rdf.implementations.sesame.SesameModel (build-model))))

(deftest test-build-model-3
  (is (instance? plaza.rdf.implementations.sesame.SesameModel (build-model :non-existent))))

(deftest test-create-property-1
  (is (instance? plaza.rdf.implementations.sesame.SesameProperty (create-property *rdf-model* "http://test.com"))))

(deftest test-create-property-2
  (is (instance? plaza.rdf.implementations.sesame.SesameProperty (create-property *rdf-model* :rdf "test"))))

(deftest test-property-ns
  (is (= "http://test.com/" (qname-prefix (create-property (build-model) "http://test.com/test")))))

(deftest test-property-local
  (is (= "test" (qname-local (create-property (build-model) "http://test.com/test")))))

(deftest test-resource-id
  (is (= "http://test.com/test" (qname-local (create-property (build-model) "http://test.com/test")))))

(deftest test-create-resource-1
  (is (instance? plaza.rdf.implementations.sesame.SesameResource (create-resource *rdf-model* "http://test.com"))))

(deftest test-create-resource-2
  (is (instance? plaza.rdf.implementations.sesame.SesameResource (create-resource *rdf-model* :rdf "test"))))

(deftest test-resource-ns
  (is (= "http://test.com/" (qname-prefix (create-resource (build-model) "http://test.com/test")))))

(deftest test-resource-local
  (is (= "test" (qname-local (create-resource (build-model) "http://test.com/test")))))

(deftest test-resource-id
  (is (= "http://test.com/test" (resource-id (create-resource (build-model) "http://test.com/test")))))

(deftest test-blank-node-id
  (let [bnode (create-blank-node (build-model))]
    (is (= (.getID (to-java bnode)) (resource-id bnode)))))

(deftest test-sesame-literal
  (let [m (build-model :sesame)
        lit (create-literal m "testeado" "es")]
    (is (= (resource-id lit) "testeado@es"))
    (is (= (literal-value lit) "testeado"))
    (is (= (literal-language lit) "es"))
    (is (= (literal-datatype-uri lit) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
    (is (= (literal-datatype-obj lit) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
    (is (= (literal-lexical-form lit) "testeado"))))

(deftest test-sesame-typed-literal
  (let [model (build-model :sesame)
        res (create-typed-literal model 2)]
    (is (= "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>" (to-string res)))
    (is (not (is-blank res)))
    (is (not (is-resource res)))
    (is (not (is-property res)))
    (is (is-literal res))
    (is (= (resource-id res) "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>"))
    (is (= (literal-value res) 2))
    (is (= (literal-language res) ""))
    (is (= (literal-datatype-uri res) "http://www.w3.org/2001/XMLSchema#int"))))
