(ns plaza.rdf.core-test
  (:use [plaza.rdf core predicates sparql] :reload-all)
  (:use [clojure.test]))

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


(deftest test-create-model
  (is clojure.lang.Agent (build-model)))


(deftest test-with-rdf-ns
  (let [before *rdf-ns*
        new-ns "hello"
        result (with-rdf-ns new-ns
                 *rdf-ns*)]
    (is (= new-ns result))
    (is (= before *rdf-ns*))))

(deftest test-with-model
  (let [before-ns *rdf-ns*
        before-model *rdf-model*
        new-ns "hello"
        new-model "bye"
        result (with-rdf-ns new-ns
                 (with-model new-model
                   [*rdf-ns* *rdf-model*]))]
    (is (= [new-ns new-model] result))
    (is (= before-ns *rdf-ns*))
    (is (= before-model *rdf-model*))))

(deftest test-make-property
  (let [m (build-model)
        p1 (with-model m
             (rdf-property rdf :hola))
        p2 (with-model m
             (rdf-property rdf:type))]
    (is (= (str p1) "http://www.w3.org/1999/02/22-rdf-syntax-ns#hola"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))))

(deftest test-make-resource
  (let [m (build-model)
        p1 (with-model m
             (rdf-resource rdf :Mundo))
        p2 (with-model m
             (rdf-property rdfs:Class))]
    (is (= (str p1) "http://www.w3.org/1999/02/22-rdf-syntax-ns#Mundo"))
    (is (= (str p2) "http://www.w3.org/2000/01/rdf-schema#Class"))))


(deftest test-make-literal
  (let [m (build-model)
        p1 (with-model m
             (rdf-literal "test"))
        p2 (with-model m
             (rdf-literal "test" "es"))]
    (is (= (str p1) "test^^http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
    (is (= (str p2) "test@es"))))

(deftest test-make-typed-literal
  (let [m (build-model)
        p1 (with-model m
             (rdf-typed-literal 2))
        p2 (with-model m
             (rdf-typed-literal 2 :anyuri))]
    (is (= (str p1) "2^^http://www.w3.org/2001/XMLSchema#int"))
    (is (= (str p2) "2^^http://www.w3.org/2001/XMLSchema#anyURI"))))

(deftest test-triple-subject
  (let [m (build-model)
        p1 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject :A)))
        p2 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject [rdf :A])))]
    (is (= (str p1) "http://test.com/A"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#A"))))

(deftest test-triple-predicate
  (let [m (build-model)
        p1 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-predicate :p)))
        p2 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject [rdf :p])))]
    (is (= (str p1) "http://test.com/p"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#p"))))

(deftest test-triple-object
  (let [m (build-model)
        p1 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-object :p)))
        p2 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-object [rdf :p])))
        p3 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-object (l "test"))))
        p4 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-object (d 2))))]
    (is (= (str p1) "http://test.com/p"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#p"))
    (is (= (str p3) "test^^http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
    (is (= (str p4) "2^^http://www.w3.org/2001/XMLSchema#int"))))

(deftest test-rdf-triple-a
  (let [m (build-model)
        ts (with-model m
             (with-rdf-ns "http://test.com/"
               (rdf-triple [:a :b :c])))]
    (is (= (count ts) 3))
    (is (= (str (nth ts 0)) "http://test.com/a"))
    (is (= (str (nth ts 1)) "http://test.com/b"))
    (is (= (str (nth ts 2)) "http://test.com/c"))))

(deftest test-rdf-triple-b
  (let [m (build-model)
        ts (with-model m
             (with-rdf-ns "http://test.com/"
               (rdf-triple [:a  [:b :c
                                 :d :e]])))]
    (is (= (count ts) 2))
    (let [fts (nth ts 0)
          sts (nth ts 1)]
      (is (= (str (nth fts 0)) "http://test.com/a"))
      (is (= (str (nth fts 1)) "http://test.com/b"))
      (is (= (str (nth fts 2)) "http://test.com/c"))
      (is (= (str (nth sts 0)) "http://test.com/a"))
      (is (= (str (nth sts 1)) "http://test.com/d"))
      (is (= (str (nth sts 2)) "http://test.com/e")))))

(deftest test-add-triples
  (let [m (build-model)]
    (with-model m (model-add-triples [[:a :b :c] [:d :e :f] [:g [:h :i :j :k]]]))
    (is (= 4 (.size (.toList (.listStatements @m)))))))

(deftest test-add-triples-2
  (let [m (build-model)]
    (with-model m (model-add-triples (make-triples [[:a :b :c] [:d :e :f] [:g [:h :i :j :k]]])))
    (is (= 4 (.size (.toList (.listStatements @m)))))))

(deftest test-optional
  (let [is-optional (optional [:foo])]
    (is (:optional (meta (first is-optional))))))

(deftest test-optional-2
  (let [is-optional (optional [:foo :bar])
        is-opt (opt [:foo :bar])]
    (is (= is-optional is-opt))))

(deftest test-document-to-model-1
  (let [m (build-model)
        _m (with-model m (document-to-model :xml (java.io.ByteArrayInputStream. (.getBytes *test-xml*))))]
    (is (= (count (model-to-triples m)) 3))))

(deftest test-find-resources
  (let [m (build-model)
        _m (with-model m (document-to-model :xml (java.io.ByteArrayInputStream. (.getBytes *test-xml*))))
        res (find-resources m)]
    (is (= (count res) 2))))

(deftest test-find-resource-uris
  (let [m (build-model)
        _m (with-model m (document-to-model :xml (java.io.ByteArrayInputStream. (.getBytes *test-xml*))))
        res (find-resource-uris m)]
    (is (= (count res) 2))))

(deftest test-blank-node
  (let [b1 (blank-node)
        b2 (b)
        b3 (blank-node :a)
        b4 (b :a)]
    (is (is-blank-node b1))
    (is (is-blank-node b2))
    (is (is-blank-node b3))
    (is (is-blank-node b4))
    (is (= :a (keyword (blank-node-id b3))))
    (is (= :a (keyword (blank-node-id b4))))))

(deftest test-blank-node-is
  (is (not (is-blank-node :?a)))
  (is (not (is-blank-node (d 2))))
  (is (not (is-blank-node (l "test"))))
  (is (not (is-blank-node (rdf-resource "http://test.com/Test")))))
