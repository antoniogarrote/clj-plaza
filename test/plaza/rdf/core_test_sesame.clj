(ns plaza.rdf.core-test-sesame
  (:use [plaza.rdf core] :reload-all)
  (:use [plaza.rdf.implementations sesame] :reload-all)
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

(def *test-xml-blanks* "<?xml version=\"1.0\"?>
<rdf:RDF xmlns:csf=\"http://schemas.microsoft.com/connectedservices/pm#\" xmlns:owl=\"http://www.w3.org/2002/07/owl#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">
    <rdf:Description rdf:about=\"urn:upn_abc\">
        <csf:Phone>
            <rdf:Description>
                <csf:Phone-Home-Primary>425-555-0111</csf:Phone-Home-Primary>
                <csf:Phone-Mobile-Other>425-555-0114</csf:Phone-Mobile-Other>
                <csf:Phone-Office-Other>425-555-0115</csf:Phone-Office-Other>
              </rdf:Description>
        </csf:Phone>
     </rdf:Description>
</rdf:RDF>")

;; we'll test with Sesame
(init-sesame-framework)

(deftest test-create-model-sesame
  (is-model (build-model :sesame)))


(deftest test-with-rdf-ns-sesame
  (let [before *rdf-ns*
        new-ns "hello"
        result (with-rdf-ns new-ns
                 *rdf-ns*)]
    (is (= new-ns result))
    (is (= before *rdf-ns*))))

(deftest test-with-model-sesame
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

(deftest test-make-property-sesame
  (let [m (build-model :sesame)
        p1 (with-model m
             (rdf-property rdf :hola))
        p2 (with-model m
             (rdf-property rdf:type))]
    (is (= (to-string p1) "http://www.w3.org/1999/02/22-rdf-syntax-ns#hola"))
    (is (= (to-string p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))))

(deftest test-make-resource-sesame
  (let [m (build-model :sesame)
        p1 (with-model m
             (rdf-resource rdf :Mundo))
        p2 (with-model m
             (rdf-property rdfs:Class))]
    (is (= (to-string p1) "http://www.w3.org/1999/02/22-rdf-syntax-ns#Mundo"))
    (is (= (to-string p2) "http://www.w3.org/2000/01/rdf-schema#Class"))))


(deftest test-make-literal-sesame
  (let [m (build-model :sesame)
        p1 (with-model m
             (rdf-literal "test"))
        p2 (with-model m
             (rdf-literal "test" "es"))]
    (is (= (to-string p1) "test"))
    (is (= (to-string p2) "test@es"))))

(deftest test-make-typed-literal-sesame
  (let [m (build-model :sesame)
        p1 (with-model m
             (rdf-typed-literal 2))
        p2 (with-model m
             (rdf-typed-literal 2 :anyuri))]
    (is (= (to-string p1) "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>"))
    (is (= (to-string p2) "\"2\"^^<http://www.w3.org/2001/XMLSchema#anyURI>"))))

(deftest test-triple-subject-sesame
  (let [m (build-model :sesame)
        p1 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject :A)))
        p2 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject [rdf :A])))]
    (is (= (to-string p1) "http://test.com/A"))
    (is (= (to-string p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#A"))))

(deftest test-triple-predicate-sesame
  (let [m (build-model :sesame)
        p1 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-predicate :p)))
        p2 (with-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject [rdf :p])))]
    (is (= (to-string p1) "http://test.com/p"))
    (is (= (to-string p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#p"))))

(deftest test-triple-object-sesame
  (let [m (build-model :sesame)
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
    (is (= (to-string p1) "http://test.com/p"))
    (is (= (to-string p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#p"))
    (is (= (to-string p3) "test"))
    (is (= (to-string p4) "\"2\"^^<http://www.w3.org/2001/XMLSchema#int>"))))

(deftest test-rdf-triple-a-sesame
  (let [m (build-model :sesame)
        ts (with-model m
             (with-rdf-ns "http://test.com/"
               (rdf-triple [:a :b :c])))]
    (is (= (count ts) 3))
    (is (= (to-string (nth ts 0)) "http://test.com/a"))
    (is (= (to-string (nth ts 1)) "http://test.com/b"))
    (is (= (to-string (nth ts 2)) "http://test.com/c"))))

(deftest test-rdf-triple-b-sesame
  (let [m (build-model :sesame)
        ts (with-model m
             (with-rdf-ns "http://test.com/"
               (rdf-triple [:a  [:b :c
                                 :d :e]])))]
    (is (= (count ts) 2))
    (let [fts (nth ts 0)
          sts (nth ts 1)]
      (is (= (to-string (nth fts 0)) "http://test.com/a"))
      (is (= (to-string (nth fts 1)) "http://test.com/b"))
      (is (= (to-string (nth fts 2)) "http://test.com/c"))
      (is (= (to-string (nth sts 0)) "http://test.com/a"))
      (is (= (to-string (nth sts 1)) "http://test.com/d"))
      (is (= (to-string (nth sts 2)) "http://test.com/e")))))

(deftest test-add-triples-sesame
  (let [m (build-model :sesame)]
    (with-model m (model-add-triples [[:a :b :c] [:d :e :f] [:g [:h :i :j :k]]]))
    (is (= 4 (count (walk-triples m (fn [s p o] [s p o])))))))

(deftest test-add-triples-2-sesame
  (let [m (build-model :sesame)]
    (with-model m (model-add-triples (make-triples [[:a :b :c] [:d :e :f] [:g [:h :i :j :k]]])))
    (is (= 4 (count (walk-triples m (fn [s p o] [s p o])))))))

(deftest test-remove-triples-1-sesame
  (let [m (defmodel
             (model-add-triples (make-triples [[:a :b (d 2)]]))
             (model-add-triples (make-triples [[:e :f (l "test")]])))]
    (do (with-model m (model-remove-triples (make-triples [[:a :b (d 2)]])))
        (= 1 (count (model-to-triples m))))))

(deftest test-optional-sesame
  (let [is-optional (optional [:foo])]
    (is (:optional (meta (first is-optional))))))

(deftest test-optional-2-sesame
  (let [is-optional (optional [:foo :bar])
        is-opt (opt [:foo :bar])]
    (is (= is-optional is-opt))))

(deftest test-document-to-model-1-sesame
  (let [m (build-model :sesame)
        _m (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes *test-xml*)) :xml))]
    (is (= (count (model-to-triples m)) 3))))

(deftest test-document-to-model-2-sesame
  (let [m (build-model :sesame)
        _m (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes *test-xml-blanks*)) :xml))]
    (is (= (count (model-to-triples m)) 4))
    (is (or (is-blank-node (o (first (model-to-triples m))))
            (is-blank-node (o (second (model-to-triples m))))
            (is-blank-node (o (nth (model-to-triples m) 2)))
            (is-blank-node (o (nth (model-to-triples m) 3)))))))

(deftest test-find-resources-sesame
  (let [m (build-model :sesame)
        _m (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes *test-xml*)) :xml))
        res (find-resources m)]
    (is (= (count res) 2))))

(deftest test-find-resource-uris-sesame
  (let [m (build-model :sesame)
        _m (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes *test-xml*)) :xml))
        res (find-resource-uris m)]
    (is (= (count res) 2))))

(deftest test-blank-node-sesame
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

(deftest test-blank-node-is-sesame
  (is (not (is-blank-node :?a)))
  (is (not (is-blank-node (d 2))))
  (is (not (is-blank-node (l "test"))))
  (is (not (is-blank-node (rdf-resource "http://test.com/Test")))))

(deftest test-has-meta-sesame
  (is (:triples (meta (make-triples [[:a :b :c]])))))
