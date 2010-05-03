(ns plaza.components.rdf-test
  (:use [plaza.components.rdf] :reload-all)
  (:use [clojure.test]))

(deftest test-create-model
  (is clojure.lang.Agent (build-model)))


(deftest test-with-rdf-ns
  (let [before *rdf-ns*
        new-ns "hello"
        result (with-rdf-ns new-ns
                 *rdf-ns*)]
    (is (= new-ns result))
    (is (= before *rdf-ns*))))

(deftest test-with-rdf-model
  (let [before-ns *rdf-ns*
        before-model *rdf-model*
        new-ns "hello"
        new-model "bye"
        result (with-rdf-ns new-ns
                 (with-rdf-model new-model
                   [*rdf-ns* *rdf-model*]))]
    (is (= [new-ns new-model] result))
    (is (= before-ns *rdf-ns*))
    (is (= before-model *rdf-model*))))

(deftest test-make-property
  (let [m (build-model)
        p1 (with-rdf-model m
             (rdf-property rdf :hola))
        p2 (with-rdf-model m
             (rdf-property rdf:type))]
    (is (= (str p1) "http://www.w3.org/1999/02/22-rdf-syntax-ns#hola"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))))

(deftest test-make-resource
  (let [m (build-model)
        p1 (with-rdf-model m
             (rdf-resource rdf :Mundo))
        p2 (with-rdf-model m
             (rdf-property rdfs:Class))]
    (is (= (str p1) "http://www.w3.org/1999/02/22-rdf-syntax-ns#Mundo"))
    (is (= (str p2) "http://www.w3.org/2000/01/rdf-schema#Class"))))


(deftest test-make-literal
  (let [m (build-model)
        p1 (with-rdf-model m
             (rdf-literal "test"))
        p2 (with-rdf-model m
             (rdf-literal "test" "es"))]
    (is (= (str p1) "test^^http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
    (is (= (str p2) "test@es"))))

(deftest test-make-typed-literal
  (let [m (build-model)
        p1 (with-rdf-model m
             (rdf-typed-literal 2))
        p2 (with-rdf-model m
             (rdf-typed-literal 2 :anyuri))]
    (is (= (str p1) "2^^http://www.w3.org/2001/XMLSchema#int"))
    (is (= (str p2) "2^^http://www.w3.org/2001/XMLSchema#anyURI"))))

(deftest test-triple-subject
  (let [m (build-model)
        p1 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject :A)))
        p2 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject [rdf :A])))]
    (is (= (str p1) "http://test.com/A"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#A"))))

(deftest test-triple-predicate
  (let [m (build-model)
        p1 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-predicate :p)))
        p2 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-subject [rdf :p])))]
    (is (= (str p1) "http://test.com/p"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#p"))))

(deftest test-triple-object
  (let [m (build-model)
        p1 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-object :p)))
        p2 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-object [rdf :p])))
        p3 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-object (l "test"))))
        p4 (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (triple-object (d 2))))]
    (is (= (str p1) "http://test.com/p"))
    (is (= (str p2) "http://www.w3.org/1999/02/22-rdf-syntax-ns#p"))
    (is (= (str p3) "test^^http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
    (is (= (str p4) "2^^http://www.w3.org/2001/XMLSchema#int"))))

(deftest test-rdf-triple-a
  (let [m (build-model)
        ts (with-rdf-model m
             (with-rdf-ns "http://test.com/"
               (rdf-triple [:a :b :c])))]
    (is (= (count ts) 3))
    (is (= (str (nth ts 0)) "http://test.com/a"))
    (is (= (str (nth ts 1)) "http://test.com/b"))
    (is (= (str (nth ts 2)) "http://test.com/c"))))

(deftest test-rdf-triple-b
  (let [m (build-model)
        ts (with-rdf-model m
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
    (with-rdf-model m (model-add-triples [[:a :b :c] [:d :e :f] [:g [:h :i :j :k]]]))
    (is (= 4 (.size (.toList (.listStatements @m)))))))

(deftest test-add-triples-2
  (let [m (build-model)]
    (with-rdf-model m (model-add-triples (make-triples [[:a :b :c] [:d :e :f] [:g [:h :i :j :k]]])))
    (is (= 4 (.size (.toList (.listStatements @m)))))))

(deftest test-sparql-to-pattern-1
  (let [res (sparql-to-pattern "SELECT ?v WHERE { ?v ?p 2 . optional {?v ?q 3 . ?v ?q 4 } }")]
    (is (= (count res) 3))
    (is (= (count (filter #(:optional (meta %1)) res)) 2))))

(deftest test-sparql-to-pattern-2
  (let [res (sparql-to-pattern "SELECT ?v WHERE { ?v ?p 2 . ?v ?q 3 . ?v ?q 4 }")]
    (is (= (count res) 3))
    (is (= (count (filter #(:optional (meta %1)) res)) 0))))

(deftest test-build-query-1
  (let [query (sparql-to-query "PREFIX rdf: <http://test.com> SELECT ?v WHERE { ?v ?p 2 .?v rdf:algo 3 . ?v ?q 4  }")
        built-query (build-query query)]
    (is (= (.isEmpty (first (.getElements (.getQueryPattern built-query))))) false)
    (is (= (.getResultVars built-query) ["v"]))
    (is (= (.getQueryType built-query) com.hp.hpl.jena.query.Query/QueryTypeSelect))))

(deftest test-build-query-2
  (let [query (sparql-to-query "PREFIX  dc:   <http://purl.org/dc/elements/1.1/>\nPREFIX  a:    <http://www.w3.org/2000/10/annotation-ns#>\nPREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n\nSELECT  ?annot\nWHERE\n  { ?annot  a:annotates  <http://www.w3.org/TR/rdf-sparql-query/> ;\n            dc:date      ?date .\n    FILTER ( ?date < \"2005-01-01T00:00:00Z\"^^xsd:dateTime )\n  }\n")]
    (is (= (count (:filters query)) 1))
    (is (= (count (:pattern query)) 2))))

(deftest test-optional
  (let [is-optional (optional [:foo])]
    (is (:optional (meta (first is-optional))))))

(deftest test-optional-2
  (let [is-optional (optional [:foo :bar])
        is-opt (opt [:foo :bar])]
    (is (= is-optional is-opt))))

(deftest test-make-pattern-build-1
     (let [pattern (make-pattern [[:?x rdf:type :http://test.com/Test]
                                  (optional [:?y :?z (d 2)])])
           query (defquery
                   (query-set-vars [:?y])
                   (query-set-type :select)
                   (query-set-pattern pattern))
           query-str (.toString (build-query query))
           parsed-pattern (sparql-to-pattern query-str)]
       (is (= 1 (count (filter #(:optional (meta %1)) parsed-pattern))))))

(deftest test-make-pattern-build-2
     (let [pattern (make-pattern [[:?x rdf:type :http://test.com/Test]
                                  (opt [:?y :?z (d 2)]
                                       [:?l :?m (l "cat")])])
           query (defquery
                   (query-set-vars [:?y])
                   (query-set-type :select)
                   (query-set-pattern pattern))
           query-str (.toString (build-query query))
           parsed-pattern (sparql-to-pattern query-str)]
       (is (= 2 (count (filter #(:optional (meta %1)) parsed-pattern))))))

(deftest test-check
  (is (check (is-literal?) (l "test"))))

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
        result (filter (triple? (not? (object-and? (is-literal?))))
                       tps)]
    (is (= 2 (count result)))))

(deftest test-filters-4
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple? (not? (object? (is-literal?))))
                       tps)]
    (is (= 2 (count result)))))

(deftest test-filters-5
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result (filter (triple? (object? (is-resource?)))
                       tps)]
    (is (= 2 (count result)))))

(deftest test-filters-6
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola")] [:g [:h :i :j :k]]])
        result-1 (filter (triple? (object? (literal? "hola")))
                         tps)
        result-2 (filter (triple? (object? (literal? "adios")))
                         tps)]
    (is (= 1 (count result-1)))
    (is (= 0 (count result-2)))))

(deftest test-filters-7
  (let [tps (make-triples [[:a :b (d 2)] [:d :e (l "hola" "es")] [:g [:h :i :j :k]]])
        result-1 (filter (triple? (object? (literal? "hola" "es")))
                         tps)
        result-2 (filter (triple? (object? (literal? "hola" "en")))
                         tps)]
    (is (= 1 (count result-1)))
    (is (= 0 (count result-2)))))

(deftest test-filters-8
  (is (check (qname-prefix? :rdf) (triple-subject rdf:Property)))
  (is (not (check (qname-prefix? :rdf) (triple-subject rdfs:Class)))))

(deftest test-predicate-2
  (is (check (datatype? :int) (d 1)))
  (is (not (check (datatype? :int) (d 2.0))))
  (is (check (datatype? "http://www.w3.org/2001/XMLSchema#int") (d 1))))

(deftest test-predicate-3
  (is (= true (check (literal-fn? (fn[l] true )) (l "cat"))))
  (is (= false (check (literal-fn? (fn [l] false)) (l "cat")))))

(deftest test-abstraction-1
  (let [triples (make-triples [[:a :b :c]])
        pattern (make-pattern [[:?x :b :c]])]
    (is (= pattern (triples-abstraction triples (subject? (uri? "http://plaza.org/ontologies/a")) {:subject :?x})))))

(deftest test-build-filters-2
  (let [gt (.toString (build-filter (make-filter :> :?x (d 3))))
        gt-2 (.toString (build-filter (make-filter :> :?x (make-filter :bound :?y))))]
    (is (= gt "( ?x > \"3\"^^xsd:int )"))
    (is (= gt-2 "( ?x > bound(?y) )"))))

(deftest test-build-filters-3
  (let [*tq* "PREFIX  dc:   <http://purl.org/dc/elements/1.1/>\nPREFIX  a:    <http://www.w3.org/2000/10/annotation-ns#>\nPREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n\nSELECT  ?annot\nWHERE\n  { ?annot  a:annotates  <http://www.w3.org/TR/rdf-sparql-query/> ;\n            dc:date      ?date .\n    FILTER ( bound(?date) < \"2005-01-01T00:00:00Z\"^^xsd:dateTime )\n  }\n"
        res (.toString (build-filter (first (:filters (sparql-to-query *tq*)))))]
    (is (= res "( bound(?date) < \"2005-01-01T00:00:00Z\"^^xsd:dateTime )"))))
