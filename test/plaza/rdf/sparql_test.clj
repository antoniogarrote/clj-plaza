(ns plaza.rdf.sparql-test
  (:use [plaza.rdf predicates core sparql] :reload-all)
  (:use [plaza.rdf.implementations jena] :reload-all)
  (:use [clojure.test]))

;; we'll test with jena
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


(deftest test-framework-sparql-to-pattern-1
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        res (parse-sparql-to-pattern framework "SELECT ?v WHERE { ?v ?p 2 . optional {?v ?q 3 . ?v ?q 4 } }")]
    (is (= (count res) 3))
    (is (= (count (filter #(:optional (meta %1)) res)) 2))))

(deftest test-framework-sparql-to-pattern-2
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        res (parse-sparql-to-pattern framework "SELECT ?v WHERE { ?v ?p 2 . ?v ?q 3 . ?v ?q 4 }")]
    (is (= (count res) 3))
    (is (= (count (filter #(:optional (meta %1)) res)) 0))))


(deftest test-sparql-to-pattern-1
  (let [res (sparql-to-pattern "SELECT ?v WHERE { ?v ?p 2 . optional {?v ?q 3 . ?v ?q 4 } }")]
    (is (= (count res) 3))
    (is (= (count (filter #(:optional (meta %1)) res)) 2))))

(deftest test-sparql-to-pattern-2
  (let [res (sparql-to-pattern "SELECT ?v WHERE { ?v ?p 2 . ?v ?q 3 . ?v ?q 4 }")]
    (is (= (count res) 3))
    (is (= (count (filter #(:optional (meta %1)) res)) 0))))

(deftest test-build-query-framework-1
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        query (parse-sparql-to-query framework "PREFIX rdf: <http://test.com> SELECT ?v WHERE { ?v ?p 2 .?v rdf:algo 3 . ?v ?q 4  }")
        built-query (build-query framework query)]
    (is (= (.isEmpty (first (.getElements (.getQueryPattern built-query))))) false)
    (is (= (.getResultVars built-query) ["v"]))
    (is (= (.getQueryType built-query) com.hp.hpl.jena.query.Query/QueryTypeSelect))))

;; (deftest test-build-query-1
;;   (let [query (sparql-to-query "PREFIX rdf: <http://test.com> SELECT ?v WHERE { ?v ?p 2 .?v rdf:algo 3 . ?v ?q 4  }")
;;         built-query (build-query query)]
;;     (is (= (.isEmpty (first (.getElements (.getQueryPattern built-query))))) false)
;;     (is (= (.getResultVars built-query) ["v"]))
;;     (is (= (.getQueryType built-query) com.hp.hpl.jena.query.Query/QueryTypeSelect))))

(deftest test-build-query-2
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        query (parse-sparql-to-query framework "PREFIX  dc:   <http://purl.org/dc/elements/1.1/>\nPREFIX  a:    <http://www.w3.org/2000/10/annotation-ns#>\nPREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n\nSELECT  ?annot\nWHERE\n  { ?annot  a:annotates  <http://www.w3.org/TR/rdf-sparql-query/> ;\n            dc:date      ?date .\n    FILTER ( ?date < \"2005-01-01T00:00:00Z\"^^xsd:dateTime )\n  }\n")]
    (is (= (count (:filters query)) 1))
    (is (= (count (:pattern query)) 2))))

(deftest test-build-query-3
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        res (.toString (build-query framework
                                    (defquery
                                      (query-set-vars [:?y])
                                      (query-set-type :select)
                                      (query-set-pattern
                                       (make-pattern [[:?y :?x :?p]]))
                                      (query-set-filters [(make-filter :> :?y (d 2))]))))]
    (= res "SELECT  ?y\nWHERE\n  { ?y  ?x  ?p .\n    FILTER ( ?y > \"2\"^^<http://www.w3.org/2001/XMLSchema#int> )\n  }\n")))


(deftest test-build-query-4
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        query-str (str (build-query framework
                                    (defquery
                                      (query-set-vars [?a])
                                      (query-set-pattern (make-pattern [[:a :b :c]]))
                                      (query-set-type :select)
                                      (query-set-limit 2)
                                      (query-set-distinct)
                                      (query-set-offset 5)
                                      (query-set-reduced))))]
    (is (not (= -1 (.indexOf query-str "REDUCED"))))
    (is (not (= -1 (.indexOf query-str "DISTINCT"))))
    (is (not (= -1 (.indexOf query-str "OFFSET"))))
    (is (not (= -1 (.indexOf query-str "LIMIT"))))))

(deftest test-make-pattern-build-1
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        pattern (make-pattern [[:?x rdf:type :http://test.com/Test]
                               (optional [:?y :?z (d 2)])])
        query (defquery
                (query-set-vars [:?y])
                (query-set-type :select)
                (query-set-pattern pattern))
        query-str (.toString (build-query framework query))
        parsed-pattern (parse-sparql-to-pattern framework query-str)]
    (is (= 1 (count (filter #(:optional (meta %1)) parsed-pattern))))))

(deftest test-make-pattern-build-2
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        pattern (make-pattern [[:?x rdf:type :http://test.com/Test]
                               (opt [:?y :?z (d 2)]
                                    [:?l :?m (l "cat")])])
        query (defquery
                (query-set-vars [:?y])
                (query-set-type :select)
                (query-set-pattern pattern))
        query-str (.toString (build-query *sparql-framework* query))
        parsed-pattern (parse-sparql-to-pattern framework query-str)]
    (is (= 2 (count (filter #(:optional (meta %1)) parsed-pattern))))))

;; (deftest test-abstraction-1
;;   (let [triples (make-triples [[:a :b :c]])
;;         pattern (make-pattern [[:?x :b :c]])]
;;     (is (= pattern (triples-abstraction triples (subject? (uri? "http://plaza.org/ontologies/a")) {:subject :?x})))))

(deftest test-build-filters-2
  (let [framework (plaza.rdf.implementations.jena.JenaSparqlFramework.)
        gt (.toString (build-filter framework (make-filter :> :?x (d 3))))
        gt-2 (.toString (build-filter framework (make-filter :> :?x (make-filter :bound :?y))))]
    (is (= gt "( ?x > \"3\"^^xsd:int )"))
    (is (= gt-2 "( ?x > bound(?y) )"))))

(deftest test-build-filters-3
  (let [*tq* "PREFIX  dc:   <http://purl.org/dc/elements/1.1/>\nPREFIX  a:    <http://www.w3.org/2000/10/annotation-ns#>\nPREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n\nSELECT  ?annot\nWHERE\n  { ?annot  a:annotates  <http://www.w3.org/TR/rdf-sparql-query/> ;\n            dc:date      ?date .\n    FILTER ( bound(?date) < \"2005-01-01T00:00:00Z\"^^xsd:dateTime )\n  }\n"
        res (.toString (build-filter *sparql-framework* (first (:filters (parse-sparql-to-query *sparql-framework* *tq*)))))]
    (is (= res "( bound(?date) < \"2005-01-01T00:00:00Z\"^^xsd:dateTime )"))))

(deftest test-go-back-query
  (let [query (defquery
                (query-set-type :select)
                (query-set-vars [:?x])
                (query-set-pattern
                 (make-pattern [[:?x "a" (d 2)]])))

        triples (make-triples [[:m :a (d 2)]
                               [:n :b (d 2)]
                               [:o :a (d 2)]
                               [:p :a (d 3)]])

        model (defmodel
                (model-add-triples triples))

        results (model-query-triples model query)]
    (is (= (count results) 2))))

(deftest test-collect-vars
  (is (= (set (pattern-collect-vars (make-pattern [[:?a :?b (l "test")] [:a :?b (d 2)]])))  (set [:?a :?b])))
  (is (= (set (pattern-collect-vars (make-pattern [[?s ?p ?o]]))) (set [:?s :?p :?o]))))



(deftest test-has-meta-pattern
  (is (:pattern (meta (make-pattern [[?a ?b ?c]])))))

(deftest test-model-pattern-apply-checks-meta
  (let [m (defmodel (model-add-triples [[:a :b :c] [:a :b :e]]))
        result (model-pattern-apply m [[?s ?p :c]])]
    (is (= 1 (count result)))
    (is (= 1 (count (first result))))
    (is (not (keyword? (nth (first (first result)) 2))))))

