(ns plaza.rdf.implementations.jena-test
  (:use [plaza.rdf core] :reload-all)
  (:use [plaza.rdf.implementations jena common] :reload-all)
  (:use [clojure.test]))

;; we'll test with jena
(init-jena-framework)


(deftest test-jena-resource
  (let [model (build-model :jena)
        res (create-resource model "http://test.com/test")]
    (is (= "http://test.com/test" (to-string res)))
    (is (not (is-blank res)))
    (is (is-resource res))
    (is (not (is-property res)))
    (is (= "http://test.com/test" (resource-id res)))
    (is (= "http://test.com/" (qname-prefix res)))
    (is (= "test" (qname-local res)))))

(deftest test-jena-property
  (let [model (build-model :jena)
        res (create-property model "http://test.com/test")]
    (is (= "http://test.com/test" (to-string res)))
    (is (not (is-blank res)))
    (is (is-resource res))
    (is (is-property res))
    (is (= "http://test.com/test" (resource-id res)))
    (is (= "http://test.com/" (qname-prefix res)))
    (is (= "test" (qname-local res)))))


(deftest test-jena-blank-node
  (let [model (build-model :jena)
        res (create-blank-node model "a")]
    (is (= "_:a" (to-string res)))
    (is (is-blank res))
    (is (not (is-resource res)))
    (is (not (is-property res)))
    (is (= "a" (resource-id res)))
    (is (= "_" (qname-prefix res)))
    (is (= "a" (qname-local res)))))

(deftest test-jena-literal
  (let [model (build-model :jena)
        res (create-literal model "a" "es")]
    (is (= "a@es" (to-string res)))
    (is (not (is-blank res)))
    (is (not (is-resource res)))
    (is (not (is-property res)))
    (is (is-literal res))
    (is (= (resource-id res) "a@es"))
    (is (= (literal-value res) "a"))
    (is (= (literal-language res) "es"))
    (is (= (literal-datatype-uri res) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))))

(deftest test-jena-typed-literal
  (let [model (build-model :jena)
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

(deftest test-locks-1
  (let [*m* (build-model :jena)
        counter (ref 0)
        sync (promise)
        sync-lock (promise)]
    (.start (Thread. (fn [] (do (model-critical-write *m* (deliver sync-lock :continue) (dosync (alter counter (fn [x] :a))) (deliver sync :continue))))))
    @sync-lock
    (model-critical-write *m* (dosync (is (= @counter :a)) (alter counter (fn [x] :b))))
    @sync
    (is (= (dosync @counter) :b))))

(deftest test-query-string
  (let [*m* (build-model :jena)
        query-str "SELECT ?s ?p ?o WHERE { ?s ?p ?o .}"]
    (with-model *m* (model-add-triples [[:a :b :c]]))
    (is (= 1 (count (query-triples *m* query-str))))
    (is (= 3 (count (ffirst (query-triples *m* query-str)))))))

(deftest test-datatype-symbol
  (is (= :string (datatype-symbol "http://www.w3.org/2001/XMLSchema#string")))
  (is (= nil (datatype-symbol "http://www.w3.org/anything/not/a/atatype"))))
