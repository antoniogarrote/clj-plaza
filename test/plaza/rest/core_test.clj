(ns plaza.rest.core-test
  (:use [plaza.rdf.core] :reload-all)
  (:use [plaza.rdf.implementations.jena] :reload-all)
  (:use [plaza.rest.core] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)
(use 'plaza.rdf.vocabularies.foaf)

(deftest test-tbox-find-register
  (do (tbox-register-schema :fag foaf:Agent-schema)
      (is (= foaf:Agent-schema (tbox-find-schema :fag)))
      (is (= foaf:Agent-schema (tbox-find-schema foaf:Agent)))))

(deftest test-match-route-extension
  (let [exp #"/Agent(\..*)?"]
    (is (= "rdf" (match-route-extension exp "/Agent.rdf")))
    (is (= nil (match-route-extension exp "/Agent")))))
