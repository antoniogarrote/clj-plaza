(ns plaza.rest.core-test
  (:use compojure.core
        compojure.response
        ring.adapter.jetty)
  (:use [plaza.rdf.core] :reload-all)
  (:use [plaza.rdf.implementations.jena] :reload-all)
  (:use [plaza.triple-spaces distributed-server] :reload-all)
  (:use [plaza.rdf.schemas] :reload-all)
  (:use [plaza.rdf.sparql] :reload-all)
  (:use [plaza.triple-spaces.core] :reload-all)
  (:use [plaza.rest.core] :reload-all)
  (:use [clojure.test])
  (:use [clojure.contrib.logging :only [log]])
  (:require [clojure-http.resourcefully]))

(init-jena-framework)
(load-rdfs-schemas)
(use 'plaza.rdf.vocabularies.foaf)


(defn- clean-ts
  ([ts] (in ts [[?s ?p ?o]])))

(defn- breathe
  ([] (Thread/sleep 2000)))

(defn- build-mulgara
  ([] (build-model :mulgara :rmi "rmi://localhost/server1")))

(defonce *should-test* false)


(when *should-test*

  (println "********* RESTful Semantic Resources tests ENABLED *********")
  (println " A redis localhost instance must be running at port 6379 \n and a default Mulgara triple repository,")
  (println " change the value of the *should-test* symbol in the test file to disable")
  (println " A new Jetty instance will be created so port 8082 must also be free.")
  (println "**********************************************************")

  (try

   (deftest test-tbox-find-register
     (do (tbox-register-schema :fag foaf:Agent-schema)
         (is (= foaf:Agent-schema (tbox-find-schema :fag)))
         (is (= foaf:Agent-schema (tbox-find-schema foaf:Agent)))))

   (deftest test-match-route-extension
     (let [exp #"/Agent(\..*)?"]
       (is (= "rdf" (match-route-extension exp "/Agent.rdf")))
       (is (= nil (match-route-extension exp "/Agent")))))


   ;; We load the Friend Of A Friend vocabulary
   ;; and register the Agent schema in the TBox
   (use 'plaza.rdf.vocabularies.foaf)
   (tbox-register-schema :foaf-agent plaza.rdf.vocabularies.foaf/foaf:Agent-schema)

   ;; We create a Triple Space for the resources
   (defonce *mulgara* (build-model :mulgara :rmi "rmi://localhost/server1"))
   (def-ts :resource (make-distributed-triple-space "test" *mulgara* :redis-host "localhost" :redis-db "testdist" :redis-port 6379))


   ;; Application routes
   (defroutes example
     (spawn-rest-resource! :foaf-agent "/Agent/:id" :resource)
     (spawn-rest-collection-resource! :foaf-agent "/Agent" :resource))

   ;; Runnin the application
   (future (run-jetty (var example) {:port 8082}))

   (Thread/sleep 5000)

   (deftest test-del-post-get-xml
     (println "***************************************************\n DELETE - POST - GET XML \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 3 (count (model-to-triples m))))))

   (deftest test-del-post-get-n3
     (println "***************************************************\n DELETE - POST - GET N3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 3 (count (model-to-triples m))))
       (let [subj-pre (str (first (first (model-to-triples m))))
             subj (clojure.contrib.str-utils2/replace subj-pre "localhost" "localhost:8082")
             res2 (clojure-http.resourcefully/get (str subj ".n3"))
             m2 (build-model :jena)]
         (with-model m2 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res2)))) :n3))
         (is (= 3 (count (model-to-triples m2))))
         (is (= (str (first (first (model-to-triples m2))))
                (str (first (first (model-to-triples m)))))))))

   (deftest test-del-post-get-html
     (println "***************************************************\n DELETE - POST - GET HTML \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 3 (count (model-to-triples m))))
       (let [subj-pre (str (first (first (model-to-triples m))))
             subj (clojure.contrib.str-utils2/replace subj-pre "localhost" "localhost:8082")
             m2 (build-model :jena)]
         (load-stream m2 (str subj ".html") :html)
         (is (= 3 (count (model-to-triples m2))))
         (is (= (str (first (first (model-to-triples m2))))
                (str (first (first (model-to-triples m)))))))))

   (deftest test-del-post-get-js3
     (println "***************************************************\n DELETE - POST - GET JS3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 3 (count (model-to-triples m))))
       (let [subj-pre (str (first (first (model-to-triples m))))
             subj (clojure.contrib.str-utils2/replace subj-pre "localhost" "localhost:8082")
             res2 (clojure-http.resourcefully/get (str subj ".js3"))
             ts (clojure.contrib.json/read-json (apply str (:body-seq res2)))]
         (doseq [t ts] (log :info t))
         (is (= 3 (count ts))))))

   (deftest test-del-post-get-json
     (println "***************************************************\n DELETE - POST - GET JS3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 3 (count (model-to-triples m))))
       (let [subj-pre (str (first (first (model-to-triples m))))
             subj (clojure.contrib.str-utils2/replace subj-pre "localhost" "localhost:8082")
             res2 (clojure-http.resourcefully/get (str subj ".json"))
             ts (clojure.contrib.json/read-json (apply str (:body-seq res2)))]
         (log :info (str "triples: " ts))
         (is (= 20 (:age ts)))
         (is (= "male" (:gender ts)))
         (is (= "http://xmlns.com/foaf/0.1/Agent" (:type ts)))
         (is (= 4 (count (keys ts)))))))

   (finally (shutdown-agents))
   ))
