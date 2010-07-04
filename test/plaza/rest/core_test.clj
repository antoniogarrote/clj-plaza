(ns plaza.rest.core-test
  (:use compojure.core
        compojure.response
        ring.adapter.jetty)
  (:use [plaza.rdf.core] :reload-all)
  (:use [plaza.rdf.implementations.jena] :reload-all)
  (:use [plaza.triple-spaces distributed-server] :reload-all)
  (:use [plaza.rdf.schemas] :reload-all)
  (:use [plaza.rdf.sparql] :reload-all)
  (:use [plaza.rdf.vocabularies.foaf] :reload-all)
  (:use [plaza.triple-spaces.core] :reload-all)
  (:use [plaza.rdf.implementations.stores.mulgara])
  (:use [plaza.rest core utils handlers] :reload-all)
  (:use [clojure.test])
  (:use [clojure.contrib.logging :only [log]])
  (:require [clojure-http.resourcefully]
            [clojure.contrib.str-utils2 :as str2]))

(init-jena-framework)
(init-vocabularies)


(defn- clean-ts
  ([ts] (in ts [[?s ?p ?o]])))

(defn- breathe
  ([] (Thread/sleep 2000)))

(defn- build-mulgara
  ([] (build-model :mulgara :rmi "rmi://localhost/server1")))

(defonce *should-test* true)


(when *should-test*

  (println "********* RESTful Semantic Resources tests ENABLED *********")
  (println " A redis localhost instance must be running at port 6379 \n and a default Mulgara triple repository,")
  (println " Change the value of the *should-test* symbol in the test file to disable")
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

     (spawn-rest-collection-resource! :foaf-agent "/Agent" :resource)
                                        ; testing of forbidden methods

     (spawn-rest-collection-resource! :foaf-agent "/WriteOnlyResource" :resource
                                      :allowed-methods [:post :delete])

     (spawn-rest-collection-resource! :foaf-agent "/CustomGetCollection" :resource
                                      :allowed-methods [:get]
                                      :get-handle-fn (fn [request environment] {:body "custom get handler"
                                                                                :headers {"Content-Type" "text-plain"}
                                                                                :status 200}))

     (spawn-rest-resource! :foaf-agent "/AgentForbiddenDescription/:id" :Resource
                           :handle-service-metadata? false)

     (spawn-rest-resource! :foaf-agent "/CustomGet/:id" :resource
                           :allowed-methods [:get]
                           :get-handle-fn (fn [id request environment] {:body "custom get handler"
                                                                        :headers {"Content-Type" "text-plain"}
                                                                        :status 200}))

     (spawn-rest-resource! :foaf-agent "/CustomIds/:name" :resource
                           :allowed-methods [:get]
                           :id-match-fn (fn [req env]
                                          (let [pattern (str (:resource-qname-prefix env) (:resource-qname-local env))]
                                            (str2/replace pattern ":name" (get (:params req) "name")))))

     (spawn-rest-collection-resource! :foaf-agent "/CustomIds" :resource
                                      :allowed-methods [:post]
                                      :id-gen-fn (fn [req env]
                                                   (let [prefix (:resource-qname-prefix env)
                                                         local (:resource-qname-local env)]
                                                     {:id (get (:params req) "name") :uri (str prefix local "/" (get (:params req) "name"))}))))

   ;; Runnin the application
   (future (run-jetty (var example) {:port 8082}))

   (Thread/sleep 5000)

   (deftest test-del-post-get-xml
     (println "***************************************************\n DELETE - POST - GET XML \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))))


   (deftest test-del-post-format
     (println "***************************************************\n DELETE - POST (JSON) \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent.json?age=20&gender=male")
           ts (clojure.contrib.json/read-json (apply str (:body-seq res)))]
       (is (= 20 (:age ts)))
       (is (= "male" (:gender ts)))))


   (deftest test-del-post-get-n3
     (println "***************************************************\n DELETE - POST - GET N3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [subj (str (first (first (model-to-triples m))))
             res2 (clojure-http.resourcefully/get (str subj ".n3"))
             m2 (build-model :jena)]
         (with-model m2 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res2)))) :n3))
         (doseq [t (model-to-triples m2)] (log :error (str "*** " t)))
         (is (= 4 (count (model-to-triples m2))))
         (is (= (str (first (first (model-to-triples m2))))
                (str (first (first (model-to-triples m)))))))))


   (deftest test-del-post-get-delete-n3
     (println "***************************************************\n DELETE - POST - GET - DELETE N3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [subj (str (first (first (model-to-triples m))))
             res2 (clojure-http.resourcefully/get (str subj ".n3"))
             m2 (build-model :jena)]
         (with-model m2 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res2)))) :n3))
         (doseq [t (model-to-triples m2)] (log :error (str "*** " t)))
         (is (= 4 (count (model-to-triples m2))))
         (let [uri-to-delete (str (first (first (model-to-triples m2))) ".n3")
               result-delete (clojure-http.resourcefully/delete uri-to-delete)
               m3 (build-model :jena)]
           (with-model m3 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq result-delete)))) :n3))
           (is (= (str (first (first (model-to-triples m3))))
                (str (first (first (model-to-triples m2))))))))))


   (deftest test-del-post-get-html
     (println "***************************************************\n DELETE - POST - GET HTML \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [subj (str (first (first (model-to-triples m))))
             m2 (build-model :jena)]
         (load-stream m2 (str subj ".html") :html)
         (is (= 4 (count (model-to-triples m2))))
         (is (= (str (first (first (model-to-triples m2))))
                (str (first (first (model-to-triples m)))))))))

   (deftest test-del-post-get-js3
     (println "***************************************************\n DELETE - POST - GET JS3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [subj (str (first (first (model-to-triples m))))
             res2 (clojure-http.resourcefully/get (str subj ".js3"))
             ts (clojure.contrib.json/read-json (apply str (:body-seq res2)))]
         (is (= 4 (count ts))))))

   (deftest test-del-post-get-json
     (println "***************************************************\n DELETE - POST - GET JS3 \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [subj (str (first (first (model-to-triples m))))
             res2 (clojure-http.resourcefully/get (str subj ".json"))
             ts (clojure.contrib.json/read-json (apply str (:body-seq res2)))]
         (log :info (str "triples: " ts))
         (is (= 20 (:age ts)))
         (is (= "male" (:gender ts)))
         (is (= "http://xmlns.com/foaf/0.1/Agent" (:type ts)))
         (is (= 5 (count (keys ts)))))))

   (deftest test-del-post-get-form
     (println "***************************************************\n DELETE - POST FORM \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/Agent" {} {"age" 20 "gender" "male"})
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [subj (str (first (first (model-to-triples m))))
             res2 (clojure-http.resourcefully/get (str subj ".json"))
             ts (clojure.contrib.json/read-json (apply str (:body-seq res2)))]
         (log :info (str "triples: " ts))
         (is (= 20 (:age ts)))
         (is (= "male" (:gender ts)))
         (is (= "http://xmlns.com/foaf/0.1/Agent" (:type ts)))
         (is (= 5 (count (keys ts)))))))

   (deftest test-get-forbiden
     (println "***************************************************\n DELETE - GET FORBIDEN \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/WriteOnlyResource")
     (let [res-post (clojure-http.resourcefully/post "http://localhost:8082/WriteOnlyResource" {} {"age" 20 "gender" "male"})]
       (try (clojure-http.resourcefully/get "http://localhost:8082/WriteOnlyResource")
            (is false) ;; this method is not allowed, this should not be executed
            (catch java.io.IOException ex (is true)))))

   (deftest test-custom-get
     (println "***************************************************\n CUSTOM HANDLERS \n******************************************************")
     (let [res1 (clojure-http.resourcefully/get "http://localhost:8082/CustomGet/something")
           res2 (clojure-http.resourcefully/get "http://localhost:8082/CustomGetCollection")]
       (is (= "custom get handler" (apply str (:body-seq res1))))
       (is (= "custom get handler" (apply str (:body-seq res2))))))

   (deftest test-custom-id-fn
     (println "***************************************************\n DELETE - POST - GET CUSTOM ID \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res (clojure-http.resourcefully/post "http://localhost:8082/CustomIds?age=20&gender=male&name=wadus")
           m (build-model :jena)]
       (with-model m (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res)))) :xml))
       (is (= 4 (count (model-to-triples m))))
       (let [res2 (clojure-http.resourcefully/get "http://localhost:8082/CustomIds/wadus.n3")
             m2 (build-model :jena)]
         (with-model m2 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res2)))) :n3))
         (is (= 4 (count (model-to-triples m2))))
         (is (= (str (first (first (model-to-triples m2))))
                (str (first (first (model-to-triples m)))))))))

   (deftest test-del-get-offset-limit
     (println "***************************************************\n GET OFFSET LIMIT \n******************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res1 (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=20&gender=male")
           res2 (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=21&gender=male")
           res3 (clojure-http.resourcefully/post "http://localhost:8082/Agent?age=22&gender=male")
           m1 (build-model :jena)
           m2 (build-model :jena)
           some0 (clojure-http.resourcefully/get "http://localhost:8082/Agent.n3")
           some1 (clojure-http.resourcefully/get "http://localhost:8082/Agent.n3?_limit=2&_offset=0")
           some2 (clojure-http.resourcefully/get "http://localhost:8082/Agent.n3?_limit=2&_offset=2")]
       (log :error (str "*** RES 0\r\n" some0))
       (log :error (str "*** RES 1\r\n" some1))
       (log :error (str "*** RES 2\r\n" some2))
       (with-model m1 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq some1)))) :n3))
       (with-model m2 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq some2)))) :n3))
       (is (= 8 (count (model-to-triples m1))))
       (is (= 4 (count (model-to-triples m2))))))

   (deftest test-service_descriptions
     (println "***********************************************\n SERVICE DESCRIPTIONS \n************************************************")
     (clojure-http.resourcefully/delete "http://localhost:8082/Agent")
     (let [res1 (clojure-http.resourcefully/get "http://localhost:8082/Agent/collection_resource_service.n3")
           m1 (build-model :jena)]
       (with-model m1 (document-to-model (java.io.ByteArrayInputStream. (.getBytes (apply str (:body-seq res1)))) :n3))
       (is (= 238 (count (model-to-triples m1))))))


   (catch Exception ex (throw ex))
   (finally (do (log :info "CLEANING...")
                (shutdown-agents)))))
