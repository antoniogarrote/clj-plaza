;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 07.06.2010

(ns plaza.examples.webapp
  (:use [plaza.rest.core])
  (:use plaza.rdf.implementations.jena
        compojure.core
        compojure.response
        ring.adapter.jetty
        [plaza.rdf.core]
        [plaza.rdf.schemas]
        [plaza.triple-spaces.server.mulgara]
        [plaza.triple-spaces.core]
        [plaza.rdf.implementations.jena]
        [plaza.triple-spaces.distributed-server]
        [clojure.contrib.logging :only [log]])
  (:require [compojure.route :as route]))


;; We will use jena
(init-jena-framework)
(load-rdfs-schemas)

;; We load the Friend Of A Friend vocabulary
;; and register the Agent schema in the TBox
(use 'plaza.rdf.vocabularies.foaf)
(tbox-register-schema :foaf-agent foaf:Agent-schema)

;; We create a Triple Space for the resources
(defonce *mulgara* (build-model :mulgara :rmi "rmi://localhost/server1"))
(def-ts :resource (make-distributed-triple-space "test" *mulgara* :redis-host "localhost" :redis-db "testdist" :redis-port 6379))


;; Application routes
(defroutes example
  (GET "/" [] "<h1>Testing plaza...</h1>")
  (spawn-rest-resource! :foaf-agent "/Agent/:id" :resource)
  (spawn-rest-collection-resource! :foaf-agent "/Agent" :resource)
  (route/not-found "Page not found"))

;; Runnin the application
(run-jetty (var example) {:port 8081})
