;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 07.06.2010

(ns plaza.examples.webapp
  (:use [compojure core]
        [compojure response]
        [ring.adapter jetty]
        [plaza.rdf core schemas]
        [plaza.rdf.vocabularies foaf]
        [plaza.rdf.implementations jena]
        [plaza.rdf.implementations.stores.mulgara]
        [plaza.triple-spaces.core]
        [plaza.triple-spaces.distributed-server]
        [plaza.rest core])
  (:require [compojure.route :as route]
            [clojure.contrib.str-utils2 :as str2]))


;; We will use jena
(init-jena-framework)

;; We init and define vocabularies
(init-vocabularies)
(def ComputationCelebrity-schema
     (make-rdfs-schema foaf:Person
                       :name            {:uri foaf:name           :range :string}
                       :surname         {:uri foaf:surname        :range :string}
                       :nick            {:uri foaf:nick          :range :string}
                       :birthday        {:uri foaf:birthday       :range :date}
                       :interest        {:uri foaf:topic_interest :range :string}
                       :wikipedia_entry {:uri foaf:holdsAccount   :range rdfs:Resource}))

(tbox-register-schema :celebrity ComputationCelebrity-schema)
(tbox-register-schema :foaf-agent foaf:Agent-schema)

;; We create a Triple Space for the resources

;; Any triple space can be used, the following commented lines defined a
;; persistent distributed triple space using Mulgara as the backend.

;(defonce *mulgara* (build-model :mulgara :rmi "rmi://localhost/server1"))
;(def-ts :celebrities (make-distributed-triple-space "test" *mulgara* :redis-host "localhost" :redis-db "testdist" :redis-port 6379))

;; For testint we will use a non persistent basic triple space

;;; Single node triple space
(def-ts :celebrities (make-basic-triple-space))


;; Application routes
(defroutes example
  (GET "/" [] "<h1>Testing plaza...</h1>")
  (spawn-rest-resource! :celebrity "/Celebrity/:id" :celebrities)
  (spawn-rest-collection-resource! :celebrity "/Celebrity" :celebrities)
  (spawn-rest-resource! :foaf-agent "/CustomIds/:name" :resource
                        :id-property-alias :name
                        :id-property-uri foaf:name
 ;                       :allowed-methods [:get]
                        :handle-service-metadata? false
                        :id-match-fn (fn [req env]
                                       (let [pattern (str (:resource-qname-prefix env) (:resource-qname-local env))]
                                         (str2/replace pattern ":id" (get (:params req) "name")))))
  (spawn-rest-collection-resource! :foaf-agent "/CustomIds" :resource
;                                  :allowed-methods [:post]
                                   :handle-service-metadata? false
                                   :id-gen-fn (fn [req env]
                                                (let [prefix (:resource-qname-prefix env)
                                                      local (:resource-qname-local env)]
                                                  {:id (get (:params req) "name") :uri (str prefix local "/" (get (:params req) "name"))})))
  (route/not-found "Page not found"))

;; Running the application
(run-jetty (var example) {:port 8081})
