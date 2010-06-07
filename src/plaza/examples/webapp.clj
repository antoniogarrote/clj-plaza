(ns plaza.examples.webapp
  (:use compojure.core
        compojure.response
        ring.adapter.jetty
        [clojure.contrib.logging :only [log]])
  (:require [compojure.route :as route]))

(use 'plaza.utils)
(use 'plaza.rdf.core)
(use 'plaza.rdf.sparql)
(use 'plaza.rdf.predicates)
(use 'plaza.rdf.implementations.jena)
(use 'plaza.triple-spaces.core)
(use 'plaza.triple-spaces.distributed-server)

(init-jena-framework)

(defn resource-argument-map [& mapping]
  (let [args (partition 3 mapping)]
    (reduce (fn [acum [k uri f]] (assoc acum k {:uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri)) :mapper f})) {} args)))

(defn apply-resource-argument-map [params mapping]
  (let [ks (keys mapping)]
    (reduce (fn [acum k] (let [{uri :uri f :mapper} (k mapping)]
                           (conj acum [uri (f (get params (keyword-to-string k)))])))
            [] ks)))

(defn random-uuid []
  (.replace (str (java.util.UUID/randomUUID)) "-" ""))

(defn random-resource-id [ns]
  (str ns (random-uuid)))

(defn build-triples-from-resource-map [uri mapping]
  (let [resource-uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri))]
    (log :info (str "resource uri " resource-uri " and mapping " mapping))
    (map #(cons resource-uri %1) mapping)))

(defn build-model-from-resource-map [uri map]
  (defmodel (model-add-triples (build-triples-from-resource-map uri map))))

(defn render-triples [triples format]
  (let [m (defmodel (model-add-triples triples))
                w (java.io.StringWriter.)]
            (output-string m w format)
            (.toString w)))

(defonce *mulgara* (build-model :mulgara :rmi "rmi://localhost/server1"))
(def-ts :resource (make-distributed-triple-space "test" *mulgara* :redis-host "localhost" :redis-db "testdist" :redis-port 6379))

(def *resource-map* (resource-argument-map :name "http://test.com/Person#name" #(l %1)
                                           :age  "http://test.com/Person#age"  #(d (Integer/parseInt %1))))


(defn format-to-mime [format]
  (condp = format
    "xml" "application/xml"
    "rdf" "application/xml"
    "n3" "text/rdf+n3"
    "ttl" "application/x-turtle"
    "turtle" "application/x-turtle"
    "application/xml"))

(defn mime-to-format [format]
  (condp = format
    "xml" :xml
    "application/xml" :xml
    "rdf" :xml
    "n3" :n3
    "text/rdf+n3" :n3
    "ttl" :ttl
    "application/x-turtle" :turtle
    "turtle" :turtle
    :xml))


(defroutes example
  (GET "/" [] "<h1>Hello World Wide Web mod5!</h1>")

  (POST "/Person" request
        (log :info (str "request: " request))
        (let [mapping (apply-resource-argument-map (:params request) *resource-map*)
              resource-id (random-resource-id "http://test.com/Person/")
              triples-pre  (build-triples-from-resource-map resource-id mapping)
              triples (conj triples-pre [resource-id rdf:type "http://test.com/Person"])]
          (out (ts :resource) triples)
          (render-triples triples :xml)))

  (GET "/Person.:format" request
       (log :info (str "request: " request))
       (let [results (rd (ts :resource) [[?s ?p ?o]
                                         [?s rdf:type "http://test.com/Person"]])
             triples (distinct (flatten-1 results))]
         {:body (render-triples triples (mime-to-format (get (:params request) "format")))
          :headers {"Content-Type" (format-to-mime (get (:params request) "format"))}
          :status 200}))

  (route/not-found "Page not found"))

;(future (run-jetty (var example) {:port 8080}))

(run-jetty (var example) {:port 8081})
