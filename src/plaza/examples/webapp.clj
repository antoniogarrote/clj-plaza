(ns plaza.examples.webapp
  (:use compojure.core
        compojure.response
        ring.adapter.jetty
        [clojure.contrib.logging :only [log]])
  (:require [compojure.route :as route]))

(use 'plaza.utils)
(use 'plaza.rdf.core)
(use 'plaza.rdf.models)
(use 'plaza.rdf.sparql)
(use 'plaza.rdf.predicates)
(use 'plaza.rdf.implementations.jena)
(use 'plaza.triple-spaces.core)
(use 'plaza.triple-spaces.distributed-server)

(init-jena-framework)

(defn resource-argument-map [& mapping]
  (let [args (partition 3 mapping)]
    (reduce (fn [acum [k uri f]] (assoc acum k {:uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri)) :mapper f})) {} args)))

(defn model-argument-map [model & mapping]
  (let [args (partition 2 mapping)]
    (reduce (fn [acum [k f]] (assoc acum k {:uri (property-uri model k) :mapper f})) {} args)))

(defn apply-resource-argument-map [params mapping]
  (let [ks (keys mapping)]
    (reduce (fn [acum k] (let [{uri :uri f :mapper} (k mapping)
                               arg (get params (keyword-to-string k))]
                           (if (nil? arg) acum (conj acum [uri (f arg)]))))
            [] ks)))

(defn random-uuid []
  (.replace (str (java.util.UUID/randomUUID)) "-" ""))

(defn random-resource-id [ns]
  (str ns (random-uuid)))

(defn build-triples-from-resource-map [uri mapping]
  (let [resource-uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri))]
    (map #(cons resource-uri %1) mapping)))

(defn build-query-from-resource-map [mapping resource-type]
  (concat [[?s ?p ?o]
           [?s rdf:type resource-type]]
          (vec (map #(cons ?s %1) mapping))))

(defn build-model-from-resource-map [uri map]
  (defmodel (model-add-triples (build-triples-from-resource-map uri map))))

(defn render-triples [triples format]
  (let [m (defmodel (model-add-triples triples))
        w (java.io.StringWriter.)]
    (output-string m w format)
    (.toString w)))

(defonce *mulgara* (build-model :mulgara :rmi "rmi://localhost/server1"))
(def-ts :resource (make-distributed-triple-space "test" *mulgara* :redis-host "localhost" :redis-db "testdist" :redis-port 6379))


(defn supported-format [format]
  (condp = format
    "application/xml" :xml
    "text/rdf+n3" :n3
    "application/x-turtle" :turtle
    "*/*" :xml
    nil))

(defn parse-accept-header [accept-str]
  (if (nil? accept-str)
    "application/xml"
    (let [formats-str (aget (.split accept-str ";") 0)
          formats (seq (.split formats-str ","))
          supported (map supported-format formats)
          selected (first (filter #(not (nil? %1)) supported))]
      (if (nil? selected) :xml selected))))

(defn mime-to-format [request]
  (let [format (let [fmt (get (:params request) "format")]
                 (if (nil? fmt)
                   (keyword-to-string (parse-accept-header (get (:headers request) "accept")))
                   fmt)) ]
    (condp = format
      "xml" :xml
      "application/xml" :xml
      "rdf" :xml
      "n3" :n3
      "text/rdf+n3" :n3
      "ttl" :ttl
      "application/x-turtle" :turtle
      "turtle" :turtle
      :xml)))

(defn format-to-mime [request]
  (let [format (let [fmt (get (:params request) "format")]
                 (if (nil? fmt)
                   (keyword-to-string (parse-accept-header (get (:headers request) "accept")))
                   fmt)) ]
    (condp = format
      "xml" "application/xml"
      "rdf" "application/xml"
      "n3" "text/rdf+n3"
      "ttl" "application/x-turtle"
      "turtle" "application/x-turtle"
      "application/xml")))

(defn default-uuid-gen [prefix local request]
  (random-resource-id (str prefix local)))

(defmacro log-request [kind prefix local request & body]
  `(let [pre# (System/currentTimeMillis)]
     (log :info (str ~kind " (" ~prefix ~local "): \r\n" ~request "\r\n"))
     (let [result# (do ~@body)
           post# (System/currentTimeMillis)]
       (log :info (str "FINISHED (" (:status result#) "): " (- post# pre#) " millisecs "))
       result#)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Resource functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def *resource* (make-rdfs-model "http://test.com/Person"
                                 :name   {:uri "http://test.com/name" :range :string}
                                 :age    {:uri "http://test.com/age"  :range :int}))


(def *resource-map* (model-argument-map *resource* :name #(l %1)
                                                   :age #(d (Integer/parseInt %1))))

(def *resource-type* "http://test.com/Person")
(def *resource-qname-prefix* "http://test.com/")
(def *resource-qname-local* "Person")
(def *id-gen-function* default-uuid-gen)
(def *resource-ts* :resource)

(defn handle-get-collection [request]
  (log-request "GET collection" *resource-qname-prefix* *resource-qname-local* request
               (let [mapping (apply-resource-argument-map (:params request) *resource-map*)
                     query (build-query-from-resource-map mapping *resource-type*)
      ;               _test (doseq [t query] (println t))
                     results (rd (ts *resource-ts*) query)
                     triples (distinct (flatten-1 results))]
                 {:body (render-triples triples (mime-to-format request))
                  :headers {"Content-Type" (format-to-mime request)}
                  :status 200})))

(defn handle-post-collection [request]
  (log-request "POST collection" *resource-qname-prefix* *resource-qname-local* request
               (let [mapping (apply-resource-argument-map (:params request) *resource-map*)
                     resource-id (*id-gen-function* *resource-qname-prefix* *resource-qname-local* request)
                     triples-pre  (build-triples-from-resource-map resource-id mapping)
                     triples (conj triples-pre [resource-id rdf:type *resource-type*])]
                 (out (ts *resource-ts*) triples)
                 {:body (render-triples triples :xml)
                  :headers {"Content-Type" "application/xml"}
                  :status 201})))

(defn handle-delete-collection [request]
  (log-request "DELETE collection" *resource-qname-prefix* *resource-qname-local* request
               (let [mapping (apply-resource-argument-map (:params request) *resource-map*)
                     query (build-query-from-resource-map mapping *resource-type*)
                     results (rd (ts *resource-ts*) query)
                     triples (distinct (flatten-1 results))]
                 {:body (render-triples triples (mime-to-format request))
                  :headers {"Content-Type" (format-to-mime request)}
                  :status 200})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defroutes example
  (GET "/" [] "<h1>Hello World Wide Web mod5!</h1>")

  (POST "/Person" request (handle-post-collection request))
  (GET "/Person.:format" request (handle-get-collection request))
  (GET "/Person" request (handle-get-collection request))

  (route/not-found "Page not found"))

;(run-jetty (var example) {:port 8081})
