;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 07.06.2010

(ns plaza.examples.webapp
  (:use [plaza.rest.core])
  (:use plaza.rdf.implementations.jena
        compojure.core
        compojure.response
        ring.adapter.jetty
        [plaza.utils]
        [plaza.rdf.core]
        [plaza.rdf.schemas]
        [plaza.triple-spaces.server.mulgara]
        [plaza.triple-spaces.core]
        [plaza.rdf.implementations.jena]
        [plaza.triple-spaces.distributed-server]
        [clojure.contrib.logging :only [log]])
  (:require [compojure.route :as route]))

;; we will use jena
(init-jena-framework)


(defonce *mulgara* (build-model :mulgara :rmi "rmi://localhost/server1"))


(def-ts :resource (make-distributed-triple-space "test" *mulgara* :redis-host "localhost" :redis-db "testdist" :redis-port 6379))


(use 'plaza.rdf.vocabularies.foaf)

(defn default-id-match
  "Matches a resource using the requested URI"
  ([request environment]
     (let [pattern (str (:resource-qname-prefix environment) (:resource-qname-local environment))]
       (clojure.contrib.str-utils2/replace pattern ":id" (get (:params request) "id")))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Resource functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-environment-map [resource-or-symbol path ts opts]
  (let [resource (if (keyword? resource-or-symbol) (tbox-find-schema resource-or-symbol) resource-or-symbol)
        resource-type (type-uri resource)
        resource-map (model-to-argument-map resource)
        id-gen (if (nil? (:id-gen-fn opts)) default-uuid-gen (:id-gen-fn opts))
        resource-qname-prefix (:resouce-qname-prefix opts)
        resource-qname-local path
        resource-ts ts]
    { :resource-map resource-map :resource-type resource-type :resource-qname-prefix resource-qname-prefix
     :resource-qname-local resource-qname-local :id-gen-function id-gen :resource-ts resource-ts :resource resource
     :path (str path "*") :path-re (re-pattern (str "(\\..*)?$"))}))

(defn make-single-resource-environment-map [resource-or-symbol path ts opts]
  (let [pre-map (make-environment-map resource-or-symbol path ts opts) resource (if (keyword? resource-or-symbol) (tbox-find-schema resource-or-symbol) resource-or-symbol)
        id-match (if (nil? (:id-match-fn opts)) default-id-match (:id-match-fn opts))]
    (assoc pre-map :id-match-function id-match)))

(defn build-default-qname-prefix
  "Returns the domain of a RING request"
  ([request]
     (let [scheme (:scheme request)
           host (:server-name request)]
       (str (keyword-to-string scheme) "://" host))))

(defmacro spawn-rest-collection-resource! [resource path ts & opts]
  (let [opts (apply hash-map opts)]
    `(let [env-pre# (make-environment-map ~resource ~path ~ts ~opts)]
       (ANY  (:path env-pre#) request-pre# (wrap-request (str (.toUpperCase (keyword-to-string (:request-method request-pre#))) " collection")
                                                         (:resource-qname-prefix env-pre#)
                                                         (:resource-qname-local env-pre#)
                                                         request-pre#
                                                         (let [env# (if (nil? (:resource-qname-prefix env-pre#))
                                                                      (assoc env-pre# :resource-qname-prefix (build-default-qname-prefix request-pre#))
                                                                      env-pre#)
                                                               old-params# (:params request-pre#)
                                                               format# (let [fmt# (match-route-extension (:path-re env#) (:uri request-pre#))]
                                                                         (if (nil? fmt#) nil (clojure.contrib.str-utils2/lower-case fmt#)))
                                                               params# (assoc old-params# "format" format#)
                                                               request# (assoc request-pre# :params params#)]
                                                           (cond
                                                            (= (:request-method request#) :get) (handle-get-collection request# env#)
                                                            (= (:request-method request#) :post) (handle-post-collection request# env#)
                                                            (= (:request-method request#) :delete) (handle-delete-collection request# env#)
                                                            :else (handle-method-not-allowed request# env#))))))))

(defmacro spawn-rest-resource! [resource path ts & opts]
  (let [opts (apply hash-map opts)]
    `(let [env-pre# (make-single-resource-environment-map ~resource ~path ~ts ~opts)]
       (ANY  (:path env-pre#) request-pre# (wrap-request (str (.toUpperCase (keyword-to-string (:request-method request-pre#))) " resource")
                                                         (:resource-qname-prefix env-pre#)
                                                         (:resource-qname-local env-pre#)
                                                         request-pre#
                                                         (let [env# (if (nil? (:resource-qname-prefix env-pre#))
                                                                      (assoc env-pre# :resource-qname-prefix (build-default-qname-prefix request-pre#))
                                                                      env-pre#)
                                                               old-params# (:params request-pre#)
                                                               format# (let [fmt# (match-route-extension (:path-re env#) (:uri request-pre#))]
                                                                         (if (nil? fmt#) nil (clojure.contrib.str-utils2/lower-case fmt#)))
                                                               params# (assoc old-params# "format" format#)
                                                               request# (assoc request-pre# :params params#)
                                                               id# ((:id-match-function env#) request# env#)]
                                                           (cond
                                                            (= (:request-method request#) :get) (handle-get id# request# env#)
                                                            (= (:request-method request#) :put) (handle-put id# request# env#)
                                                            (= (:request-method request#) :delete) (handle-delete id# request# env#)
                                                            :else (handle-method-not-allowed request# env#))))))))


;;; Handlers

(defn handle-get [id request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-single-resource-query-from-resource-map mapping id)
        results (rd (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
        (log :info (str "GET REQUEST -> mapping:" mapping " triples:" triples))
        {:body (render-triples triples (mime-to-format request) (:resource environment))
         :headers {"Content-Type" (format-to-mime request)}
         :status 200}))

(defn handle-put [id request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-single-resource-all-triples-query id)
        triples-pre  (build-triples-from-resource-map id mapping)
        triples-to-update (conj triples-pre [id rdf:type (:resource-type environment)])
        results (swap (ts (:resource-ts environment)) query triples-to-update)
        triples (distinct (flatten-1 results))]
    (log :info (str "PUT REQUEST -> mapping:" mapping " query:" query))
    (log :info (str "QUERY"))
    (doseq [t query] (log :info t))
    (log :info (str "VALUES"))
    (doseq [t triples-to-update] (log :info t))
    {:body (render-triples triples (mime-to-format request) (:resource environment))
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-delete [id request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-single-resource-query-from-resource-map mapping id)
        results (in (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results) (:resource environment))]
        (log :info (str "DELETE REQUEST -> mapping:" mapping " triples:" triples))
        {:body (render-triples triples (mime-to-format request))
         :headers {"Content-Type" (format-to-mime request)}
         :status 200}))


(defn handle-get-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))
        results (rd (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
     (log :info (str "GET REQUEST -> mapping:" mapping " triples:" triples))
    {:body (render-triples triples (mime-to-format request) (:resource environment))
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-post-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        resource-id ((:id-gen-function environment) (:resource-qname-prefix environment) (:resource-qname-local environment) request)
        triples-pre  (build-triples-from-resource-map resource-id mapping)
        triples (conj triples-pre [resource-id rdf:type (:resource-type environment)])]
    (log :info (str "POST REQUEST -> id:" resource-id " mapping:" mapping " triples:" triples))
    (out (ts (:resource-ts environment)) triples)
    {:body (render-triples triples :xml (:resource environment))
     :headers {"Content-Type" "application/xml"}
     :status 201}))

(defn handle-delete-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))
        results (in (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
    (log :info (str "GET REQUEST -> mapping:" mapping " query:" query))
    {:body (render-triples triples (mime-to-format request) (:resource environment))
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-method-not-allowed [request environment]
  {:body "method not allowed"
   :headers {"Content-Type" "text/plain"}
   :status 405})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(tbox-register-schema :foaf-agent foaf:Agent-schema)

(defroutes example
  (GET "/" [] "<h1>Testing plaza...</h1>")
  (spawn-rest-resource! :foaf-agent "/Agent/:id" :resource)
  (spawn-rest-collection-resource! :foaf-agent "/Agent" :resource)
  (route/not-found "Page not found"))

;(run-jetty (var example) {:port 8081})
