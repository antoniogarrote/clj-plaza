;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 01.07.2010

(ns plaza.rest.handlers
  (:use
   [plaza.rest utils]
   [plaza.rdf core]
   [plaza.rdf schemas]
   [plaza.rdf sparql]
   [plaza.rdf predicates]
   [plaza.triple-spaces core]
   [plaza utils]
   [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.json :as json]
            [clojure.contrib.str-utils2 :as str2]))

;;; Handlers

(defn combine-handlers
  "The combinator function, merges all the handlers and filters and apply them to the initial
   request and environment"
  ([request environment]
     (let [pre-graph-filters (:pre-build-graph-query (:filters environment))
           post-graph-filters (:post-build-graph-query (:filters environment))
           pre-build-filters (:pre-build-triples (:filters environment))
           post-build-filters (:post-build-triples (:filters environment))
           pre-render-filters (:pre-render-triples (:filters environment))
           post-render-filters (:post-render-triples (:filters environment))
           build-graph-query-handler [(:build-graph-query-handler environment)]
           build-triples-handler [(:build-triples-handler environment)]
           render-triples-handler [(:render-triples-handler environment)]
           handlers (concat pre-graph-filters build-graph-query-handler post-graph-filters
                            pre-build-filters build-triples-handler post-build-filters
                            pre-render-filters render-triples-handler post-render-filters)]
       (log :error (str "POST BUILD GRAPH QUERY FILTERS: " (count post-graph-filters )))
       (loop [request request
              environment environment
              handlers handlers]
         (if (empty? handlers)
             {:body (:body environment)
              :headers (:headers environment)
              :status (:status environment)}
             (let [handler (first handlers)]
               (recur request
                      (handler request environment)
                      (rest handlers))))))))

(defn default-render-triples-handler
  ([request environment]
     (let [triples (:triples environment)]
       (-> environment
           (assoc :body (render-triples triples (mime-to-format request) (:resource environment) request))
           (assoc :headers {"Content-Type" (format-to-mime request)})
           (assoc :status 200)))))

;; Single GET

(defn get-single-build-graph
  ([request environment]
     (let [id (:id environment)
           mapping (apply-resource-argument-map (:params request) (:resource-map environment))
           query (build-single-resource-query-from-resource-map mapping id)]
       (-> environment
           (assoc :mapping mapping)
           (assoc :query query)))))

(defn get-single-build-triples
  ([request environment]
     (let [query (:query environment)
           results (rd (ts (:resource-ts environment)) query)
           triples (distinct (flatten-1 results))]
       (assoc environment :triples triples))))

(defn handle-get [id request environment]
  (let [build-graph-handler (if (nil? (:build-graph-query-handler environment)) get-single-build-graph (:build-graph-query-handler environment))
        build-triples-handler (if (nil? (:build-triples-handler environment)) get-single-build-triples (:build-triples-handler environment))
        render-triples-handler (if (nil? (:render-triples-handler environment)) default-render-triples-handler (:build-graph-query-handler environment))
        environment-p (-> environment
                          (assoc :build-graph-query-handler build-graph-handler)
                          (assoc :build-triples-handler build-triples-handler)
                          (assoc :render-triples-handler render-triples-handler)
                          (assoc :id id))]
    (combine-handlers request environment-p)))


;; Single PUT

(defn put-single-build-graph
  ([request environment]
     (let [id (:id environment)
           mapping (apply-resource-argument-map (:params request) (:resource-map environment))
           query (build-single-resource-all-triples-query id)]
       (-> environment
           (assoc :mapping mapping)
           (assoc :query query)))))

(defn put-single-build-triples
  ([request environment]
     (let [id (:id environment)
           mapping (:mapping environment)
           query (:query environment)
           triples-pre  (build-triples-from-resource-map id mapping)
           triples-to-update (conj triples-pre [id rdf:type (:resource-type environment)])
           results (swap (ts (:resource-ts environment)) query triples-to-update)
           triples (distinct (flatten-1 results))]
       (assoc environment :triples triples))))

(defn handle-put [id request environment]
  (let [build-graph-handler (if (nil? (:build-graph-query-handler environment)) put-single-build-graph (:build-graph-query-handler environment))
        build-triples-handler (if (nil? (:build-triples-handler environment)) put-single-build-triples (:build-triples-handler environment))
        render-triples-handler (if (nil? (:render-triples-handler environment)) default-render-triples-handler (:build-graph-query-handler environment))
        environment-p (-> environment
                          (assoc :build-graph-query-handler build-graph-handler)
                          (assoc :build-triples-handler build-triples-handler)
                          (assoc :render-triples-handler render-triples-handler)
                          (assoc :id id))]
    (combine-handlers request environment-p)))

;; Single DELETE

(defn delete-single-build-graph
  ([request environment]
     (let [id (:id environment)
           query (build-single-resource-all-triples-query id)]
       (-> environment
           (assoc :query query)))))

(defn delete-single-build-triples
  ([request environment]
     (let [id (:id environment)
           query (:query environment)
           results (in (ts (:resource-ts environment)) query)
           triples (distinct (flatten-1 results))]
       (assoc environment :triples triples))))


(defn handle-delete [id request environment]
  (let [build-graph-handler (if (nil? (:build-graph-query-handler environment)) delete-single-build-graph (:build-graph-query-handler environment))
        build-triples-handler (if (nil? (:build-triples-handler environment)) delete-single-build-triples (:build-triples-handler environment))
        render-triples-handler (if (nil? (:render-triples-handler environment)) default-render-triples-handler (:build-graph-query-handler environment))
        environment-p (-> environment
                          (assoc :build-graph-query-handler build-graph-handler)
                          (assoc :build-triples-handler build-triples-handler)
                          (assoc :render-triples-handler render-triples-handler)
                          (assoc :id id))]
    (combine-handlers request environment-p)))


;; collection GET

(defn get-collection-build-graph
  ([request environment]
     (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
           query (build-query-from-resource-map mapping (:resource-type environment))]
       (-> environment
           (assoc :mapping mapping)
           (assoc :query query)))))

(defn get-collection-build-triples
  ([request environment]
     (let [query (:query environment)
           results (let [rd-argspp []
                         rd-argsp (if (:limit request) (conj rd-argspp (f :limit (:limit request))) rd-argspp)
                         rd-args (if (:offset request) (conj rd-argsp (f :offset (:offset request))) rd-argsp)]
                     (rd (ts (:resource-ts environment)) query rd-args))
           triples (distinct (flatten-1 results))]
       (assoc environment :triples triples))))


(defn handle-get-collection [request environment]
  (let [build-graph-handler (if (nil? (:build-graph-query-handler environment)) get-collection-build-graph (:build-graph-query-handler environment))
        build-triples-handler (if (nil? (:build-triples-handler environment)) get-collection-build-triples (:build-triples-handler environment))
        render-triples-handler (if (nil? (:render-triples-handler environment)) default-render-triples-handler (:build-graph-query-handler environment))
        environment-p (-> environment
                          (assoc :build-graph-query-handler build-graph-handler)
                          (assoc :build-triples-handler build-triples-handler)
                          (assoc :render-triples-handler render-triples-handler))]
    (combine-handlers request environment-p)))

;; collection POST

(defn post-collection-build-graph
  ([request environment]
     (let [params (dissoc (:params request) (name (:id-property-alias environment)))
           mapping (apply-resource-argument-map params (:resource-map environment))
           {id :id resource-uri :uri} ((:id-gen-function environment) request environment)]
       (-> environment
           (assoc :mapping mapping)
           (assoc :params params)
           (assoc :id id)
           (assoc :resource-uri resource-uri)))))

(defn post-collection-build-triples
  ([request environment]
     (let [resource-uri (:resource-uri environment)
           mapping (:mapping environment)
           id (:id environment)
           triples-pre  (conj  (build-triples-from-resource-map resource-uri mapping) [resource-uri rdf:type (:resource-type environment)])
           triples (if (nil? id) triples-pre (conj triples-pre [resource-uri (:id-property-uri environment) (d id)]))]
       (out (ts (:resource-ts environment)) triples)
       (assoc environment :triples triples))))

(defn post-collection-render-triples
  ([request environment]
     (let [triples (:triples environment)]
       (-> environment
           (assoc :body (render-triples triples (mime-to-format request) (:resource environment) request))
           (assoc :headers {"Content-Type" (format-to-mime request)})
           (assoc :status 201)))))

(defn handle-post-collection [request environment]
  (let [build-graph-handler (if (nil? (:build-graph-query-handler environment)) post-collection-build-graph (:build-graph-query-handler environment))
        build-triples-handler (if (nil? (:build-triples-handler environment)) post-collection-build-triples (:build-triples-handler environment))
        render-triples-handler (if (nil? (:render-triples-handler environment)) post-collection-render-triples (:build-graph-query-handler environment))
        environment-p (-> environment
                          (assoc :build-graph-query-handler build-graph-handler)
                          (assoc :build-triples-handler build-triples-handler)
                          (assoc :render-triples-handler render-triples-handler))]
    (combine-handlers request environment-p)))

;; collection DELETE

(defn delete-collection-build-graph
  ([request environment]
     (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))]
       (-> environment
           (assoc :mapping mapping)
           (assoc :query query)))))

(defn delete-collection-build-triples
  ([request environment]
     (let [query (:query environment)
           results (in (ts (:resource-ts environment)) query)
           triples (distinct (flatten-1 results))]
       (assoc environment :triples triples))))

(defn handle-delete-collection [request environment]
  (let [build-graph-handler (if (nil? (:build-graph-query-handler environment)) delete-collection-build-graph (:build-graph-query-handler environment))
        build-triples-handler (if (nil? (:build-triples-handler environment)) delete-collection-build-triples (:build-triples-handler environment))
        render-triples-handler (if (nil? (:render-triples-handler environment)) default-render-triples-handler (:build-graph-query-handler environment))
        environment-p (-> environment
                          (assoc :build-graph-query-handler build-graph-handler)
                          (assoc :build-triples-handler build-triples-handler)
                          (assoc :render-triples-handler render-triples-handler))]
    (combine-handlers request environment-p)))

;; not allowed

(defn handle-method-not-allowed [request environment]
  {:body "method not allowed"
   :headers {"Content-Type" "text/plain"}
   :status 405})
