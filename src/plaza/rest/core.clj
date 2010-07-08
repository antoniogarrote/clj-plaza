;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 07.06.2010

(ns plaza.rest.core
  (:use
   [hiccup.core]
   [plaza.utils]
   [plaza.rdf.core]
   [plaza.rdf.schemas]
   [plaza.rdf.sparql]
   [plaza.rdf.predicates]
   [plaza.rdf.vocabularies wsmo-lite hrests plaza]
   [plaza.triple-spaces.core]
   [plaza.rest utils handlers]
   [compojure core response route]
   [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.json :as json]
            [clojure.contrib.str-utils2 :as str2]))



(defn make-environment-map [resource-or-symbol path ts opts]
  (let [resource (if (keyword? resource-or-symbol) (tbox-find-schema resource-or-symbol) resource-or-symbol)
        resource-type (type-uri resource)
        resource-map (model-to-argument-map resource)
        id-gen (if (nil? (:id-gen-fn opts)) default-uuid-gen (:id-gen-fn opts))
        id-property-alias (if (nil? (:id-property-alias opts)) :id (:id-property-alias opts))
        id-property-uri (if (and (nil? (:id-property-uri opts))
                                 (nil? (:id-property-alias opts))) plz:restResourceId (:id-property-uri opts))
        service-matcher-fn (if (nil? (:service-metadata-matcher-fn opts)) default-service-metadata-matcher-fn (:service-metadata-matcher-fn))
        schema-matcher-fn (if (nil? (:schema-metadata-matcher-fn opts)) default-schema-metadata-matcher-fn (:schema-metadata-matcher-fn))
        handle-service-metadata (if (nil? (:handle-service-metadata? opts)) true (:handle-service-metadata? opts))
        handle-schema-metadata (if (nil? (:handle-schema-metadata? opts)) true (:handle-schema-metadata? opts))
        resource-qname-prefix (:resouce-qname-prefix opts)
        resource-qname-local path
        resource-ts ts
        allowed-methods (:allowed-methods opts)
        get-handle-fn (:get-handle-fn opts)
        post-handle-fn (:post-handle-fn opts)
        put-handle-fn (:put-handle-fn opts)
        delete-handle-fn (:delete-handle-fn opts)
        service-uri-gen-fn (if (nil? (:service-uri-gen-fn opts)) default-uri-template-for-service (:service-uri-gen-fn opts))
        augmentated-resource (if (nil? (:id-property-alias opts)) (augmentate-resource resource id-property-uri) resource)
        augmentated-resource-map (if (nil? (:id-property-alias opts))
                                   (model-to-argument-map augmentated-resource)
                                   resource-map)
        empty-filters {:pre-build-graph-query []
                       :post-build-graph-query []
                       :pre-build-triples []
                       :post-build-triples []
                       :pre-render-triples []
                       :post-render-triples []}
        filters (let [filters-def (:filters opts)
                      kind-filters (keys filters-def)]
                  (reduce (fn [flts kind]
                            (reduce (fn [flts flt] (add-filter kind flt flts))
                                    flts
                                    (get filters-def kind)))
                          empty-filters
                          kind-filters))]
    {:resource-map augmentated-resource-map
     :resource-type resource-type
     :resource-qname-prefix resource-qname-prefix
     :resource-qname-local resource-qname-local
     :id-gen-function id-gen
     :resource-ts resource-ts
     :resource augmentated-resource
     :path (str path "*")
     :path-re (re-pattern (str "(\\..*)?$"))
     :service-matcher-fn service-matcher-fn
     :schema-matcher-fn schema-matcher-fn
     :handle-schema-metadata? handle-schema-metadata
     :handle-service-metadata? handle-service-metadata
     :allowed-methods allowed-methods
     :get-handle-fn get-handle-fn
     :post-handle-fn post-handle-fn
     :put-handle-fn put-handle-fn
     :delete-handle-fn delete-handle-fn
     :base-path path
     :service-uri-gen-fn service-uri-gen-fn
     :kind :collection
     :id-property-alias id-property-alias
     :id-property-uri id-property-uri
     :filters filters}))

(defn make-single-resource-environment-map [resource-or-symbol path ts opts]
  (let [coll-env (make-environment-map resource-or-symbol path ts opts)
        id-match (if (nil? (:id-match-fn opts)) default-id-match (:id-match-fn opts))]
    (-> coll-env (assoc :id-match-function id-match) (assoc :kind :individual))))

(defn build-default-qname-prefix
  "Returns the domain of a RING request"
  ([request]
     (let [scheme (:scheme request)
           host (:server-name request)]
       (str (keyword-to-string scheme) "://" host))))

(defn parse-standard-request-params
  "Transforms the request to use conventions like _method param for DELETE and PUT methods or jsonp callback arg"
  ([request]
     (let [request-methods (let [meth (get (:params request) "_method")]
                             (if (not (nil? meth)) (cond (or (= meth "post") (= meth "POST")) (let [paramsp (dissoc (:params request) "_method")]
                                                                                                (-> request (assoc  :request-method :post)
                                                                                                    (assoc :params paramsp)))
                                                         (or (= meth "put") (= meth "PUT")) (let [paramsp (dissoc (:params request) "_method")]
                                                                                              (-> request (assoc  :request-method :put)
                                                                                                  (assoc :params paramsp)))
                                                         (or (= meth "delete") (= meth "DELETE")) (let [paramsp (dissoc (:params request) "_method")]
                                                                                                    (-> request (assoc  :request-method :delete)
                                                                                                        (assoc :params paramsp)))
                                                         (or (= meth "get") (= meth "GET")) (let [paramsp (dissoc (:params request) "_method")]
                                                                                              (-> request (assoc  :request-method :get)
                                                                                                  (assoc :params paramsp)))
                                                         :else request)
                                 request))
           jsonp-request (let [callback (get (:params request-methods) "_callback")
                               format (get (:params request-methods) "format")]
                           (if (and (not (nil? callback)) (or (= format "json") (= format "js") (= format "js3")))
                             (let [paramsp (-> (:params request-methods)
                                               (assoc "format" (str format ":jsonp"))
                                               (dissoc "_callback"))]
                               (-> request-methods (assoc :jsonp-callback callback)
                                   (assoc :params paramsp)))
                             request-methods))
           limit-request (let [limit (get (:params jsonp-request) "_limit")]
                           (if-not (nil? limit)
                             (let [paramsp (-> (:params jsonp-request)
                                               (dissoc "_limit"))]
                               (-> jsonp-request (assoc :limit limit)
                                   (assoc :params paramsp)))
                             jsonp-request))
           offset-request (let [offset (get (:params limit-request) "_offset")]
                            (if-not (nil? offset)
                              (let [paramsp (-> (:params limit-request)
                                                (dissoc "_offset"))]
                                (-> limit-request (assoc :offset offset)
                                    (assoc :params paramsp)))
                              limit-request))]
       offset-request)))

(defn render-format-service [service-description format request environment]
  "Formats a service description using the right format"
  (let [request-domain (make-request-domain request environment)]
    (condp = format
      :json (json/json-str (hRESTS-description-to-json service-description request-domain))
      :json-jsonp (json/json-str (hRESTS-description-to-json service-description request-domain))
      :rdfa (hRESTS-description-to-rdfa service-description request-domain request)
      (render-triples (hRESTS-description-to-triples service-description request-domain) format plaza.rdf.schemas/rdfs:Class-schema request))))

(defn check-tbox-request
  "Checks if the request is asking for TBox metadata instead of the actual service data"
  ([request environment]
     (if (:handle-schema-metadata? environment)
       (if ((:schema-matcher-fn environment) request environment)
         {:body (render-triples (to-rdf-triples (:resource environment)) (mime-to-format request) plaza.rdf.schemas/rdfs:Class-schema request)
          :headers {"Content-Type" (format-to-mime request)}
          :status 200}
         (if (:handle-service-metadata? environment)
           (if-let [kind-serv ((:service-matcher-fn environment) request environment)]
             (let [service (tbox-find-service (:path environment))]
               (if (nil? service)
                 false
                 (let [env-kind-serv (:kind environment)
                       full-request-uri (make-full-request-uri request environment)]
                   (if (= env-kind-serv kind-serv)
                     {:body (render-format-service (assoc service :uri full-request-uri) (mime-to-format request) request environment)
                      :headers {"Content-Type" (format-to-mime request)}
                      :status 200}
                     (if-let [parent-service (tbox-find-parent-service (:path environment))]
                       (let [schema (-> (:resource environment) (deaugmentate-resource (:id-property-uri environment)))
                             environmentp (assoc environment :resource schema)]
                         {:body (render-format-service (assoc parent-service :uri full-request-uri) (mime-to-format request) request environmentp)
                          :headers {"Content-Type" (format-to-mime request)}
                          :status 200})
                       false)))))
             false)
           false))
       false)))

(defn should-handle-method
  "Checks if a request with a certain methods should be handled"
  ([method request environment]
     (and (= (:request-method request) method)
          (or (nil? (:allowed-methods environment))
              (not (nil? (some #(= method %1) (:allowed-methods environment))))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn dispatch-to-handler
  "Dispatches the request to the user provided hander function or to the default handler
   using the HTTP method information"
  ([kind method request environment]
     (condp = method
       :get (if (nil? (:get-handle-fn environment)) (if (= kind :collection) handle-get-collection handle-get) (:get-handle-fn environment))
       :post (if (nil? (:post-handle-fn environment)) handle-post-collection (:post-handle-fn environment))
       :put (if (nil? (:put-handle-fn environment)) handle-put (:put-handle-fn environment))
       :delete (if (nil? (:delete-handle-fn environment)) (if (= kind :collection) handle-delete-collection handle-delete) (:delete-handle-fn environment)))))

(defmacro spawn-rest-collection-resource! [resource path ts & opts]
  (let [opts (apply hash-map opts)]
    `(let [env-pre# (make-environment-map ~resource ~path ~ts ~opts)]
       (tbox-register-service (:path env-pre#) (hRESTS-collection-service-description ((:service-uri-gen-fn env-pre#) (:base-path env-pre#)) (:resource env-pre#) env-pre#))
       (ANY  (:path env-pre#) request-pre# (wrap-request (str (.toUpperCase (keyword-to-string (:request-method request-pre#))) " collection")
                                                         (:resource-qname-prefix env-pre#)
                                                         (:resource-qname-local env-pre#)
                                                         request-pre#
                                                         ;; Logic
                                                         (let [env# (if (nil? (:resource-qname-prefix env-pre#))
                                                                      (assoc env-pre# :resource-qname-prefix (build-default-qname-prefix request-pre#))
                                                                      env-pre#)
                                                               old-params# (:params request-pre#)
                                                               format# (let [fmt# (match-route-extension (:path-re env#) (:uri request-pre#))]
                                                                         (if (nil? fmt#) nil (clojure.contrib.str-utils2/lower-case fmt#)))
                                                               params# (assoc old-params# "format" format#)
                                                               request# (parse-standard-request-params (assoc request-pre# :params params#))
                                                               tbox-metadata# (check-tbox-request request# env#)]
                                                           (if (not tbox-metadata#)
                                                               (cond
                                                                (should-handle-method :get request# env#) ((dispatch-to-handler :collection :get request# env#) request# env#)
                                                                (should-handle-method :post request# env#) ((dispatch-to-handler :collection :post request# env#) request# env#)
                                                                (should-handle-method :delete request# env#) ((dispatch-to-handler :collection :delete request# env#) request# env#)
                                                                :else (handle-method-not-allowed request# env#))
                                                               tbox-metadata#)))))))

(defmacro spawn-rest-resource! [resource path ts & opts]
  (let [opts (apply hash-map opts)]
    `(let [env-pre# (make-single-resource-environment-map ~resource ~path ~ts ~opts)]
       (tbox-register-service (:path env-pre#) (hRESTS-single-service-description ((:service-uri-gen-fn env-pre#) (:base-path env-pre#)) (:resource env-pre#) env-pre#))
       (ANY  (:path env-pre#) request-pre# (wrap-request (str (.toUpperCase (keyword-to-string (:request-method request-pre#))) " resource")
                                                         (:resource-qname-prefix env-pre#)
                                                         (:resource-qname-local env-pre#)
                                                         request-pre#
                                                         ;; Logic
                                                         (let [env# (if (nil? (:resource-qname-prefix env-pre#))
                                                                      (assoc env-pre# :resource-qname-prefix (build-default-qname-prefix request-pre#))
                                                                      env-pre#)
                                                               old-params# (:params request-pre#)
                                                               format# (let [fmt# (match-route-extension (:path-re env#) (:uri request-pre#))]
                                                                         (if (nil? fmt#) nil (clojure.contrib.str-utils2/lower-case fmt#)))
                                                               params# (assoc old-params# "format" format#)
                                                               request# (parse-standard-request-params (assoc request-pre# :params params#))
                                                               id# ((:id-match-function env#) request# env#)
                                                               tbox-metadata# (check-tbox-request request# env#)]
                                                           (if (not tbox-metadata#)
                                                             (cond
                                                              (should-handle-method :get request# env#) ((dispatch-to-handler :single :get request# env#) id# request# env#)
                                                              (should-handle-method :put request# env#) ((dispatch-to-handler :single :put request# env#) id# request# env#)
                                                              (should-handle-method :delete request# env#) ((dispatch-to-handler :single :delete request# env#) id# request# env#)
                                                              :else (handle-method-not-allowed request# env#))
                                                             tbox-metadata#)))))))
