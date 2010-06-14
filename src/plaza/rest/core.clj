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
   [plaza.triple-spaces.core]
   [compojure core response route]
   [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.json :as json]
            [clojure.contrib.str-utils2 :as str2]))


(defn default-css-text []
  "   * {
         margin: 0;
         padding: 0;
         border: 0;
         outline: 0;
         font-weight: normal;
         font-style: normal;
         font-size: 100%;
         font-family: Tahoma, Geneva, arial, sans-serif;
         vertical-align: baseline
      }

      body {
         line-height: 1
      }

      :focus {
         outline: 0
      }

      ol, ul {
         list-style: none
      }

      table {
         border-collapse: collapse;
         border-spacing: 0
      }

      blockquote:before, blockquote:after, q:before, q:after {
         content: \"\"
      }

      blockquote, q {
         quotes: \"\" \"\"
      }

      input, textarea {
         margin: 0;
         padding: 0
      }

      hr {
         margin: 0;
         padding: 0;
         border: 0;
         color: #000;
         background-color: #000;
         height: 1px
      }
      .resource-request
      {
         background-color: black;
         font-size: 150%;
         color: white;
         padding-left: 20px;
         padding-top: 20px;
         padding-bottom: 20px
      }

      .resource
      {
         color: #45465b;
         background-color:#eeeeee;
         border-top:2px solid #aaaaaa;
         margin:20px;
         padding:20px;
      }

      .resource-title
      {
         font-size: 110%;
         font-weight: bold
      }

      .properties-table
      {
         margin-top: 20px
      }

      table
      {
         border-color: #dddddd;
         border-style: solid;
         border-width: 2px
      }

      thead
      {
         background-color: e7eef6
      }

      #plaza-logo
      {
         color: e7eef6;
         float: right;
         font-family: arial;
         font-size: 70%;
         font-weight: bold;
         margin-right: 40px;
         margin-top: 4px;
      }

      td
      {
         background-color: white
      }

      td, th
      {
         padding: 10px
      }

      th
      {
         font-weight: bold
      }

      td
      {
         border-top-color: #dddddd;
         border-top-style: solid;
         border-top-width: 2px
      }")

(defn resource-argument-map [& mapping]
  (let [args (partition 3 mapping)]
    (reduce (fn [acum [k uri f]] (assoc acum k {:uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri)) :mapper f})) {} args)))

(defn model-argument-map [model & mapping]
  (let [args (partition 2 mapping)]
    (reduce (fn [acum [k f]] (assoc acum k {:uri (property-uri model k) :mapper f})) {} args)))

(defn model-to-argument-map [model]
  (let [args (aliases model)]
    (reduce (fn [acum k] (assoc acum k {:uri (property-uri model k) :mapper (fn [v] (prop-value-to-triple-value model k v))})) {} args)))

(defn apply-resource-argument-map [params mapping]
  (let [ks (keys mapping)]
    (reduce (fn [acum k] (let [{uri :uri f :mapper} (k mapping)
                               arg (get params (keyword-to-string k))]
                           (if (nil? arg) acum (conj acum [uri (f arg)]))))
            [] ks)))

(defn random-uuid []
  (.replace (str (java.util.UUID/randomUUID)) "-" ""))

(defn random-resource-id [ns]
  (str ns "/" (random-uuid)))

(defn build-triples-from-resource-map [uri mapping]
  (let [resource-uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri))]
    (map #(cons resource-uri %1) mapping)))

(defn build-query-from-resource-map [mapping resource-type]
  (concat [[?s ?p ?o]
           [?s rdf:type resource-type]]
          (vec (map #(cons ?s %1) mapping))))

(defn build-single-resource-query-from-resource-map [mapping resource-id]
  (concat [[resource-id ?p ?o]]
          (vec (map #(cons resource-id %1) mapping))))

(defn build-single-resource-all-triples-query [resource-id]
  [[resource-id ?p ?o]])

(defn build-model-from-resource-map [uri map]
  (defmodel (model-add-triples (build-triples-from-resource-map uri map))))

(defn to-js3-triples
  ([ts]
     (let [tsp (map (fn [[s p o]]
                      (if (is-literal o)
                        (let [value-pre (literal-value o)
                              value (if (or (instance? com.hp.hpl.jena.datatypes.xsd.XSDDateTime value-pre))
                                      (str value-pre) value-pre)]
                          [(str s) (str p) {:value value :datatype (literal-datatype-uri o)}])
                        [(str s) (str p) (str o)]))
                    ts)]
       (json/json-str tsp))))

(defn to-json-triples
  ([ts schema]
     (if (empty? ts)
       (json/json-str [])
       (let [gts (group-by (fn [[s p o]] (str s)) ts)]
         (if (= 1 (count (keys gts)))
           (let [tsp (reduce (fn [acum [s p o]]
                               (if (is-literal o)
                                 (let [pred (property-alias schema (str p))
                                       value-pre (literal-value o)
                                       value (if (or (instance? com.hp.hpl.jena.datatypes.xsd.XSDDateTime value-pre))
                                               (str value-pre) value-pre)]
                                   (if (nil? pred) (assoc acum (str p) value)
                                       (assoc acum pred value)))
                                 (let [pred (property-alias schema (str p))]
                                   (if (nil? pred) (if (= (str p) rdf:type)
                                                     (assoc acum :type (str o))
                                                     (assoc acum (str p) (str o)))
                                       (assoc acum pred (str o))))))
                             {} (first (vals gts)))]
             (json/json-str (assoc tsp :uri (str (first (keys gts))))))
           (json/json-str (map (fn [[s tss]]
                                 (let [jsts (reduce (fn [acum [s p o]]
                                                      (if (is-literal o)
                                                        (let [pred (property-alias schema (str p))
                                                              value (literal-value o)]
                                                          (if (nil? pred) acum
                                                              (assoc acum pred value)))
                                                        (let [pred (property-alias schema (str p))]
                                                          (if (nil? pred) acum
                                                              (assoc acum pred (str o))))))
                                                    {} tss)]
                                   (assoc jsts :uri (str s))))
                               gts)))))))

(defn to-js3-triples-jsonp
  ([ts schema callback] (str callback "(" (to-js3-triples ts schema) ");")))

(defn to-json-triples-jsonp
  ([ts schema callback] (str callback "(" (to-json-triples ts schema) ");")))

(defn extract-ns [uri]
  (if (empty? (filter #(= \# %1) uri))
    (str (clojure.contrib.string/join "/" (drop-last (clojure.contrib.string/split #"/" uri))) "/")
    (str (first (clojure.contrib.string/split #"#" uri)) "#")))

(defn add-ns
  [[mapping counter] ns]
  (if (empty? (filter #(= ns %1) (keys mapping)))
    [(assoc mapping ns (str "ns" counter)) (inc counter)]
    [mapping counter]))

(defn collect-ns [ts]
  (first (reduce (fn [[acum c] [s p o]]
                   (if (is-literal o)
                     (let [nss (extract-ns (str s))
                           nsp (extract-ns (str p))]
                       (-> [acum c]
                           (add-ns nss)
                           (add-ns nsp)))
                     (let [nss (extract-ns (str s))
                           nsp (extract-ns (str p))
                           nso (extract-ns (str o))]
                       (-> [acum c]
                           (add-ns nss)
                           (add-ns nsp)
                           (add-ns nso)))))
                 [{} 0] ts)))

(defn build-curie [nsmap uri]
  (let [prefix (first (filter #(= 2 (alength (.split uri %1))) (keys nsmap)))
        val (get nsmap prefix)
        local (aget (.split uri prefix) 1)]
    (str val ":"  local)))

(defn to-rdfa-triple
  ([s ts schema nsmap]
     [:div {:about (str s) :class "resource"}
      [:span {:class "resource-title"} "Resource: " [:a {:href (str s) :class "resource-uri"} (str s)]]
      [:table {:class "properties-table"}
       [:thead [:th "name"] [:th "property URI"] [:th "range value"] [:th "range URI"]]
       (map (fn [[s p o]]
              (if (is-literal o)
                [:tr
                 [:td (keyword-to-string (property-alias schema (str p)))]
                 [:td (str p)]
                 [:td {:property (build-curie nsmap (str p)) :datatype (literal-datatype-uri o)} (literal-value o)]
                 [:td {:class "datatype"} (literal-datatype-uri o)]]
                [:tr
                 [:td (if (= rdf:type (str p)) "type" (keyword-to-string (property-alias schema (str p))))]
                 [:td (str p)]
                 [:td [:a {:href (str o) :rel (build-curie nsmap (str p))} (str o)]]
                 [:td {:class "resourcetype"} rdfs:Resource]])) ts)]]))

(defn ns-list [nsmap]
  (reduce (fn [acum [k v]] (if (= k "/") acum (assoc acum (str "xmlns:" v) k))) {} nsmap))

(defn to-rdfa-triples
  ([ts schema request]
     (let [nsmap (collect-ns ts)
           gts (group-by (fn [[s p o]] (str s)) ts)
           rdfa-ts (map (fn [[s tsp]] (to-rdfa-triple s tsp schema nsmap)) gts)
           nslistp (assoc  (ns-list nsmap) :xmlns "http://www.w3.org/1999/xhtml")
           nslistpp (assoc nslistp :version "XHTML+RDFa 1.0")]
       (html [:html nslistpp
              [:head [:title (str (clojure.contrib.string/upper-case (keyword-to-string (:request-method request))) " " (:uri request))]]
              [:body
               [:style {:type "text/css" :media "screen"} (default-css-text)]
               [:div {:class "resource-body"}
                [:div {:class "resource-request"} (str "Request: " (clojure.contrib.string/upper-case (keyword-to-string (:request-method request)) ) " "
                                                       (keyword-to-string (:scheme request)) "://" (:server-name request) (:uri request))
                 [:span {:id "plaza-logo"} "( plaza )"]]
                [:span {:class "resource-list"}
                 rdfa-ts]]]]))))

(defn render-triples [triples format schema request]
  (if (or (= format :json-jsonp) (= format :js3-jsonp) (= format :js-jsonp))
    (if (or (= format :json-jsonp) (= format :js-jsonp))
      (to-json-triples-jsonp triples schema (:jsonp-callback request))
      (to-js3-triples-jsonp triples (:jsonp-callback request)))
    (if (or (= format :json) (= format :js3) (= format :js))
      (if (or (= format :json) (= format :js))
        (to-json-triples triples schema)
        (to-js3-triples triples))
      (if (or (= format :html) (= format :xhtml) (= format :rdfa))
        (to-rdfa-triples triples schema request)
        (let [m (defmodel (model-add-triples triples))
              w (java.io.StringWriter.)]
          (output-string m w format)
          (.toString w))))))

(defn supported-format [format]
  (condp = format
    "application/xml" :xml
    "text/rdf+n3" :n3
    "application/x-turtle" :turtle
    "text/html" :rdfa
    "application/xhtml+xml" :rdfa
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
      "json" :json
      "json:jsonp" :json-jsonp
      "js" :json
      "js:jsonp" :json-jsonp
      "js3" :js3
      "js3:jsonp" :js3-jsonp
      "xhtml" :rdfa
      "html" :rdfa
      "text/html" :rdfa
      "application/xhtml+xml" :rdfa
      "rdfa" :rdfa
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
      "json" "application/json"
      "js" "application/json"
      "js3" "application/json"
      "json:jsonp" "application/json"
      "js:jsonp" "application/json"
      "js3:jsonp" "application/json"
      "rdfa" "text/html"
      "html" "text/html"
      "xhtml" "text/html"
      "application/xml")))

(defn default-uuid-gen [request environment]
  (let [prefix (:resource-qname-prefix environment)
        local (:resource-qname-local environment)
        port (if (= (:server-port request) 80) "" (str ":" (:server-port request)))]
    (random-resource-id (str prefix port local))))

(defmacro wrap-request [kind prefix local request & body]
  `(let [pre# (System/currentTimeMillis)]
     (try
      (log :info (str "--> " ~kind " (" ~prefix ~local "): \r\n" ~request "\r\n"))
      (let [result# (do ~@body)
            post# (System/currentTimeMillis)]
        (log :info (str "*** FINISHED (" (:status result#) "): " (- post# pre#) " millisecs "))
        result#)
      (catch Exception ex#
        (log :error (str "*** ERROR (500): " (- (System/currentTimeMillis) pre#) " millisecs\r\n" (extract-exception-trace ex#)))
        {:body (str "application internal error")
         :headers {"Content-Type" "text/plain"}
         :status 500}))))

(defn match-route-extension
  "check if a route matches a pattern"
  ([pattern route]
     (let [[_path extension] (re-find pattern route)]
       (if (nil? extension) extension
           (let [parts (.split extension "\\.")]
             (if (= (alength parts) 2)
               (aget parts 1)
               nil))))))

;; TBox

(defonce *tbox* (ref {}))

(defn tbox-register-schema
  "Adds a new model to the TBox"
  ([name ontology-model]
     (dosync (alter *tbox* #(assoc %1 name ontology-model)))))

(defn tbox-find-schema
  "Checks if the provided alias or URI points to a schema in the TBox"
  ([alias-or-uri]
     (if (not (nil? (get @*tbox* alias-or-uri)))
       (get @*tbox* alias-or-uri)
       (first (filter #(= (str alias-or-uri) (str (type-uri %1))) (vals @*tbox*))))))

(defn default-id-match
  "Matches a resource using the requested URI"
  ([request environment]
     (let [port (if (= (:server-port request) 80) "" (str ":" (:server-port request)))
           pattern (str (:resource-qname-prefix environment) port (:resource-qname-local environment))]
       (str2/replace pattern ":id" (get (:params request) "id")))))

(defn default-service-metadata-matcher-fn
  ([request environment]
     (if (nil? (re-find  #"service(\..*)?$" (:uri request))) false true)))

(defn default-schema-metadata-matcher-fn
  ([request environment]
     (if (nil? (re-find  #"schema(\..*)?$" (:uri request))) false true)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Resource functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn hRESTS-collection-service-description
  "Builds a triple set describing the service offered by the current request"
  ([path resource]
     {:uri path}))


(defn make-environment-map [resource-or-symbol path ts opts]
  (let [resource (if (keyword? resource-or-symbol) (tbox-find-schema resource-or-symbol) resource-or-symbol)
        resource-type (type-uri resource)
        resource-map (model-to-argument-map resource)
        id-gen (if (nil? (:id-gen-fn opts)) default-uuid-gen (:id-gen-fn opts))
        service-matcher-fn (if (nil? (:service-metadata-matcher-fn opts)) default-schema-metadata-matcher-fn (:service-metadata-matcher-fn))
        schema-matcher-fn (if (nil? (:schema-metadata-matcher-fn opts)) default-schema-metadata-matcher-fn (:schema-metadata-matcher-fn))
        handle-service-metadata (if (nil? (:handle-service-metadata opts)) true (:handle-service-metadata opts))
        handle-schema-metadata (if (nil? (:handle-schema-metadata opts)) true (:handle-schema-metadata opts))
        resource-qname-prefix (:resouce-qname-prefix opts)
        resource-qname-local path
        resource-ts ts
        allowed-methods (:allowed-methods opts)
        get-handle-fn (:get-handle-fn opts)
        post-handle-fn (:post-handle-fn opts)
        put-handle-fn (:put-handle-fn opts)
        delete-handle-fn (:delete-handle-fn opts)]
    {:resource-map resource-map :resource-type resource-type :resource-qname-prefix resource-qname-prefix
     :resource-qname-local resource-qname-local :id-gen-function id-gen :resource-ts resource-ts :resource resource
     :path (str path "*") :path-re (re-pattern (str "(\\..*)?$")) :service-matcher-fn service-matcher-fn
     :schema-matcher-fn schema-matcher-fn :handle-schema-metadata handle-schema-metadata
     :handle-service-metadata handle-service-metadata :allowed-methods allowed-methods :get-handle-fn get-handle-fn
     :post-handle-fn post-handle-fn :put-handle-fn put-handle-fn :delete-handle-fn delete-handle-fn}))

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
                             request-methods))]
       jsonp-request)))

(defn check-tbox-request
  "Checks if the request is asking for TBox metadata instead of the actual service data"
  ([request environment]
     (if (:handle-schema-metadata environment)
       (if ((:schema-matcher-fn environment) request environment)
         {:body (render-triples (to-rdf-triples (:resource environment)) (mime-to-format request) plaza.rdf.schemas/rdfs:Class-schema request)
          :headers {"Content-Type" (format-to-mime request)}
          :status 200}
         false)
       false)))

(defn should-handle-method
  "Checks if a request with a certain methods should be handled"
  ([method request environemnt]
     (and (= (:request-method request) method)
          (or (nil? (:allowed-methods environemnt))
              (not (nil? (some #(= method %1) (:allowed-methods environemnt))))))))


;;; Handlers

(defn handle-get [id request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-single-resource-query-from-resource-map mapping id)
        results (rd (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
    (log :info (str "GET REQUEST -> mapping:" mapping " triples:" triples " for query " query " and id " id))
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
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
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-delete [id request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-single-resource-query-from-resource-map mapping id)
        results (in (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
    (log :info (str "DELETE REQUEST -> mapping:" mapping " triples:" triples))
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))


(defn handle-get-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))
        results (rd (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
    (log :info (str "GET REQUEST -> mapping:" mapping " triples:" triples))
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-post-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        resource-id ((:id-gen-function environment) request environment)
        triples-pre  (build-triples-from-resource-map resource-id mapping)
        triples (conj triples-pre [resource-id rdf:type (:resource-type environment)])]
    (log :info (str "POST REQUEST -> id:" resource-id " mapping:" mapping " triples:" triples))
    (out (ts (:resource-ts environment)) triples)
    {:body (render-triples triples :xml (:resource environment) request)
     :headers {"Content-Type" "application/xml"}
     :status 201}))

(defn handle-delete-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))
        results (in (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
    (log :info (str "DELETE REQUEST -> mapping:" mapping " query:" query))
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-method-not-allowed [request environment]
  {:body "method not allowed"
   :headers {"Content-Type" "text/plain"}
   :status 405})

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
