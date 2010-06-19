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

(defn default-service-css-text []
  "   * {
         margin: 0;
         padding: 0;
         border: 0;
         outline: 0;
         font-weight: normal;
         font-style: normal;
         font-size: 100%;
         font-family: Tahoma, Geneva, arial, sans-serif;
         vertical-align: baseline;
      }

      body {
         line-height: 1.5
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

      code
      {
         background-color:#EEEEFF;
         border:1px solid #DDDDDD;
         font-size:95%;
         padding:0 0.5em
      }

      td
      {
         border-top-color: #dddddd;
         border-top-style: solid;
         border-top-width: 2px
      }

      .operation
      {
         color: #45465b;
         background-color:#eeeeee;
         border-top:2px solid #aaaaaa;
         margin:20px;
         padding:20px;
      }

      .operation-title span
      {
         font-size: 140%;
         font-weight: bold;
         margin-bottom: 10px
      }

      .operation-body
      {
         padding: 10px
      }

      .fragment
      {
         margin-top: 20px;
         margin-bottom: 15px;
      }

      .fragment-subtitle
      {
         margin-top: 10px;
         margin-bottom: 10px;
         font-size: 105%;
         font-weight: bold;
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
    (last
     (reduce (fn [[c acum] k] (let [{uri :uri f :mapper} (k mapping)
                                    arg (get params (keyword-to-string k))]
                                (if (nil? arg) [(inc c) (conj acum [uri (keyword (str "?o" c))])] [c (conj acum [uri (f arg)])])))
             [0 []] ks))))

(defn random-uuid []
  (.replace (str (java.util.UUID/randomUUID)) "-" ""))

(defn random-resource-id [ns]
  (random-uuid))

(defn build-triples-from-resource-map
  ([uri mapping]
      (let [resource-uri (if (seq? uri) (apply rdf-resource uri) (rdf-resource uri))]
        (reduce (fn [acum [p o]]
                  (if (keyword? o)
                    acum
                    (conj acum [resource-uri p o])))
                [] mapping))))

(defn build-query-from-resource-map [mapping resource-type]
  (concat [[?s rdf:type resource-type]]
          (vec (map (fn [[p o]]
                      (if (keyword? o)
                        (optional [?s p o])
                        [?s p o])) mapping))))

(defn build-single-resource-query-from-resource-map [mapping resource-id]
  (concat [[resource-id rdf:type :?rt]]
          (vec (map (fn [[p o]]
                      (if (keyword? o)
                        (optional [resource-id p o])
                        [resource-id p o])) mapping))))

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

(defn make-full-request-uri [request environment]
  (let [prefix (:resource-qname-prefix environment)
        local (:uri request)
        port (if (= (:server-port request) 80) "" (str ":" (:server-port request)))]
    (str prefix port local)))

(defn make-request-domain [request environment]
  (let [prefix (:resource-qname-prefix environment)
        port (if (= (:server-port request) 80) "" (str ":" (:server-port request)))]
    (str prefix port)))

(defn default-uuid-gen [request environment]
  (let [prefix (:resource-qname-prefix environment)
        local (:resource-qname-local environment)
        port (if (= (:server-port request) 80) "" (str ":" (:server-port request)))
        id (random-uuid)]
    {:id id :uri (str (str prefix port local) "/" id)}))

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
(defonce *services-tbox* (ref {}))

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

(defn tbox-register-service
  "Adds a new model to the TBox"
  ([path service-model]
     (log :info (str "*** registering service " (:uri service-model) " at " path))
     (dosync (alter *services-tbox* #(assoc %1 path service-model)))))

(defn tbox-find-service
  "Checks if the provided alias or URI points to a schema in the TBox"
  ([path]
     (get @*services-tbox* path)))

(defn tbox-find-parent-service
  "Checks if there is a collection resource in a super path for the current service"
  ([path]
     (let [parent-path (str (clojure.contrib.str-utils2/join "/" (drop-last (clojure.contrib.str-utils2/split path #"/"))) "*")
           key (first (filter #(= %1 parent-path) (keys @*services-tbox*)))]
       (get @*services-tbox* key))))

(defn default-id-match
  "Matches a resource using the requested URI"
  ([request environment]
     (let [port (if (= (:server-port request) 80) "" (str ":" (:server-port request)))
           pattern (str (:resource-qname-prefix environment) port (:resource-qname-local environment))]
       (str2/replace pattern ":id" (get (:params request) "id")))))

(defn default-service-metadata-matcher-fn
  ([request environment]
     (if (nil? (re-find  #"collection_resource_service(\..*)?$" (:uri request)))
       (if (nil? (re-find  #"single_resource_service(\..*)?$" (:uri request)))
         false :individual)
       :collection)))

(defn default-schema-metadata-matcher-fn
  ([request environment]
     (if (nil? (re-find  #"schema(\..*)?$" (:uri request))) false true)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Resource functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn default-service-operation-description
  "Default text description for a service operations"
  ([address method]
     [:span
       "The address of the operation is described by this URI template: " [:code address]
       " To consume this operation, issue a request using the " [:code method]
      " HTTP method replacing the parameters in the URI template by values according to the following input messages information"]))

(defn make-hRESTS-collection-operation
  "Builds the description of a hRESTS allowed operation on a collection resource"
  ([method path resource environment]
     (let [resource-type (type-uri resource)
           resource-schema (str path "/schema")
           aliases-post (filter #(not (= %1 (:id-property-alias environment))) (aliases resource))]
       (condp = method
         :get {:identifier (name method)
               :label (str "HTTP " (name method) " method")
               :method (name method)
               :address path
               :description (default-service-operation-description path (name method))
               :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) (aliases resource))
               :output-messages {:name "theResources" :description "The returned resources" :model resource-schema :model-type resource-type}}
         :post {:identifier (name method)
                :label (str "HTTP " (name method) " method")
                :method (name method)
                :address path
                :description (default-service-operation-description path (name method))
                :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) aliases-post)
                :output-messages {:name "theResource" :description "The newly created resource" :model resource-schema :model-type resource-type}}
         :delete {:identifier (name method)
                  :label (str "HTTP " (name method) " method")
                  :method (name method)
                  :address path
                  :description (default-service-operation-description path (name method))
                  :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) (aliases resource))
                  :output-messages {:name "deletedResources" :description "The deleted resources" :model resource-schema :model-type resource-type}}))))

(defn make-hRESTS-single-operation
  "Builds the description of a hRESTS allowed operation on a single resource"
  ([method path resource environment]
     (log :info (str "*** PATH (ind):" path))
     (let [resource-type (type-uri resource)
           resource-schema (str path "/schema")]
       (condp = method
         :get {:identifier (name method)
               :label (str "HTTP " (name method) " method")
               :method (name method)
               :address path
               :description (default-service-operation-description path (name method))
               :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) (filter #(not (= -1 (.indexOf path (keyword-to-string %1)))) (aliases resource)))
               :output-messages {:name "theResource" :description "The returned resource" :model resource-schema :model-type resource-type}}
         :put {:identifier (name method)
               :label (str "HTTP " (name method) " method")
               :method (name method)
               :address path
               :description (default-service-operation-description path (name method))
               :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) (aliases resource))
               :output-messages {:name "theResource" :description "The newly created resource" :model resource-schema :model-type resource-type}}
         :delete {:identifier (name method)
                  :label (str "HTTP " (name method) " method")
                  :method (name method)
                  :address path
                  :description (default-service-operation-description path (name method))
                  :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) (filter #(not (= -1 (.indexOf path (keyword-to-string %1)))) (aliases resource)))
                  :output-messages {:name "deletedResource" :description "The deleted resource" :model resource-schema :model-type resource-type}}))))

(defn hRESTS-collection-service-description
  "Builds a triple set describing the service offered by the current request"
  ([path resource environment]
     {:uri path
      :operations (map #(make-hRESTS-collection-operation %1 path resource environment) (if (nil? (:allowed-methods environment))
                                                                                          [:get :post :delete]
                                                                                          (:allowed-methods environment)))}))

(defn hRESTS-single-service-description
  "Builds a triple set describing the service offered by the current request"
  ([path resource environment]
     {:uri path
      :operations (map #(make-hRESTS-single-operation %1 path resource environment) (if (nil? (:allowed-methods environment))
                                                                                      [:get :put :delete]
                                                                                      (:allowed-methods environment)))}))

(defn hRESTS-message-to-triples-map
  "Transforms a hRESTS message description into a set of triples, returns a blank URI for the message and the triples"
  ([message]
     (let [uri (b)]
       [uri [[uri rdf:type wsl:Message]
             [uri "http://www.w3.org/ns/sawsdl#modelReference" (:model message)]
             [uri "http://schemas.xmlsoap.org/wsdl/http/urlReplacement" (d (name (:name message)))]]])))

(defn hRESTS-output-message-to-triples-map
  "Transforms a hRESTS message description into a set of triples, returns a blank URI for the message and the triples"
  ([message path-prefix]
     (let [uri (b)]
       [uri [[uri rdf:type wsl:Message]
             [uri "http://www.w3.org/ns/sawsdl#modelReference" (d (str path-prefix (:model message)))]]])))

(defn hRESTS-op-to-triples-map
  "Transforms a hRESTS operation description into a set of triples, returns a blank URI for the description and the triples"
  ([operation path-prefix]
     (let [uri (b)
           output-uri (b)
           pre-ts (concat [[uri rdf:type wsl:Operation]
                           [uri hr:hasAddress (d (str path-prefix (:address operation)) hr:URITemplate)]
                           [uri hr:hasMethod (d (:method operation))]]
                          (let [[uri-out ts] (hRESTS-output-message-to-triples-map (:output-messages operation) path-prefix)]
                            (concat [[uri wsl:hasOutputMessage uri-out]] ts)))
           msgs (map hRESTS-message-to-triples-map (:input-messages operation))]
       [uri (concat pre-ts (reduce (fn [acum [uri-msg ts]] (concat (conj acum [uri wsl:hasInputMessage uri-msg]) ts)) pre-ts msgs))])))

(defn hRESTS-description-to-triples
  "Transforms a hRESTS service description to a set of triples"
  ([hRESTS-description path-prefix]
     (let [ops (map #(hRESTS-op-to-triples-map %1 path-prefix) (:operations hRESTS-description))
           pre-ts (concat [[(:uri hRESTS-description) rdf:type wsl:Service]]
                          (map (fn [[uri ts]] [(:uri hRESTS-description) wsl:hasOperation uri]) ops))]
       (make-triples (concat pre-ts (reduce (fn [acum [uri ts]] (concat acum ts)) [] ops))))))

(defn hRESTS-message-to-json-map
  "Transforms a hRESTS message description into a hash"
  ([message]
     {:modelReference (str (:model message)) :urlReplacement (str (d (name (:name message))))}))

(defn hRESTS-output-message-to-json-map
  "Transforms a hRESTS message description into a hash"
  ([message path-prefix]
     {:modelReference (str path-prefix (:model message))}))

(defn hRESTS-op-to-json-map
  "Transforms a hRESTS operation description into a hash"
  ([operation path-prefix]
     {:addressTemplate (str path-prefix (:address operation)) :method (:method operation)
      :outputMessage (hRESTS-output-message-to-json-map (:output-messages operation) path-prefix)
      :inputMessages (map hRESTS-message-to-json-map (:input-messages operation))}))

(defn hRESTS-description-to-json
  "Transforms a hRESTS service description to a JSON hash"
  ([hRESTS-description path-prefix]
     (let [ops (map #(hRESTS-op-to-json-map %1 path-prefix) (:operations hRESTS-description))
           result {:uri (str (:uri hRESTS-description))
                   :operations ops}]
       (log :error (str "JSON struct " result)) result)))

(defn hRESTS-message-to-rdfa-map
  "Transforms a hRESTS message description into a hash"
  ([message]
     [:tr {:class "message input-message" :rel "wsl:hasInputMessage"}
      [:td {:class "rdf-msg-alias" :property "http:urlReplacement" :content (name (:name message))}
       (name (:name message))]
      [:td {:class "rdf-msg-property"}
       [:a {:href (str (:model message)) :rel "wasdl:modelReference"} (str (:model message))]]]))

(defn hRESTS-output-message-to-rdfa-map
  "Transforms a hRESTS message description into a hash"
  ([message path-prefix]
     [:div {:class "message output-message"}
      [:div {:class "rdf-msg-property"} [:span {:class "message-subtitle"} "Type of the output resource "]
       [:a {:href (str path-prefix (str (:model message))) :rel "wasdl:modelReference"} (str (:model message))]]]))

(defn hRESTS-op-to-rdfa-map
  "Transforms a hRESTS operation description into a hash"
  ([operation path-prefix]
     [:div {:class "operation" :typeof "wsl:Operation"}
      [:div {:class "operation-title"} [:span {:property "hr:hasMethod"} (str (:method operation))] " " [:span {:property "hr:hasAddress" :datatype "hr:URITemplate"} (:address operation)]]
      [:div {:class "operation-body"}
       [:div {:class "fragment"}
        (:description operation)]
       [:div {:class "input-messages"}
        [:div {:class "fragment-subtitle"} "Input messages"]
        [:table
         [:thead [:th "name"] [:th "RDF property"]]
         (map hRESTS-message-to-rdfa-map (:input-messages operation))]]
       [:div {:class "output-messages"}
        [:div {:class "fragment-subtitle"} "Output message"]
        (hRESTS-output-message-to-rdfa-map (:output-messages operation) path-prefix)]]]))

(defn hRESTS-description-to-rdfa
  "Transforms a hRESTS service description to a JSON hash"
  ([hRESTS-description path-prefix request]
     (let [nsmap (collect-ns (hRESTS-description-to-triples hRESTS-description path-prefix))
           nslistp {:xmlns "http://www.w3.org/1999/xhtml" :xmlns:wsl "http://www.wsmo.org/ns/wsmo-lite#"
                    :xmlns:hr "http://www.wsmo.org/ns/hrests#" :xmlns:http "http://schemas.xmlsoap.org/wsdl/http/"
                    :xmlns:wasdl "http://www.w3.org/ns/sawsdl#"}
           nslistpp (assoc nslistp :version "XHTML+RDFa 1.0")
           ops (map #(hRESTS-op-to-rdfa-map %1 path-prefix) (:operations hRESTS-description))]
       (html [:html nslistpp
              [:head [:title (str "service description " (:uri request))]]
              [:body
               [:style {:type "text/css" :media "screen"} (default-service-css-text)]
               [:div {:class "resource-body" :typeof "wsl:Service"}
                [:div {:class "resource-request"}
                 (str "Service: " (str (:uri hRESTS-description)))
                 [:pan {:id "plaza-logo"} "( plaza )"]]
                [:span {:class "operations-list" :rel "wsl:hasOperation"}
                 ops]]]]))))

(defn default-uri-template-for-service
  "Transforms a URI into a URI template replacing symbols :param by {param}"
  ([uri]
     (let [matcher (re-matcher #":[\w]+" uri)
           matches (loop [m matcher
                          a []]
                     (let [match (re-find matcher)]
                       (if (nil? match) a (recur m (conj a match)))))
           substs (reduce (fn [a i] (assoc a i (str (clojure.contrib.str-utils2/replace i ":" "{") "}"))) {} matches)
           pre (reduce (fn [a [k v]] (clojure.contrib.str-utils2/replace a k v)) uri substs)
           found-port (re-find #"\{[0-9]+\}" pre)]
       (if (nil? found-port) pre (clojure.contrib.str-utils2/replace pre found-port (str ":" (re-find #"[0-9]+" found-port)))))))

(defn augmentate-resource
  "Add Plaza Id property from property ontology if it is not present in the resource"
  ([resource]
     (add-property resource :id plz:restResourceId :string) resource))

(defn deaugmentate-resource
  "Remove the Plaza Id property from property ontology if it is present in the resource"
  ([resource]
     (remove-property-by-uri resource plz:restResourceId) resource))


(defn make-environment-map [resource-or-symbol path ts opts]
  (let [resource (if (keyword? resource-or-symbol) (tbox-find-schema resource-or-symbol) resource-or-symbol)
        resource-type (type-uri resource)
        resource-map (model-to-argument-map resource)
        id-gen (if (nil? (:id-gen-fn opts)) default-uuid-gen (:id-gen-fn opts))
        id-property-alias (if (nil? (:id-property-alias opts)) :id (:id-property-alias opts))
        id-property-uri (if (nil? (:id-property-uri opts)) plz:restResourceId (:id-property-uri opts))
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
        service-uri-gen-fn (if (nil? (:service-uri-gen-fn opts)) default-uri-template-for-service (:service-uri-gen-fn opts)) ]
    {:resource-map resource-map :resource-type resource-type :resource-qname-prefix resource-qname-prefix
     :resource-qname-local resource-qname-local :id-gen-function id-gen :resource-ts resource-ts :resource resource
     :path (str path "*") :path-re (re-pattern (str "(\\..*)?$")) :service-matcher-fn service-matcher-fn
     :schema-matcher-fn schema-matcher-fn :handle-schema-metadata? handle-schema-metadata
     :handle-service-metadata? handle-service-metadata :allowed-methods allowed-methods :get-handle-fn get-handle-fn
     :post-handle-fn post-handle-fn :put-handle-fn put-handle-fn :delete-handle-fn delete-handle-fn :base-path path
     :service-uri-gen-fn service-uri-gen-fn :kind :collection-resource :id-property-alias id-property-alias
     :id-property-uri id-property-uri }))

(defn make-single-resource-environment-map [resource-or-symbol path ts opts]
  (let [coll-env (make-environment-map resource-or-symbol path ts opts) resource (if (keyword? resource-or-symbol) (tbox-find-schema resource-or-symbol) resource-or-symbol)
        id-match (if (nil? (:id-match-fn opts)) default-id-match (:id-match-fn opts))
        augmentated-resource (if (= (str (:id-property-uri coll-env)) plz:restResourceId) (augmentate-resource (:resource coll-env)) (:resource coll-env))
        augmentated-resource-map (if (= (str (:id-property-uri coll-env)) plz:restResourceId)
                                   (model-to-argument-map (augmentate-resource (:resource coll-env)))
                                   (:resource-map coll-env))]
    (-> coll-env (assoc :id-match-function id-match) (assoc :kind :individual) (assoc :resource augmentated-resource) (assoc :resource-map augmentated-resource-map))))

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
                       full-request-uri (make-full-request-uri request environment)
                       ]
                   (if (= env-kind-serv kind-serv)
                     {:body (render-format-service (assoc service :uri full-request-uri) (mime-to-format request) request environment)
                      :headers {"Content-Type" (format-to-mime request)}
                      :status 200}
                     (if-let [parent-service (tbox-find-parent-service (:path environment))]
                       (let [schema (-> (:resource environment) (deaugmentate-resource))
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
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-delete [id request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-single-resource-query-from-resource-map mapping id)
        results (in (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))


(defn handle-get-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))
        results (let [rd-argspp []
                      rd-argsp (if (:limit request) (conj rd-argspp (f :limit (:limit request))) rd-argspp)
                      rd-args (if (:offset request) (conj rd-argsp (f :offset (:offset request))) rd-argsp)]
                  (rd (ts (:resource-ts environment)) query rd-args))
        triples (distinct (flatten-1 results))]
    {:body (render-triples triples (mime-to-format request) (:resource environment) request)
     :headers {"Content-Type" (format-to-mime request)}
     :status 200}))

(defn handle-post-collection [request environment]
  (let [params (dissoc (:params request) (name (:id-property-alias environment)))
        mapping (apply-resource-argument-map params (:resource-map environment))
        {id :id resource-uri :uri} ((:id-gen-function environment) request environment)
        triples-pre  (conj  (build-triples-from-resource-map resource-uri mapping) [resource-uri rdf:type (:resource-type environment)])
        triples (if (nil? id) triples-pre (conj triples-pre [resource-uri (:id-property-uri environment) (d id)]))]
    (out (ts (:resource-ts environment)) triples)
    {:body (render-triples triples :xml (:resource environment) request)
     :headers {"Content-Type" "application/xml"}
     :status 201}))

(defn handle-delete-collection [request environment]
  (let [mapping (apply-resource-argument-map (:params request) (:resource-map environment))
        query (build-query-from-resource-map mapping (:resource-type environment))
        results (in (ts (:resource-ts environment)) query)
        triples (distinct (flatten-1 results))]
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
       (tbox-register-service (:path env-pre#) (hRESTS-collection-service-description ((:service-uri-gen-fn env-pre#) (:base-path env-pre#)) (:resource env-pre#) env-pre#))
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
       (tbox-register-service (:path env-pre#) (hRESTS-single-service-description ((:service-uri-gen-fn env-pre#) (:base-path env-pre#)) (:resource env-pre#) env-pre#))
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
