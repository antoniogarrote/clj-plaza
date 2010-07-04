;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 01.07.2010

(ns plaza.rest.utils
  (:use
   [hiccup.core]
   [plaza.utils]
   [plaza.rdf.core]
   [plaza.rdf.schemas]
   [plaza.rdf.sparql]
   [plaza.rdf.predicates]
   [plaza.rdf.vocabularies wsmo-lite hrests plaza]
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
                                (if (nil? arg) [(inc c) (conj acum [uri (keyword (str "?o" c))])]
                                    (if (coll? arg)
                                      [c (concat acum (map #(vector uri (f %1)) arg))]
                                      [c (conj acum [uri (f arg)])]))))
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

(defn- insert-value
  ([tmp-obj pred val]
     (let [old-value (get tmp-obj pred)]
       (if (nil? old-value)
         (assoc tmp-obj pred val)
         (if (coll? old-value)
           (assoc tmp-obj pred (conj (get tmp-obj pred) val))
           (assoc tmp-obj pred [old-value val]))))))

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
                                   (if (nil? pred) (insert-value acum (str p) value)
                                       (insert-value acum pred value)))
                                 (let [pred (property-alias schema (str p))]
                                   (if (nil? pred) (if (= (str p) rdf:type)
                                                     (insert-value acum :type (str o))
                                                     (insert-value acum (str p) (str o)))
                                       (insert-value acum pred (str o))))))
                             {} (first (vals gts)))]
             (json/json-str (assoc tsp :uri (str (first (keys gts))))))
           (json/json-str (map (fn [[s tss]]
                                 (let [jsts (reduce (fn [acum [s p o]]
                                                      (if (is-literal o)
                                                        (let [pred (property-alias schema (str p))
                                                              value (literal-value o)]
                                                          (if (nil? pred) acum
                                                              (insert-value acum pred value)))
                                                        (let [pred (property-alias schema (str p))]
                                                          (if (nil? pred)
                                                            (if (= (str p) rdf:type)
                                                              (insert-value acum :type (str o))
                                                              (insert-value acum (str p) (str o)))
                                                            (insert-value acum pred (str o))))))
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
           aliases-collection (filter #(not (= %1 :id)) (aliases resource))]
       (condp = method
         :get {:identifier (name method)
               :label (str "HTTP " (name method) " method")
               :method (name method)
               :address path
               :description (default-service-operation-description path (name method))
               :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) aliases-collection)
               :output-messages {:name "theResources" :description "The returned resources" :model resource-schema :model-type resource-type}}
         :post {:identifier (name method)
                :label (str "HTTP " (name method) " method")
                :method (name method)
                :address path
                :description (default-service-operation-description path (name method))
                :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) aliases-collection)
                :output-messages {:name "theResource" :description "The newly created resource" :model resource-schema :model-type resource-type}}
         :delete {:identifier (name method)
                  :label (str "HTTP " (name method) " method")
                  :method (name method)
                  :address path
                  :description (default-service-operation-description path (name method))
                  :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) aliases-collection)
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
               :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)}) (aliases resource))
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
                  :input-messages (map (fn [alias] {:name alias :model (property-uri resource alias)})
                                       (filter #(not (= -1 (.indexOf path (keyword-to-string %1)))) (aliases resource))) ;; we just want properties in the URI
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
                   :operations ops}] result)))

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
     (add-property resource :id plz:restResourceId :string)))

(defn deaugmentate-resource
  "Remove the Plaza Id property from property ontology if it is present in the resource"
  ([resource]
     (remove-property-by-uri resource plz:restResourceId)))

(defn add-filter
  "adds a filter to the environment"
  ([kind-filter filter-fn filters]
     (let [old-kind-filters (get filters kind-filter)
           new-kind-filters (conj old-kind-filters filter-fn)]
       (assoc filters kind-filter new-kind-filters))))
