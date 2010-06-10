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
   [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.json :as json]))


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
                        [(str s) (str p) {:value (literal-value o) :datatype (literal-datatype-uri o)}]
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
                                       value (literal-value o)]
                                   (if (nil? pred) acum
                                       (assoc acum pred value)))
                                 (let [pred (property-alias schema (str p))]
                                   (if (nil? pred) acum
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
     [:div {:about (str s)} [:a {:href (str s)} (str s)]
      [:ul (map (fn [[s p o]]
                  [:li (if (is-literal o)
                         [:span (str (keyword-to-string (property-alias schema (str p))) ": ")
                          [:span {:property (build-curie nsmap (str p)) :datatype (literal-datatype-uri o)} (literal-value o)]]
                         [:span (str (keyword-to-string (property-alias schema (str p))) ": ")
                          [:a {:href (str o) :rel (build-curie nsmap (str p))} (str o)]])]) ts)]]))

(defn ns-list [nsmap]
  (reduce (fn [acum [k v]] (if (= k "/") acum (assoc acum (str "xmlns:" v) k))) {} nsmap))

(defn to-rdfa-triples
  ([ts schema]
     (let [nsmap (collect-ns ts)
           gts (group-by (fn [[s p o]] (str s)) ts)
           rdfa-ts (map (fn [[s tsp]] (to-rdfa-triple s tsp schema nsmap)) gts)
           nslistp (assoc  (ns-list nsmap) :xmlns "http://www.w3.org/1999/xhtml")
           nslistpp (assoc nslistp :version "XHTML+RDFa 1.0")]
       (html [:html nslistpp
              [:head [:title ""]]
              [:body
               rdfa-ts]]))))

(defn render-triples [triples format schema]
  (if (or (= format :json) (= format :js3) (= format :js))
    (if (or (= format :json) (= format :js))
      (to-json-triples triples schema)
      (to-js3-triples triples))
    (if (or (= format :html) (= format :xhtml) (= format :rdfa))
      (to-rdfa-triples triples schema)
      (let [m (defmodel (model-add-triples triples))
            w (java.io.StringWriter.)]
        (output-string m w format)
        (.toString w)))))

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
      "js" :json
      "js3" :js3
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
      "rdfa" "application/html+xml"
      "html" "text/html"
      "xhtml" "application/html+xml"
      "application/xml")))

(defn default-uuid-gen [prefix local request]
  (random-resource-id (str prefix local)))

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
