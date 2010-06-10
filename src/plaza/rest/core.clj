;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 07.06.2010

(ns plaza.rest.core
  (:use
   [plaza.utils]
   [plaza.rdf.core]
   [plaza.rdf.schemas]
   [plaza.rdf.sparql]
   [plaza.rdf.predicates]
   [clojure.contrib.logging :only [log]]))


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

(defn render-triples [triples format]
  (let [m (defmodel (model-add-triples triples))
        w (java.io.StringWriter.)]
    (output-string m w format)
    (.toString w)))
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
