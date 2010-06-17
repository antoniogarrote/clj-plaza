;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 26.05.2010

(ns plaza.rdf.schemas
  (:use (plaza.rdf core sparql predicates)
        (plaza.rdf.implementations common)
        (plaza utils)))

;; auxiliary functions
(defn find-property [prop triples]
  (first (filter (tc (predicate? (uri? prop))) triples)))

;; Protocols
(defprotocol OntologyModel
  "Functions that can be applied to a RDF ontology schema"
  (type-uri [this] "Returns the URI of this model")
  (add-property [this alias uri range] "Adds a new property to the model")
  (to-pattern [this props] [this subject props] "Builds a pattern suitable to look for instances of this type. A list of properties can be passed optionally")
  (to-map [this triples] "Transforms a RDF triple set into a map of properties using the provided keys")
  (property-uri [this alias] "Returns the URI for the alias of a property")
  (property-alias [this uri] "Returns the alias for a property URI")
  (parse-prop-value [this alias val] "Parses the provided string value into the right java value for the property defined by alias")
  (prop-value-to-triple-value [this alias val] "Transforms the provided value into the a valid Plaza triple literal, datatype literal or resource")
  (to-rdf-triples [this] "Transforms this schema into a set of triples")
  (aliases [this] "Returns all tha aliases for the properties of the model"))

(defn- build-pattern-for-model
  ([type-uri subj props properties]
     (let [opt-props-to-add (vec (clojure.set/difference (set (keys properties)) (set props)))
           props-to-add (if (empty? props) (keys properties) props)
           mandatory-pattern (reduce (fn [ac it]
                                       (when (nil? (it properties)) (throw (Exception. (str "Unknown property: " it " for type " type-uri " " properties))))
                                       (conj ac [subj (it properties) (keyword (str "?" (keyword-to-string it)))]))
                                     [(opt [subj rdf:type (resource-id type-uri)])]
                                     props-to-add)
           optional-pattern (reduce (fn [ac it]
                                      (when (nil? (it properties)) (throw (Exception. (str "Unknown property: " it " for type " type-uri " " properties))))
                                      (conj ac (opt [subj (it properties) (keyword (str "?" (keyword-to-string it)))])))
                                    []
                                    opt-props-to-add)]
       (make-pattern (concat mandatory-pattern optional-pattern)))))

;; Types
(deftype RDFSModel [this-uri properties ranges] OntologyModel
  (type-uri [this] this-uri)
  (add-property [this alias uri range] (let [prop-val (if (coll? uri) (apply rdf-resource uri) (rdf-resource uri))
                                             range-val (if (supported-datatype? range)
                                                         {:kind :datatype :range (rdf-resource (datatype-uri range))}
                                                         {:kind :resource :range (if (coll? range) (apply rdf-resource range) range)})]
                                         (dosync (alter properties (fn [old-props] (assoc old-props alias prop-val)))
                                                 (alter ranges (fn [old-ranges] (assoc old-ranges alias range-val))))))
  (toString [this] (str this-uri " " @properties))
  (to-pattern [this subject props] (let [subj (if (instance? plaza.rdf.core.RDFResource subject) subject (if (coll? subject) (apply rdf-resource subject) (rdf-resource subject) ))]
                                     (build-pattern-for-model this-uri subj props @properties)))
  (to-pattern [this props] (build-pattern-for-model this-uri ?s props @properties))
  (to-map [this triples-or-vector] (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))]
                                     (reduce (fn [ac it] (let [prop (str (resource-id (it @properties)))
                                                               val (find-property prop triples)]
                                                           (if (nil? val) ac (assoc ac it (nth val 2)))))
                                             {}
                                             (keys @properties))))
  (property-uri [this alias] (alias @properties))
  (property-alias [this uri] (first (filter #(= (str (get @properties %1)) (str uri)) (keys @properties))))
  (parse-prop-value [this alias val] (let [{kind :kind range :range} (get @ranges alias)]
                                        (if (= kind :resource) (rdf-resource val)
                                            (.parse (find-jena-datatype (str range)) val))))
  (prop-value-to-triple-value [this alias val] (let [{kind :kind range :range} (get @ranges alias)]
                                                 (if (= kind :resource) (rdf-resource val)
                                                     (let [jena-type (find-jena-datatype (str range))]
                                                       (parse-literal-lexical (str "\"" val "\"^^<" (.getURI jena-type) ">"))))))
  (to-rdf-triples [this] (let [subject (rdf-resource (type-uri this))]
                           (reduce (fn [ts k] (let [prop (rdf-resource (get @properties k))
                                                    range (:range (get @ranges k))
                                                    tsp (conj ts [prop (rdf-resource rdfs:range) (rdf-resource range)])]
                                                (conj tsp [prop (rdf-resource rdfs:domain) subject])))
                                   [[subject (rdf-resource rdf:type) (rdf-resource rdfs:Class)]]
                                   (keys @properties))))
  (aliases [this] (keys @properties)))

;; Type constructor

(defn make-rdfs-schema
  ([typeuri-pre & properties]
     (let [typeuri (if (coll? typeuri-pre) (apply rdf-resource typeuri-pre) (rdf-resource typeuri-pre))
           props-map-pre (apply hash-map properties)
           maps (reduce (fn [[ac-props ac-ranges] it]
                          (let [{uri :uri range :range} (it props-map-pre)
                                prop-val (if (coll? uri) (apply rdf-resource uri) (rdf-resource uri))
                                range-val (if (supported-datatype? range)
                                            {:kind :datatype :range (rdf-resource (datatype-uri range))}
                                            {:kind :resource :range (if (coll? range) (apply rdf-resource range) range)})]
                            [(assoc ac-props it prop-val)
                             (assoc ac-ranges it range-val)])) [{} {}] (keys props-map-pre))]
       (plaza.rdf.schemas.RDFSModel. typeuri (ref (first maps)) (ref (second maps))))))

;; RDFS schema

(defn load-rdfs-schemas []
  (defonce rdfs:Class-schema
    (make-rdfs-schema rdfs:Class
                      :type   {:ur  rdf:type    :range rdfs:Resource}
                      :range  {:uri rdfs:range  :range rdfs:Class}
                      :domain {:uri rdfs:domain :range rdfs:Class})))
