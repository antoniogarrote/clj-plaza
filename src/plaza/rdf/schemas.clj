;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 26.05.2010

(ns plaza.rdf.schemas
  (:use (plaza utils)
        (plaza.rdf core sparql predicates)
        (plaza.rdf.implementations common)
        [clojure.contrib.logging :only [log]]
        [clojure.contrib.seq-utils :only [includes?]]
        (plaza utils)))

(defonce *vocabularies-to-load* (ref []))

(defmacro declare-schemas-to-load
  ([& body]
     `(let [to-load# (fn [] ~@body)]
        (dosync (alter *vocabularies-to-load* (fn [old#] (conj old# to-load#)))))))

(defn init-vocabularies
  ([] (dosync (alter *vocabularies-to-load*
                     (fn [old] (doseq [v old] (v)) [])))))

;; auxiliary functions
(defn find-property [prop triples]
  (first (filter (tc (predicate? (uri? prop))) triples)))

;; Protocols
(defprotocol OntologyModel
  "Functions that can be applied to a RDF ontology schema"
  (type-uri [this] "Returns the URI of this schema, this uri will be used in a triple like [schemaUri rdf:type rdfs:Resource]")
  (super-type-uris [this] "Returns the list of all the URIs this schema is rdf:type besides rdfs:Resource")
  (add-property [this alias uri range] "Adds a new property to the model")
  (remove-property-by-uri [this uri] "Removes a property provided its URI")
  (remove-property-by-alias [this alias] "Removes a property provided its alias")
  (to-pattern [this props] [this subject props] "Builds a pattern suitable to look for instances of this type. A list of properties can be passed optionally")
  (to-map [this triples] "Transforms a RDF triple set into a map of properties using the provided keys")
  (property-uri [this alias] "Returns the URI for the alias of a property")
  (property-range-description [this alias] "Returns a map with the description of the range for the property identified by the provided alias")
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
(declare wrap-rdfs-schema)

(deftype RDFSModel [this-uri super-types properties ranges] OntologyModel
  (type-uri [this] this-uri)

  (super-type-uris [this] (conj super-types this-uri))

  (add-property [this alias uri range] (let [prop-val (if (coll? uri) (apply rdf-resource uri) (rdf-resource uri))
                                             range-val (if (supported-datatype? range)
                                                         {:kind :datatype :range (rdf-resource (datatype-uri range))}
                                                         {:kind :resource :range (if (coll? range) (apply rdf-resource range) range)})
                                             propsp (assoc properties alias prop-val)
                                             rangesp (assoc ranges alias range-val)]
                                         (wrap-rdfs-schema this-uri super-types propsp rangesp)))

  (remove-property-by-uri [this uri] (let [alias (property-alias this uri)]
                                       (if (nil? alias)
                                         this
                                         (let [propsp (dissoc properties alias)
                                               rangesp (dissoc ranges alias)]
                                           (wrap-rdfs-schema this-uri super-types propsp rangesp)))))

  (remove-property-by-alias [this alias] (if (nil? alias)
                                           this
                                           (let [propsp (dissoc properties alias)
                                                 rangesp (dissoc ranges alias)]
                                             (wrap-rdfs-schema this-uri super-types propsp rangesp))))

  (toString [this] (str this-uri  " " properties))

  (to-pattern [this subject props] (let [subj (if (instance? plaza.rdf.core.RDFResource subject) subject (if (coll? subject) (apply rdf-resource subject) (rdf-resource subject) ))]
                                     (build-pattern-for-model this-uri subj props properties)))

  (to-pattern [this props] (build-pattern-for-model this-uri ?s props properties))

  (to-map [this triples-or-vector] (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))]
                                     (reduce (fn [ac it] (let [prop (str (resource-id (it properties)))
                                                               val (find-property prop triples)]
                                                           (if (nil? val) ac (assoc ac it (nth val 2)))))
                                             {}
                                             (keys properties))))

  (property-uri [this alias] (alias properties))

  (property-range-description [this alias] (alias ranges))

  (property-alias [this uri] (first (filter #(= (str (get properties %1)) (str uri)) (keys properties))))

  (parse-prop-value [this alias val] (let [{kind :kind range :range} (get ranges alias)]
                                       (if (= kind :resource) (rdf-resource val)
                                           (.parse (find-jena-datatype (str range)) val))))

  (prop-value-to-triple-value [this alias val] (let [{kind :kind range :range} (get ranges alias)]
                                                 (if (= kind :resource) (rdf-resource val)
                                                     (let [jena-type (find-jena-datatype (str range))]
                                                       (parse-literal-lexical (str "\"" val "\"^^<" (.getURI jena-type) ">"))))))

  (to-rdf-triples [this] (let [subject (rdf-resource (type-uri this))]
                           (let [pre-super-types (reduce (fn [ts k] (let [prop (rdf-resource (get properties k))
                                                                          range (:range (get ranges k))
                                                                          tsp (conj ts [prop (rdf-resource rdfs:range) (rdf-resource range)])]
                                                                      (conj tsp [prop (rdf-resource rdfs:domain) subject])))
                                                         [[subject (rdf-resource rdf:type) (rdf-resource rdfs:Class)]]
                                                         (keys properties))
                                 super-types-triples (map (fn [st] [subject (rdf-resource rdf:type) (rdf-resource st)]) super-types)]
                             (concat pre-super-types super-types-triples))))

  (aliases [this] (keys properties)))

;; Type constructor

(defn make-rdfs-schema
  "Defines a new RDFS Class with the provided URI and properties"
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
       (plaza.rdf.schemas.RDFSModel. typeuri '() (first maps) (second maps)))))

(defn extend-rdfs-schemas
  "Defines a new RDFS Class that inherits all the properties defined in the list of provided schemas"
  ([typeuri schemas & properties]
     (let [props (flatten
                  (map (fn [schema]
                         (let [aliases (aliases schema)]
                           (map (fn [alias] {:alias alias :property (property-uri schema alias) :range (property-range-description schema alias)}) aliases)))
                       schemas))
           prop-map (reduce (fn [acum prop]
                              (if (contains? acum (:alias prop))
                                (assoc acum (:property prop) (:property prop))
                                (assoc acum (:alias prop) (:property prop))))
                            {} props)
           range-map (reduce (fn [acum prop]
                               (let [alias (if (nil? (get prop-map (:alias prop))) (:property prop) (:alias prop))]
                                 (assoc acum alias (:range prop))))
                             {} props)]
       (plaza.rdf.schemas.RDFSModel. typeuri (map #(type-uri %1) schemas) prop-map range-map))))

;; Utils

(defn wrap-rdfs-schema
  ([uri super-types props ranges]
     (plaza.rdf.schemas.RDFSModel. uri super-types props ranges)))

(defn- parse-rdf-schema-from-model
  ([model resource]
     (let [properties (flatten-1 (model-pattern-apply model [[?s rdfs:domain resource] [?s rdfs:range ?o]]))
           prop-ranges (reduce (fn [acum [s p o]] (if (= (str p) rdfs:range)
                                                    (let [prop-val (if (supported-datatype? (str o)) (datatype-symbol (str o)) (str o))]
                                                      (conj acum {:uri (str s) :range prop-val} ))
                                                    acum))
                               [] properties)]
       (reduce (fn [schema prop-map]
                 (let [prop-alias (keyword (extract-local-part-uri (:uri prop-map)))]
                   (add-property schema prop-alias (:uri prop-map) (:range prop-map))))
               (make-rdfs-schema (str resource)) prop-ranges))))

(defn parse-rdfs-schemas-from-model
  "Builds RDFS definition of resources from the set of triples in a RDF model"
  ([model]
     (let [resources-triples (flatten-1 (model-pattern-apply model [[?s rdf:type rdfs:Class]]))
           resources (map (fn [[resource _p _o]] (str resource)) resources-triples)]
       (map (fn [resource] (parse-rdf-schema-from-model model resource)) resources))))

;; RDFS schema

(declare-schemas-to-load
 (defonce rdfs:Class-schema
   (make-rdfs-schema rdfs:Class
                     :type   {:ur  rdf:type    :range rdfs:Resource}
                     :range  {:uri rdfs:range  :range rdfs:Class}
                     :domain {:uri rdfs:domain :range rdfs:Class})))
