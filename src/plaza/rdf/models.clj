;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 26.05.2010

(ns plaza.rdf.models
  (:use (plaza.rdf core sparql predicates)
        (plaza utils)))

;; auxiliary functions
(defn find-property [prop triples]
  (first (filter (tc (predicate? (uri? prop))) triples)))

;; Protocols
(defprotocol OntologyModel
  "Functions that can be applied to a RDF ontology schema"
  (type-uri [model] "Returns the URI of this model")
  (to-pattern [model props] [model subject props] "Builds a pattern suitable to look for instances of this type. A list of properties can be passed optionally")
  (to-map [model triples] "Transforms a RDF triple set into a map of properties using the provided keys"))

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
(deftype RDFSModel [type-uri properties] OntologyModel
  (type-uri [this] type-uri)
  (toString [this] (str type-uri " " properties))
  (to-pattern [this subject props] (let [subj (if (instance? plaza.rdf.core.RDFResource subject) subject (if (coll? subject) (apply rdf-resource subject) (rdf-resource subject) ))]
                                     (build-pattern-for-model type-uri subj props properties)))
  (to-pattern [this props] (build-pattern-for-model type-uri ?s props properties))
  (to-map [this triples-or-vector] (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))]
                                     (reduce (fn [ac it] (let [prop (str (resource-id (it properties)))
                                                               val (find-property prop triples)]
                                                           (if (nil? val) ac (assoc ac it (nth val 2)))))
                                             {}
                                             (keys properties)))))

;; Type constructor
(defn def-rdfs-model
  ( [type-uri-pre & properties]
      (let [type-uri (apply rdf-resource type-uri-pre)
            props-map-pre (apply hash-map properties)
            props-map (reduce (fn [ac it] (assoc ac it (if (coll? (it props-map-pre)) (apply rdf-resource (it props-map-pre)) (rdf-resource (it props-map-pre))))) {} (keys props-map-pre))]
        (plaza.rdf.models.RDFSModel. type-uri props-map))))
