;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 30.04.2010

(ns plaza.rdf.core
  (:use [plaza.utils])
  (:import (com.hp.hpl.jena.rdf.model ModelFactory)
           (com.hp.hpl.jena.reasoner.rulesys RDFSRuleReasonerFactory)
           (com.hp.hpl.jena.vocabulary ReasonerVocabulary)
           (com.hp.hpl.jena.datatypes.xsd XSDDatatype)
           (com.hp.hpl.jena.datatypes.xsd.impl XMLLiteralType)
           (com.hp.hpl.jena.query QueryFactory)
           (com.hp.hpl.jena.sparql.syntax Element ElementGroup ElementOptional ElementFilter)
           (com.hp.hpl.jena.graph Node Triple)
           (com.hp.hpl.jena.sparql.expr E_Str E_Lang E_Datatype E_Bound E_IsIRI E_IsURI E_IsBlank E_IsLiteral E_GreaterThanOrEqual E_GreaterThan
                                        E_LessThanOrEqual E_LessThan E_NotEquals E_Equals E_Subtract E_Add E_Multiply E_Divide)))


;; Axiomatic vocabulary

(def rdfs "http://www.w3.org/2000/01/rdf-schema#")
(def rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#")

(def rdf:Property "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")
(def rdf:type "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

(def rdfs:subPropertyOf "http://www.w3.org/2000/01/rdf-schema#subPropertyOf")
(def rdfs:domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def rdfs:range "http://www.w3.org/2000/01/rdf-schema#range")
(def rdfs:Class "http://www.w3.org/2000/01/rdf-schema#Class")


;;; Declaration of a model


(defn build-model
  "Creates a new model to store triples"
  ([]
     (agent (ModelFactory/createDefaultModel))))


;;; Root bindings


;; The root *rdf-model* that will be used by default
(def *rdf-model* (build-model))

;; The root *namespace* that will be used by default
(def *rdf-ns* "http://plaza.org/ontologies/")

;; sets the root bindings, useful when reading configuration.
;; with-model and with-rdf-ns should be use in actual code.
(defn alter-root-model
  "Alters the root binding for the default model. This function
   should only be used when setting up the application, with-model
   macro should be used by default"
  ([new-model]
     (alter-var-root #'*rdf-model* (fn [_] new-model))))

(defn alter-root-rdf-ns
  "Alters the root binding for the default model. This function
   should only be used when setting up the application, with-rdf-ns
   macro should be used by default"
  ([new-rdf-ns]
     (alter-var-root #'*rdf-ns* (fn [_] new-rdf-ns))))

(defmacro with-rdf-ns
  "Sets up the default namespace for a set of forms"
  [ns & rest]
  `(binding [*rdf-ns* ~ns]
     ~@rest))

(defmacro with-model
  "Sets up the default model for a set of forms"
  [model & rest]
  `(binding [*rdf-model* ~model]
     ~@rest))

(defmacro defmodel
  "Sets up the default model for a set of forms and returns the model"
  [& rest]
  `(binding [*rdf-model* (build-model)]
     ~@rest
     *rdf-model*))

;;; Namespaces

;; Dictionaries for data
(def *rdf-ns-table* (ref {:rdfs "http://www.w3.org/2000/01/rdf-schema#"
                          :rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}))

(def *rdf-ns-inverse-table* (ref {"http://www.w3.org/2000/01/rdf-schema#" :rdfs
                                  "http://www.w3.org/1999/02/22-rdf-syntax-ns#" :rdf }))

(defn register-rdf-ns
  "Add a registered namespace to the registry of namespaces"
  ([ns uri]
     (dosync
      (alter *rdf-ns-table* (fn [old] (assoc old (keyword ns) uri)))
      (alter *rdf-ns-inverse-table* (fn [old] (assoc old uri (keyword ns)))))))

(defn find-ns-registry
  "Checks if a provided namespace has an associated uri in the registry of namespaces"
  ([ns]
     (dosync
      (get @*rdf-ns-table* ns))))

(defn find-ns-registry-by-uri
  "Checks if a provided namespace has an associated uri in the registry of namespaces"
  ([uri]
     (dosync
      (get @*rdf-ns-inverse-table* uri))))

(defn expand-ns
  "Provided a pair [ns local] tries to expand the ns with the information in the rdf-ns registry"
  ([ns local]
     (let [registry-ns (find-ns-registry ns)
           expanded-ns (keyword-to-string (if (nil? registry-ns) ns registry-ns))]
       (str expanded-ns (keyword-to-string local)))))

;;; Manipulation of models

(defn reset-model
  "Resets the root model with a fresh model object"
  ([] (alter-root-model (build-model))))

(defn rdf-property
  "Creates a new rdf property"
  ([ns local] (.createProperty @*rdf-model* (expand-ns ns local)))
  ([uri] (if (.startsWith (keyword-to-string uri) "http://")
           (.createProperty @*rdf-model* (keyword-to-string uri))
           (.createProperty @*rdf-model* *rdf-ns* (keyword-to-string uri)))))

(defn rdf-resource
  "Creates a new rdf resource"
  ([ns local] (.createResource @*rdf-model* (expand-ns ns local)))
  ([uri] (if (.startsWith (keyword-to-string uri) "http://")
           (.createResource @*rdf-model* (keyword-to-string uri))
           (.createResource @*rdf-model* (str *rdf-ns* (keyword-to-string uri))))))

(defn is-blank-node
  "Checks if a RDF resource"
  ([resource]
     (if (or (string? resource)
             (keyword? resource))
       false
       (.isAnon resource))))

(defn blank-node
  ([]
     (.createResource @*rdf-model* (com.hp.hpl.jena.rdf.model.AnonId.)))
  ([id]
     (let [anon-id (keyword-to-string id)]
       (.createResource @*rdf-model* (com.hp.hpl.jena.rdf.model.AnonId. anon-id)))))

(defn b
  ([] (blank-node))
  ([id] (blank-node id)))


(defn blank-node-id
  ([b]
     (keyword (.toString (.getId b)))))

(defn resource-uri
  "Returns the URI of a resource (subject, predicate or object)"
  ([resource]
     (let [tmp (.getURI resource)]
       (if (nil? tmp)
         (.toString (.getId resource))
         tmp))))

(defn resource-qname-prefix
  "Returns the prefix part of the qname of the resource"
  ([resource]
     (.getNameSpace resource)))

(defn resource-qname-local
  "Returns the local part of the qname of the resource"
  ([resource]
     (.getLocalName resource)))

(defn is-resource
  "Matches a literal with a certain literal value"
  ([atom]
     (cond (or (= (class atom) com.hp.hpl.jena.rdf.model.impl.ResourceImpl)
               (= (class atom) com.hp.hpl.jena.rdf.model.impl.PropertyImpl))
           true
           true false)))

(defn rdf-literal
  "Creates a new rdf literal"
  ([lit] (.createLiteral @*rdf-model* lit true))
  ([lit lang] (.createLiteral @*rdf-model* lit lang)))

(defn l
  "shorthand for rdf-literal"
  ([lit] (rdf-literal lit))
  ([lit lang] (rdf-literal lit lang)))


(defn find-datatype
  "Finds the right datatype object from the string representation"
  ([literal]
     (let [lit (let [literal-str (keyword-to-string literal)]
                 (if (.startsWith literal-str "http://")
                   (aget (.split literal-str "#") 1)
                   literal))]
       (cond
        (= "xmlliteral" (.toLowerCase (keyword-to-string lit))) XMLLiteralType/theXMLLiteralType
        (= "anyuri" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDanyURI
        (= "boolean" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDboolean
        (= "byte" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDbyte
        (= "date" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDdate
        (= "datetime" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDdateTime
        (= "decimal" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDdecimal
        (= "double" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDdouble
        (= "float" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDfloat
        (= "int" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDint
        (= "integer" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDinteger
        (= "long" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDlong
        (= "string" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDstring))))


(defn rdf-typed-literal
  "Creates a new rdf literal with an associated type"
  ([lit] (.createTypedLiteral @*rdf-model* lit))
  ([lit type] (let [dt (find-datatype type)]
                (.createTypedLiteral @*rdf-model* lit dt))))

(defn d
  "shorthand for rdf-typed-literal"
  ([val] (rdf-typed-literal val))
  ([val dt] (rdf-typed-literal val dt)))

(defn literal-value
  "Returns the value of a literal"
  ([lit] (.getValue lit)))

(defn literal-language
  "Returns the language of a literal"
  ([lit] (.getLanguage lit)))

(defn literal-datatype-uri
  "Returns the datatype URI associated to this literal"
  ([lit] (.getDatatypeURI lit)))

(defn literal-lexical-form
  "Returns the lexical form associated to this literal"
  ([lit] (.getLexicalForm lit)))

(defn triple-subject
  "Defines a subject for a statement"
  ([subject]
     (if (is-resource subject) subject
         (if (coll? subject)
           (let [[rdf-ns local] subject]
             (rdf-resource rdf-ns local))
           (if (is-blank-node subject)
             subject
             (if (.startsWith (keyword-to-string subject) "?")
               (keyword subject)
               (rdf-resource subject)))))))

(defn triple-predicate
  "Defines the predicate of a statement"
  ([predicate]
     (if (is-resource predicate) predicate
         (if (coll? predicate)
           (let [[rdf-ns local] predicate]
             (rdf-property rdf-ns local))
           (if (is-blank-node predicate)
             (throw (Exception. "Blank node cannot be predicate in a model"))
             (if (.startsWith (keyword-to-string predicate) "?")
               (keyword predicate)
               (rdf-property predicate)))))))

(defn triple-object
  "Defines the object of a statement"
  ([literal]
     (if (instance? com.hp.hpl.jena.rdf.model.Literal literal)
       literal
       (triple-subject literal))))

(defn rdf-triple
  "Parses a RDF triple"
  ([to-parse]
     (if (coll? (first to-parse))
       (map #(rdf-triple %1) to-parse)
       (if (= (count to-parse) 3)
         (let [[s p o] to-parse]
           [(triple-subject s)
            (triple-predicate p)
            (triple-object o)])
         (let [[s ts] to-parse
               c      (fold-list ts)]
           (vec (map (fn [po] (rdf-triple [s (nth po 0) (nth po 1)]))  c)))))))

(defn make-triples
  "Builds a new collection of triples"
  ([ts]
     (with-meta
       (reduce
        (fn [acum item]
          (if (and (coll? item) (coll? (first item)))
            (vec (concat acum item))
            (conj acum item)))
        []
        (map (fn [t] (rdf-triple t)) ts))
       {:triples true})))


(defn model-add-triples
  "Add a collection of triples to a model"
  ([ts]
     (let [mts (if (:triples (meta ts)) ts (make-triples ts))]
       (loop [acum mts]
         (when (not (empty? acum))
           (let [[ms mp mo] (first acum)]
             (do
               (send *rdf-model* (fn [ag] (do (.add ag ms mp mo) ag)))
               (await *rdf-model*)
               (recur (rest acum)))))))))

(defn is-model
  "Checks if an object is a model"
  ([obj]
     (or (= (class obj) com.hp.hpl.jena.rdf.model.impl.ModelCom)
         (if (= (class obj) clojure.lang.Agent)
           (= (class @obj) com.hp.hpl.jena.rdf.model.impl.ModelCom)
           false))))


(defn model-to-triples
  "Extracts the triples stored into a model"
  ([model]
     (if (is-model model)
       (let [model-it (.listStatements @model)]
         (loop [should-continue (.hasNext model-it)
                acum []]
           (if should-continue
             (let [st (.nextStatement model-it)
                   subject (.getSubject st)
                   predicate (.getPredicate st)
                   object (let [node (.getObject st)]
                            (cond
                             (.isLiteral node) (.as node com.hp.hpl.jena.rdf.model.Literal)
                             true              (.as node com.hp.hpl.jena.rdf.model.Resource)))]
               (recur (.hasNext model-it)
                      (conj acum [subject predicate object])))
             acum)))
       ;; we have already received triples
       model)))

;; Models IO

(defn- parse-format
  ([format]
     (cond (= (.toLowerCase (keyword-to-string format)) "xml") "RDF/XML"
           (= (.toLowerCase (keyword-to-string format)) "ntriple") "N-TRIPLE"
           (= (.toLowerCase (keyword-to-string format)) "n3") "N3"
           (= (.toLowerCase (keyword-to-string format)) "ttl") "TURTLE"
           (= (.toLowerCase (keyword-to-string format)) "turtle") "TTL")))

(defn document-to-model
  "Adds a set of triples read from a serialized document into a model"
  ([format stream]
     (let [format (parse-format format)]
       (do
         (send *rdf-model* (fn [ag] (.read ag stream (find-ns-registry *rdf-ns*) format)))
         (await *rdf-model*)
         *rdf-model*))))


(defn model-to-format
  "Writes a model using the chosen format"
  ([]
     (.write @*rdf-model* *out* (parse-format :ntriple)))
  ([format]
     (.write @*rdf-model* *out* (parse-format format)))
  ([model format]
     (.write @model *out* (parse-format format))))

(defn triples-to-format
  "Writes a set of triple using the "
  ([triples & args]
     (with-model (build-model)
       (model-add-triples triples)
       (apply model-to-format args))))

;; model manipulation predicates
(defn subject-from-triple
  "Extract the subject from a triple"
  ([[s p o]]
     s))

(defn s
  "Shortcut for subject-from-triple"
  ([t] (subject-from-triple t)))

(defn predicate-from-triple
  "Extract the predicate from a triple"
  ([[s p o]]
     p))

(defn p
  "Shortcut for predicate-from-triple"
  ([t] (predicate-from-triple t)))

(defn object-from-triple
  "Extract the object from a triple"
  ([[s p o]]
     o))

(defn o
  "Shortcut for object-from-triple"
  ([t] (object-from-triple t)))

(defn find-resources
  "Retrieves the resources (collections triples with the same subject) inside a model or triple set"
  ([model-or-triples]
     (let [triples (model-to-triples model-or-triples)]
       (loop [acum {}
              max (count triples)
              idx 0]
         (if (< idx max)
           (let [t (nth triples idx)
                 key (.toString (subject-from-triple t))
                 acump (if (get acum key) (assoc acum key (conj (get acum key) t)) (assoc acum key [t]))]
             (recur acump max (+ idx 1)))
           (vals acum))))))

(defn find-resource-uris
  "Retrieves the resource uris (collections triples with the same subject) inside a model or triple set"
  ([model-or-triples]
     (let [triples (model-to-triples model-or-triples)]
       (loop [acum []
              max (count triples)
              idx 0]
         (if (< idx max)
           (let [t (nth triples idx)
                 key (.toString (subject-from-triple t))
                 acump (if (empty? (filter #(= key %1) acum)) (conj acum key) acum)]
             (recur acump max (+ idx 1)))
           acum)))))



(defn optional
  "Marks the provided list of triples as optional triples in a query"
  ([& triples]
     (with-meta
       (vec (map #(with-meta %1 {:optional true}) triples))
       {:optional true})))

(defmacro opt
  "A shortcut for optional"
  ([& args]
     `(optional ~@args)))


;; Triples <-> Pattern transformations

(defn triples-abstraction
  "Transforms a set of triples into a pattern replacing some components with variables"
  ([triples matcher-fn abstraction-map]
     (vec
      (map (fn [t]
             (if (matcher-fn t t)
               (let [t-s (if (:subject abstraction-map)
                           [(triple-subject (:subject abstraction-map)) (nth t 1) (nth t 2)]
                           t)
                     t-p (if (:predicate abstraction-map)
                           [(nth t-s 0) (triple-predicate (:predicate abstraction-map)) (nth t-s 2)]
                           t-s)
                     t-o (if (:object abstraction-map)
                           [(nth t-p 0) (nth t-p 1) (triple-object (:object abstraction-map))]
                           t-p)]
                 t-o)
               t))
           triples))))
