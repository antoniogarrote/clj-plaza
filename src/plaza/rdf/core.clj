;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 30.04.2010


(ns plaza.rdf.core
  (:use (plaza utils)))

;; Axiomatic vocabulary

(def rdfs "http://www.w3.org/2000/01/rdf-schema#")
(def rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#")

(def rdf:Property "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")
(def rdf:type "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

(def rdfs:subPropertyOf "http://www.w3.org/2000/01/rdf-schema#subPropertyOf")
(def rdfs:domain "http://www.w3.org/2000/01/rdf-schema#domain")
(def rdfs:range "http://www.w3.org/2000/01/rdf-schema#range")
(def rdfs:Class "http://www.w3.org/2000/01/rdf-schema#Class")

;; RDF model components

(defprotocol RDFNode
  "Common queries for objects that can be inserted in a RDF graph"
  (is-blank [resource] "Returns true if this resource is a blank node")
  (is-resource [resource] "Returns true if this resource is not a literal or datatype literal")
  (is-property [resource] "Returns true if this resource is a RDF property")
  (is-literal [resource] "Returns true if this resource is a RDF literal"))

(defprotocol RDFResource
  "Any object that can be inserted in a RDF graph"
  (resource-id [resource] "Returns some ID identifying the resource: URI, blank node ID..")
  (qname-prefix [resource] "Returns the prefix part of the qname of this resource, returns '_' if it is a blank node")
  (qname-local [resource] "Returns the local part of the qname of this resource, returns '_' if it is a blank node")
  (literal-value [resource] "Returns the value of this literal")
  (literal-language [resource] "Returns the language of this literal")
  (literal-datatype-uri [resource] "Returns the datatype-uri of this literal datatype")
  (literal-datatype-obj [resource] "Returns an objectual representation of this datatype")
  (literal-lexical-form [resource] "Returns the lexical form of this literal"))

;; RDF model

(defprotocol RDFModel
  "Operations for the manipulation of RDF"
  (create-resource [model ns local] [model uri] "Creates a new RDF resource statement")
  (create-property [model ns local] [model uri] "Creates a new RDF property statement")
  (create-blank-node [model] [model id] "Creates a new RDF blank node")
  (create-literal [model lit] [model lit lang] "Creates a new RDF literal")
  (create-typed-literal [model lit] [model lit type] "Creates a new RDF typed literal")
  (critical-write [model f] "Applies a function within a write lock")
  (critical-read [model f] "Applies a function within a read lock")
  (add-triples [model triples] "Add some triples to the model")
  (remove-triples [model triples] "Remove triples from the model")
  (walk-triples [model f] "iterates through the triples of the model")
  (load-stream [model stream format] "Load triples from a stream")
  (output-string  [model format] "Outputs the triples to a string")
  (query [model query] "Queries this model using the provided query and returns a map of bindings")
  (query-triples [model query] "Queries this model using the provided query and returns a list of graphs"))


;; Utility protocols

(defprotocol RDFPrintable
  (to-string [resource] "Canonical String representation of this RDF object"))

(defprotocol RDFDatatypeMapper
  "Maps a keyword value or a URI to the right datatype object"
  (find-datatype [literal lit] "Finds the type for the literal representation"))

(defprotocol JavaObjectWrapper
  "Allows to retrieve the wrapped object"
  (to-java [wrapper] "Returns the object wrapper by this type object"))


;;; Root bindings


;; The root *rdf-model* that will be used by default
(def *rdf-model* nil)

;; The root *namespace* that will be used by default
(def *rdf-ns* "http://plaza.org/ontologies/")

;; The root *rdf-model-builder-fn* that will be used by default to build models
(def *rdf-model-builder-fn* nil)

;;; Declaration of a model

(defmulti build-model
  (fn [& options] [(first options)]))

(defmethod build-model [nil]
  ([& options] (if (nil? *rdf-model-builder-fn*)
                 (throw (Exception. "Some framework must be loaded"))
                 (build-model *rdf-model-builder-fn*))))

(defmethod build-model :default
  ([& options] (if (nil? *rdf-model-builder-fn*)
                 (throw (Exception. "Some framework must be loaded"))
                 (build-model *rdf-model-builder-fn*))))



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

(defn alter-root-model-builder-fn
  "Alters the root binding for the default model builder function.
   This function should only be used when setting up the application
   with-rdf-ns macro should be used by default"
  ([builder-fn]
     (alter-var-root #'*rdf-model-builder-fn* (fn [_] builder-fn))))

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
       (if (.startsWith expanded-ns "http")
         (str expanded-ns (keyword-to-string local))
         (throw (Exception. (str "Unknown RDF namespace " expanded-ns " with local part " local)))))))

;;; Manipulation of models

(defn reset-model
  "Resets the root model with a fresh model object"
  ([] (alter-root-model (build-model))))

(defn rdf-property
  "Creates a new rdf property"
  ([ns local] (create-property *rdf-model* (expand-ns ns local)))
  ([uri] (create-property *rdf-model* uri)))

(defn rdf-resource
  "Creates a new rdf resource"
  ([ns local] (create-resource *rdf-model* (expand-ns ns local)))
  ([uri] (create-resource *rdf-model* uri)))

(defn is-blank-node
  "Checks if a RDF resource"
  ([resource]
     (if (or (string? resource)
             (keyword? resource))
       false
       (is-blank resource))))

(defn blank-node
  ([]
     (create-blank-node *rdf-model*))
  ([id]
     (let [anon-id (keyword-to-string id)]
       (create-blank-node *rdf-model* anon-id))))

(defn b
  ([] (blank-node))
  ([id] (blank-node id)))


(defn blank-node-id
  ([b]
     (keyword (str (resource-id b)))))

(defn rdf-literal
  "Creates a new rdf literal"
  ([lit] (create-literal *rdf-model* lit))
  ([lit lang] (create-literal *rdf-model* lit lang)))

(defn l
  "shorthand for rdf-literal"
  ([lit] (rdf-literal lit))
  ([lit lang] (rdf-literal lit lang)))

(defn rdf-typed-literal
  "Creates a new rdf literal with an associated type"
  ([lit] (create-typed-literal *rdf-model* lit))
  ([lit type] (create-typed-literal *rdf-model* lit type)))

(defn d
  "shorthand for rdf-typed-literal"
  ([val] (rdf-typed-literal val))
  ([val dt] (rdf-typed-literal val dt)))

(defn rdf-date
  "Creates a new RDF date"
  ([]
     (rdf-date (java.util.Date.)))
  ([d]
     (if (instance? java.util.Date d)
       (rdf-date (+ 1900 (.getYear d)) (.getMonth d) (.getDate d))
       (if (instance? java.util.Calendar d)
         (rdf-typed-literal d)
         (throw (Exception. (str "Don't know how to build a date from " d))))))
  ([y m d]
     (let [c (java.util.Calendar/getInstance)]
       (do (.set c y m d)
           (rdf-typed-literal c)))))

(defn date
  "Shortcut for rdf-date"
  ([& args]
     (apply rdf-date args)))

(defn triple-subject
  "Defines a subject for a statement"
  ([subject]
     (if (and (instance? plaza.rdf.core.RDFResource subject)
              (is-resource subject))
       subject
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
     (if (and (instance? plaza.rdf.core.RDFResource predicate)
              (is-resource predicate))
       predicate
       (if (instance? plaza.rdf.core.RDFResource predicate)
         (rdf-property predicate)
         (if (coll? predicate)
           (let [[rdf-ns local] predicate]
             (rdf-property rdf-ns local))
           (if (is-blank-node predicate)
             (throw (Exception. "Blank node cannot be predicate in a model"))
             (if (.startsWith (keyword-to-string predicate) "?")
               (keyword predicate)
               (rdf-property predicate))))))))

(defn triple-object
  "Defines the object of a statement"
  ([literal]
     (if (and (instance? plaza.rdf.core.RDFResource literal)
              (is-literal literal))
       literal
       (triple-subject literal))))

(defn rdf-triple
  "Parses a RDF triple"
  ([to-parse]
     (if (and (coll? (first to-parse))
              (= (count (first to-parse)) 3))
       (map #(rdf-triple %1) to-parse)
       (if (= (count to-parse) 3)
         (let [[s p o] to-parse]
           [(triple-subject s)
            (triple-predicate p)
            (triple-object o)])
         (let [[s ts] to-parse
               c      (fold-list ts)]
           (vec (map (fn [po] (rdf-triple [s (nth po 0) (nth po 1)]))  c)))))))

(defn- rdf-clone-resource
  ([res]
     (rdf-resource (resource-id res))))

(defn- rdf-clone-property
  ([prop]
     (if (instance? plaza.rdf.core.RDFPrintable prop)
       (rdf-property (to-string prop))
       (rdf-property (str prop)))))

(defn- rdf-clone-literal
  ([lit] (if (= (literal-language lit) "")
           (rdf-typed-literal (literal-value lit) (literal-datatype-obj lit))
           (rdf-literal (literal-value lit) (literal-language lit)))))

(defn- rdf-clone-property
  ([res]
     (rdf-resource (resource-id res))))

(defn- rdf-clone-blank-node
  ([blank]
     (blank-node (blank-node-id blank))))

(defn rdf-clone
  "Clones a triple component"
  ([rdf-obj]
     (cond
      (keyword? rdf-obj) rdf-obj        ;variable
      (is-blank-node rdf-obj) (rdf-clone-blank-node rdf-obj)
      (is-literal rdf-obj) (rdf-clone-literal rdf-obj)   ;literal
      (is-property rdf-obj) (rdf-clone-property rdf-obj) ;property
      (is-resource rdf-obj) (rdf-clone-resource rdf-obj) ;resource
      )))

(defn resource-uri
  "Extracts the URI of a resource"
  ([resource] (resource-id resource)))

(defn resource-qname-prefix
  "Extracts the prefix of a QNAME resource URI"
  ([resource] (qname-prefix resource)))

(defn resource-qname-local
  "Extracts the local part of a QNAME resource URI"
  ([resource] (qname-local resource)))


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


(defn- check-triples
  "Ensures a set of triples is built"
  ([ts]
     (if (:triples (meta ts)) ts (make-triples ts))))

(defmacro model-critical-write [m & body]
  `(critical-write ~m (fn [] ~@body)))

(defmacro model-critical-read [m & body]
  `(critical-read ~m (fn [] ~@body)))

(defn model-add-triples
  "Adds a collection of triples to a model"
  ([ts]
     (let [mts (check-triples ts)]
       (add-triples *rdf-model* mts))))

(defn model-remove-triples
  "Removes a collection of triples to a model"
  ([ts]
     (let [mts (check-triples ts)]
       (remove-triples *rdf-model* mts))))

(defn is-model
  "Checks if an object is a model"
  ([obj]
     (instance? plaza.rdf.core.RDFModel obj)))

;;;@todo
(defn model-to-triples
  "Extracts the triples stored into a model"
  ([model]
     (with-meta (walk-triples model (fn [s p o] [s p o])) {:triples true})))

;; Models IO

(defn document-to-model
  "Adds a set of triples read from a serialized document into a model"
  ([stream format]
     (load-stream *rdf-model* stream format)))


(defn model-to-format
  "Writes a model using the chosen format"
  ([]
     (output-string *rdf-model* :ntriple))
  ([format]
     (output-string *rdf-model* format))
  ([model format]
     (output-string model format)))


(defn triples-to-format
  "Writes a set of triple using the "
  ([triples & args]
     (with-model (build-model)
       (model-add-triples triples)
       (apply model-to-format args))))

(defn triples-to-string
  "Shows a set of triples using string representations for the triples components"
  ([triples]
     (map (fn [[s p o]] [(to-string s) (to-string p) (to-string o)]) triples)))

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
                 key (to-string (subject-from-triple t))
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
                 key (to-string (subject-from-triple t))
                 acump (if (empty? (filter #(= key %1) acum)) (conj acum key) acum)]
             (recur acump max (+ idx 1)))
           acum)))))


(defn parse-format
  ([format]
     (cond (= (.toLowerCase (keyword-to-string format)) "xml") "RDF/XML"
           (= (.toLowerCase (keyword-to-string format)) "ntriple") "N-TRIPLE"
           (= (.toLowerCase (keyword-to-string format)) "n3") "N3"
           (= (.toLowerCase (keyword-to-string format)) "ttl") "TURTLE"
           (= (.toLowerCase (keyword-to-string format)) "turtle") "TTL"
           (= (.toLowerCase (keyword-to-string format)) "xhtml") "XHTML"
           (= (.toLowerCase (keyword-to-string format)) "html") "HTML"
           (= (.toLowerCase (keyword-to-string format)) "trig") "TRIG"
           (= (.toLowerCase (keyword-to-string format)) "trix") "TRIX"
           true "RDF/XML")))


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

