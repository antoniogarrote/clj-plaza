;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 11.05.2010

(ns plaza.rdf.implementations.sesame
  (:use [plaza.utils]
        [plaza.rdf core sparql])
  (:import (org.openrdf.repository Repository RepositoryException)
           (org.openrdf.repository.sail SailRepository)
           (org.openrdf.rio RDFFormat)
           (org.openrdf.sail.memory MemoryStore)
           (org.openrdf.model.impl URIImpl BNodeImpl LiteralImpl ValueFactoryImpl)
           (org.openrdf.model URI BNode Literal ValueFactory)
           (org.openrdf.sail.inferencer.fc ForwardChainingRDFSInferencer)))

;; Loading RDFa java

                                        ;(Class/forName "net.rootdev.javardfa.RDFaReader")

;; declaration of symbols


;; Shared functions

(defn find-sesame-datatype
  "Finds the right datatype object from the string representation"
  ([literal]
     (let [lit (let [literal-str (keyword-to-string literal)]
                 (if (.startsWith literal-str "http://")
                   (aget (.split literal-str "#") 1)
                   literal))]
       (cond
        (= "xmlliteral" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"
        (= "anyuri" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#anyURI"
        (= "boolean" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#boolean"
        (= "byte" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#byte"
        (= "date" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#date"
        (= "datetime" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#dateTime"
        (= "decimal" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#decimal"
        (= "double" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#double"
        (= "float" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#float"
        (= "int" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#int"
        (= "integer" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#integer"
        (= "long" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#long"
        (= "string" (.toLowerCase (keyword-to-string lit))) "http://www.w3.org/2001/XMLSchema#string"))))

(defn sesame-typed-literal-tojava
  "Transforms a sesame typed literal into the equivalente Java object"
  ([lit]
     (cond
      (= "http://www.w3.org/2001/XMLSchema#boolean" (str (.getDatatype lit))) (.booleanValue lit)
      (= "http://www.w3.org/2001/XMLSchema#byte" (str (.getDatatype lit))) (.byteValue lit)
      (= "http://www.w3.org/2001/XMLSchema#date" (str (.getDatatype lit))) (.calendarValue lit)
      (= "http://www.w3.org/2001/XMLSchema#dateTime" (str (.getDatatype lit))) (.calendarValue lit)
      (= "http://www.w3.org/2001/XMLSchema#decimal" (str (.getDatatype lit))) (.decimalValue lit)

      (= "http://www.w3.org/2001/XMLSchema#double" (str (.getDatatype lit))) (.doubleValue lit)
      (= "http://www.w3.org/2001/XMLSchema#float" (str (.getDatatype lit))) (.floatValue lit)
      (= "http://www.w3.org/2001/XMLSchema#int" (str (.getDatatype lit))) (.intValue lit)
      (= "http://www.w3.org/2001/XMLSchema#integer" (str (.getDatatype lit))) (.integerValue lit)
      (= "http://www.w3.org/2001/XMLSchema#long" (str (.getDatatype lit))) (.longValue lit)
      (= "http://www.w3.org/2001/XMLSchema#string" (str (.getDatatype lit))) (.stringValue lit)
      true (.stringValue lit))))

(defn- translate-plaza-format
  "Translates an string representation of a RDF format to the sesame object"
  ([format]
     (cond (= format "RDF/XML")  RDFFormat/RDFXML
           (= format "N-TRIPLE")  RDFFormat/NTRIPLES
           (= format "N3")  RDFFormat/N3
           (= format "TURTLE")  RDFFormat/TURTLE
           (= format "TTL")  RDFFormat/TURTLE
           (= format "XHTML")  :todo
           (= format "HTML")  :todo
           (= format "TRIG")  RDFFormat/TRIG
           (= format "TRIX")  RDFFormat/TRIX
           true      RDFFormat/RDFXML)))

;; SPARQL


;; Sesame implementation

(deftype SesameResource [res] RDFResource RDFNode JavaObjectWrapper
  (to-java [resource] res)
  (to-string [resource]  (str res))
  (is-blank [resource] false)
  (is-resource [resource] true)
  (is-property [resource] false)
  (is-literal [resource] false)
  (resource-id [resource] (to-string resource))
  (qname-prefix [resource] (.getNamespace res))
  (qname-local [resource] (.getLocalName res))
  (literal-value [resource] (throw (Exception. "Cannot retrieve literal value for a blank node")))
  (literal-language [resource] (throw (Exception. "Cannot retrieve lang for a blank node")))
  (literal-datatype-uri [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-datatype-obj [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-lexical-form [resource] (str res)))


(deftype SesameBlank [res] RDFResource RDFNode JavaObjectWrapper
  (to-java [resource] res)
  (to-string [resource] (.stringValue res))
  (is-blank [resource] true)
  (is-resource [resource] false)
  (is-property [resource] false)
  (is-literal [resource] false)
  (resource-id [resource] (.getID res))
  (qname-prefix [resource] "_")
  (qname-local [resource] (.getId res))
  (literal-value [resource] (throw (Exception. "Cannot retrieve literal value for a blank node")))
  (literal-language [resource] (throw (Exception. "Cannot retrieve lang for a blank node")))
  (literal-datatype-uri [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-datatype-obj [resource] (throw (Exception. "Cannot retrieve datatype-uri for a resource")))
  (literal-lexical-form [resource] (.getId res)))


(deftype SesameLiteral [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper
  (to-java [resource] res)
  (to-string [resource] (let [lang (literal-language resource)]
                          (if (= "" lang)
                            (literal-lexical-form resource)
                            (str  (literal-lexical-form resource) "@" lang))))
  (is-blank [resource] false)
  (is-resource [resource] false)
  (is-property [resource] false)
  (is-literal [resource] true)
  (resource-id [resource] (if (not (= (.getLanguage res) "")) (str (.getLabel res) "@" (.getLanguage res)) (.getLabel res)))
  (qname-prefix [resource] (throw (Exception. "Cannot retrieve qname-prefix value for a literal")))
  (qname-local [resource] (throw (Exception. "Cannot retrieve qname-local value for a literal")))
  (literal-value [resource] (.stringValue res))
  (literal-language [resource] (let [lang (.getLanguage res)] (if (nil? lang) "" lang)))
  (literal-datatype-uri [resource] "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral")
  (literal-datatype-obj [resource] (find-sesame-datatype :xmlliteral))
  (literal-lexical-form [resource] (resource-id resource))
  (find-datatype [resource literal] (find-sesame-datatype literal)))

(deftype SesameTypedLiteral [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper
  (to-java [resource] res)
  (to-string [resource]  (str res))
  (is-blank [resource] false)
  (is-resource [resource] false)
  (is-property [resource] false)
  (is-literal [resource] true)
  (resource-id [resource] (to-string resource))
  (qname-prefix [resource] (throw (Exception. "Cannot retrieve qname-prefix value for a literal")))
  (qname-local [resource] (throw (Exception. "Cannot retrieve qname-local value for a literal")))
  (literal-value [resource] (sesame-typed-literal-tojava res))
  (literal-language [resource] "")
  (literal-datatype-uri [resource] (str (.getDatatype res)))
  (literal-datatype-obj [resource] (find-sesame-datatype (str (.getDatatype res))))
  (literal-lexical-form [resource] (to-string resource))
  (find-datatype [resource literal] (find-sesame-datatype literal)))

(deftype SesameProperty [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper
  (to-java [resource] res)
  (to-string [resource]  (str res))
  (is-blank [resource] false)
  (is-resource [resource] true)
  (is-property [resource] true)
  (is-literal [resource] false)
  (resource-id [resource] (to-string resource))
  (qname-prefix [resource] (.getNamespace res))
  (qname-local [resource] (.getLocalName res))
  (literal-value [resource] (throw (Exception. "Cannot retrieve literal value for a blank node")))
  (literal-language [resource] (throw (Exception. "Cannot retrieve lang for a blank node")))
  (literal-datatype-uri [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-datatype-obj [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-lexical-form [resource] (str res)))


(deftype SesameModel [mod] RDFModel RDFDatatypeMapper JavaObjectWrapper
  (to-java [model] mod)
  (create-resource [model ns local] (plaza.rdf.implementations.sesame.SesameResource. (.. ValueFactoryImpl getInstance (createURI (expand-ns ns local)))))
  (create-resource [model uri]
                   (if (instance? plaza.rdf.core.RDFResource uri)
                     uri
                     (if (.startsWith (keyword-to-string uri) "http://")
                       (plaza.rdf.implementations.sesame.SesameResource. (.. ValueFactoryImpl getInstance (createURI (keyword-to-string uri))))
                       (plaza.rdf.implementations.sesame.SesameResource. (.. ValueFactoryImpl getInstance (createURI (expand-ns *rdf-ns* (keyword-to-string uri))))))))
  (create-property [model ns local] (plaza.rdf.implementations.sesame.SesameProperty. (.. ValueFactoryImpl getInstance (createURI (expand-ns ns local)))))
  (create-property [model uri]
                   (if (instance? plaza.rdf.implementations.sesame.SesameResource uri)
                     (plaza.rdf.implementations.sesame.SesameProperty. (.. ValueFactoryImpl getInstance (to-string uri)))
                     (if (.startsWith (keyword-to-string uri) "http://")
                       (plaza.rdf.implementations.sesame.SesameProperty. (.. ValueFactoryImpl getInstance (createURI (keyword-to-string uri))))
                       (plaza.rdf.implementations.sesame.SesameProperty. (.. ValueFactoryImpl getInstance (createURI (expand-ns *rdf-ns* (keyword-to-string uri))))))))
  (create-blank-node [model] (plaza.rdf.implementations.sesame.SesameBlank. (.. ValueFactoryImpl getInstance (createBNode (str (.getTime (java.util.Date.)))))))
  (create-blank-node [model id] (plaza.rdf.implementations.sesame.SesameBlank. (.. ValueFactoryImpl getInstance (createBNode (keyword-to-string id)))))
  (create-literal [model lit] (plaza.rdf.implementations.sesame.SesameLiteral. (.. ValueFactoryImpl getInstance (createLiteral lit))))
  (create-literal [model lit lang] (plaza.rdf.implementations.sesame.SesameLiteral. (.. ValueFactoryImpl getInstance (createLiteral lit (keyword-to-string lang)))))
  (create-typed-literal [model lit] (plaza.rdf.implementations.sesame.SesameTypedLiteral. (.. ValueFactoryImpl getInstance (createLiteral lit))))
  (create-typed-literal [model lit type] (plaza.rdf.implementations.sesame.SesameTypedLiteral.
                                          (.. ValueFactoryImpl
                                              getInstance (createLiteral
                                                           lit
                                                           (.. ValueFactoryImpl getInstance (createURI (str (find-sesame-datatype type))))))))
  (critical-write [model f] (let [connection (.getConnection mod)]
                              (try
                               (do
                                 (.setAutoCommit connection false)
                                 (let [res (f)]
                                   (.commit connection)))
                               (catch RepositoryException e (.rollback connection))
                               (finally (.close connection)))))
  (critical-read [model f] (critical-write model f)) ;; is reading thread-safe in Sesame?
  (add-triples [model triples] (let [connection (.getConnection mod)]
                                 (try
                                  (do
                                    (.setAutoCommit connection false)
                                    (loop [acum triples]
                                      (when (not (empty? acum))
                                        (let [[ms mp mo] (first acum)]
                                          (.add connection (to-java ms) (to-java mp) (to-java mo) (into-array org.openrdf.model.Resource []))
                                          (recur (rest acum)))))
                                    (.commit connection))
                                  (catch RepositoryException e (.rollback connection))
                                  (finally (.close connection)))
                                 model))

  (remove-triples [model triples] (let [connection (.getConnection mod)]
                                    (try
                                     (do
                                       (.setAutoCommit connection false)
                                       (loop [acum triples]
                                         (when (not (empty? acum))
                                           (let [[ms mp mo] (first acum)]
                                             (.remove connection (to-java ms) (to-java (create-property model mp)) (to-java mo)
                                                      (into-array org.openrdf.model.Resource [])) ;; varargs!
                                             (recur (rest acum)))))
                                       (.commit connection))
                                     (catch RepositoryException e (.rollback connection))
                                     (finally (.close connection)))
                                    model))
  (walk-triples [model f]
                (let [connection (.getConnection mod)]
                  (try
                   (do
                     (let [stmts (.asList (.getStatements connection nil nil nil true (into-array org.openrdf.model.Resource [])))
                           res (map (fn [st]
                                      (let [s (let [subj (.getSubject st)]
                                                (if (instance? org.openrdf.model.Resource subj)
                                                  (if (instance? org.openrdf.model.BNode subj)
                                                    (create-blank-node model (str (.getID subj)))
                                                    (create-resource model (str subj)))
                                                  (create-resource model (str subj))))
                                            p (create-property model (str (.getPredicate st)))
                                            o (let [obj (.getObject st)]
                                                (if (instance? org.openrdf.model.Literal obj)
                                                  (if (or (nil? (.getDatatype obj))
                                                          (= (.getDatatype obj) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
                                                    (create-literal model (.getLabel obj) (.getLanguage obj))
                                                    (create-typed-literal model (.getLabel obj) (str (.getDatatype obj))))
                                                  (if (instance? org.openrdf.model.Resource obj)
                                                    (if (instance? org.openrdf.model.BNode obj)
                                                      (create-blank-node model (str (.getID obj)))
                                                      (create-resource model (str obj)))
                                                    (create-resource model (str obj)))))]
                                        (f s p o)))
                                    stmts)]
                       (.commit connection)
                       res))
                   (catch RepositoryException e (.rollback connection))
                   (finally (.close connection)))))
  (load-stream [model stream format]

               (let [format (translate-plaza-format (parse-format format))
                     connection (.getConnection mod)]
                 (try
                  (if (string? stream)
                    (.add connection (plaza.utils/grab-document-url stream) stream format)
                    (.add connection stream *rdf-ns* format))
                  (finally (.close connection)))
                 model))
  (output-string  [model format]
                  (do
                    (let [connection (.getConnection mod)
                          writer (org.openrdf.rio.Rio/createWriter (translate-plaza-format (parse-format format)) *out*)]
                      (try
                       (.export writer connection)
                       (finally (.close connection)))))
                  model)
  (find-datatype [model literal] (translate-plaza-format (parse-format format)))
  (query [model query] (let [connection (.getConnection mod)]
                         (try
                          (.prepareTupleQuery connection org.openrdf.query.QueryLanguage/SPARQL query)
                          (finally (.close connection)))))
  (query-triples [model query] :todo))


(deftype SesameSparqlFramework [] SparqlFramework
  (parse-sparql-to-query [framework sparql] :todo)
  (parse-sparql-to-pattern [framework sparql] :todo)
  (build-filter [framework filter] :todo)
  (build-query [framework query] :todo)
  (is-var-expr [framework expr] :todo)
  (var-to-keyword [framework var-expr] :todo))


(defn- parse-sesame-object
  "Parses any Sesame relevant object into its plaza equivalent type"
  ([model sesame]
     (if (instance? :ResourceImpl sesame)
       (if (:isAnon sesame)
         (create-blank-node model (str (.getId sesame)))
         (create-resource model (str sesame)))
       (if (instance? :PropertyImpl sesame)
         (create-property model (str sesame))
         (if (instance? :Literal sesame)
           (if (or (nil? (:getDatatypeURI sesame))
                   (= (:getDatatypeURI sesame) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
             (create-literal model (:getValue sesame) (:getLanguage sesame))
             (create-typed-literal model (:getValue sesame) (:getDatatypeURI sesame)))
           (throw (Exception. (str "Unable to parse object " sesame " of type " (class sesame)))))))))

;; Initialization

(defmethod build-model [:sesame]
  ([& options] (let [repo (SailRepository. (ForwardChainingRDFSInferencer. (MemoryStore.)))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))

(defmethod build-model [nil]
  ([& options] (let [repo (SailRepository. (ForwardChainingRDFSInferencer. (MemoryStore.)))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))

(defmethod build-model :default
  ([& options] (let [repo (SailRepository. (ForwardChainingRDFSInferencer. (MemoryStore.)))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))

(defn init-sesame-framework
  "Setup all the root bindings to use Plaza with the Sesame framework. This function must be called
   before start using Plaza"
  ([] (alter-root-model (build-model :sesame))
     (alter-root-sparql-framework :todo)))
