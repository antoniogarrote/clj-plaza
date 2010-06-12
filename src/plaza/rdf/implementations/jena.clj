;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 09.05.2010

(ns plaza.rdf.implementations.jena
  (:use [plaza.utils]
        [plaza.rdf core sparql]
        [plaza.rdf.implementations.common])
  (:import (com.hp.hpl.jena.rdf.model ModelFactory)
           (com.hp.hpl.jena.reasoner.rulesys RDFSRuleReasonerFactory)
           (com.hp.hpl.jena.vocabulary ReasonerVocabulary)
           (com.hp.hpl.jena.datatypes.xsd XSDDatatype)
           (com.hp.hpl.jena.sparql.core Var)
           (com.hp.hpl.jena.datatypes.xsd.impl XMLLiteralType)
           (com.hp.hpl.jena.shared Lock)
           (com.hp.hpl.jena.query QueryFactory QueryExecutionFactory DatasetFactory)
           (com.hp.hpl.jena.sparql.syntax Element ElementGroup ElementOptional ElementFilter)
           (com.hp.hpl.jena.graph Node Triple)
           (com.hp.hpl.jena.sparql.expr E_Str E_Lang E_Datatype E_Bound E_IsIRI E_IsURI E_IsBlank E_IsLiteral E_GreaterThanOrEqual E_GreaterThan
                                        E_LessThanOrEqual E_LessThan E_NotEquals E_Equals E_Subtract E_Add E_Multiply E_Divide)))

;; Loading RDFa java

(Class/forName "net.rootdev.javardfa.RDFaReader")

;; declaration of symbols

(declare parse-jena-object)

;; SPARQL

;; bulding of queries and filters

(defn- process-model-query-result
  "Transforms a query result into a dicitionary of bindings"
  ([model result]
     (let [vars (iterator-seq (.varNames result))]
       (reduce (fn [acum item] (assoc acum (keyword (str "?" item)) (parse-jena-object model (.get result item)))) {} vars))))

(defn- model-query-fn
  "Queries a model and returns a map of bindings"
  ([model query query-string]
;     (let [query (if (string? query-or-string) (sparql-to-query query-or-string) query-or-string)
;           query-string (if (string? query-or-string) query-or-string (str (build-query *sparql-framework* query)))]
;     (println (str "QUERYING JENA WITH: " query-string))
     (model-critical-read model
                          (let [qexec (QueryExecutionFactory/create query-string (to-java model))
                                        ;     (let [qexec (QueryExecutionFactory/create (build-query query)  @model)
                                results (iterator-seq (cond (= (:kind query) :select) (.execSelect qexec)))]
                            (map #(process-model-query-result model %1) results)))))

(defn- model-query-triples-fn
  "Queries a model and returns a list of triple sets with results binding variables in que query pattern"
  ([model query-or-string]
     (let [query (if (string? query-or-string) (sparql-to-query query-or-string) query-or-string)
           query-string (if (string? query-or-string) query-or-string (str (build-query *sparql-framework* query-or-string)))
           results (model-query-fn model query query-string)]
       (map #(pattern-bind (:pattern query) %1) results))))


;; JENA implementation

(deftype JenaResource [res] RDFResource RDFNode JavaObjectWrapper RDFPrintable
  (to-java [resource] res)
  (to-string [resource] (.getURI res))
  (is-blank [resource] false)
  (is-resource [resource] true)
  (is-property [resource] false)
  (is-literal [resource] false)
  (resource-id [resource] (.getURI res))
  (qname-prefix [resource] (.getNameSpace res))
  (qname-local [resource] (.getLocalName res))
  (literal-value [resource] (throw (Exception. "Cannot retrieve literal value for a resource")))
  (literal-language [resource] (throw (Exception. "Cannot retrieve lang for a resource")))
  (literal-datatype-uri [resource] (throw (Exception. "Cannot retrieve datatype-uri for a resource")))
  (literal-datatype-obj [resource] (throw (Exception. "Cannot retrieve datatype-uri for a resource")))
  (literal-lexical-form [resource] (resource-id resource))
  (toString [resource] (.getURI res))
  (hashCode [resource] (.hashCode (resource-id resource)))
  (equals [resource other-resource] (= (resource-id resource) (resource-id other-resource))))


(deftype JenaBlank [res] RDFResource RDFNode JavaObjectWrapper RDFPrintable
  (to-java [resource] res)
  (to-string [resource] (str "_:" (resource-id resource)))
  (is-blank [resource] true)
  (is-resource [resource] false)
  (is-property [resource] false)
  (is-literal [resource] false)
  (resource-id [resource] (str (.getId res)))
  (qname-prefix [resource] "_")
  (qname-local [resource] (str (resource-id resource)))
  (literal-value [resource] (throw (Exception. "Cannot retrieve literal value for a blank node")))
  (literal-language [resource] (throw (Exception. "Cannot retrieve lang for a blank node")))
  (literal-datatype-uri [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-datatype-obj [resource] (throw (Exception. "Cannot retrieve datatype-uri for a resource")))
  (literal-lexical-form [resource] (str "_:" (resource-id resource)))
  (toString [resource] (to-string resource))
  (hashCode [resource] (.hashCode (resource-id resource)))
  (equals [resource other-resource] (= (resource-id resource) (resource-id other-resource))))


(deftype JenaLiteral [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper RDFPrintable
  (to-java [resource] res)
  (to-string [resource] (let [lang (literal-language resource)]
                          (if (= "" lang)
                            (literal-lexical-form resource)
                            (str  (literal-lexical-form resource) "@" lang))))
  (is-blank [resource] false)
  (is-resource [resource] false)
  (is-property [resource] false)
  (is-literal [resource] true)
  (resource-id [resource] (to-string resource))
  (qname-prefix [resource] (throw (Exception. "Cannot retrieve qname-prefix value for a literal")))
  (qname-local [resource] (throw (Exception. "Cannot retrieve qname-local value for a literal")))
  (literal-value [resource] (.getValue res))
  (literal-language [resource] (.getLanguage res))
  (literal-datatype-uri [resource] "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral")
  (literal-datatype-obj [resource] (find-jena-datatype :xmlliteral))
  (literal-lexical-form [resource] (.getLexicalForm res))
  (find-datatype [resource literal] (find-jena-datatype literal))
  (toString [resource] (to-string resource))
  (hashCode [resource] (.hashCode (resource-id resource)))
  (equals [resource other-resource] (= (resource-id resource) (resource-id other-resource))))

(deftype JenaTypedLiteral [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper RDFPrintable
  (to-java [resource] res)
  (to-string [resource]  (str  "\""(literal-lexical-form resource) "\"^^<" (literal-datatype-uri resource) ">"))
  (is-blank [resource] false)
  (is-resource [resource] false)
  (is-property [resource] false)
  (is-literal [resource] true)
  (resource-id [resource] (to-string resource))
  (qname-prefix [resource] (throw (Exception. "Cannot retrieve qname-prefix value for a literal")))
  (qname-local [resource] (throw (Exception. "Cannot retrieve qname-local value for a literal")))
  (literal-value [resource] (.getValue res))
  (literal-language [resource] "")
  (literal-datatype-uri [resource] (str (.getDatatypeURI res)))
  (literal-datatype-obj [resource] (find-jena-datatype (.getDatatypeURI res)))
  (literal-lexical-form [resource] (.getLexicalForm res))
  (find-datatype [resource literal] (find-jena-datatype literal))
  (toString [resource] (to-string resource))
  (hashCode [resource] (.hashCode (resource-id resource)))
  (equals [resource other-resource] (= (resource-id resource) (resource-id other-resource))))

(deftype JenaProperty [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper RDFPrintable
  (to-java [resource] res)
  (to-string [resource]  (str res))
  (is-blank [resource] false)
  (is-resource [resource] true)
  (is-property [resource] true)
  (is-literal [resource] false)
  (resource-id [resource] (to-string resource))
  (qname-prefix [resource] (.getNameSpace res))
  (qname-local [resource] (.getLocalName res))
  (literal-value [resource] (throw (Exception. "Cannot retrieve literal value for a blank node")))
  (literal-language [resource] (throw (Exception. "Cannot retrieve lang for a blank node")))
  (literal-datatype-uri [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-datatype-obj [resource] (throw (Exception. "Cannot retrieve datatype-uri for a blank node")))
  (literal-lexical-form [resource] (to-string res))
  (toString [resource] (str res))
  (hashCode [resource] (.hashCode (resource-id resource)))
  (equals [resource other-resource] (= (resource-id resource) (resource-id other-resource))))


(deftype JenaModel [mod] RDFModel RDFDatatypeMapper JavaObjectWrapper RDFPrintable
  (to-java [model] mod)
  (create-resource [model ns local] (.createResource mod (expand-ns ns local)))
  (create-resource [model uri]
                   (if (instance? plaza.rdf.core.RDFResource uri)
                     uri
                     (if (.startsWith (keyword-to-string uri) "http://")
                       (plaza.rdf.implementations.jena.JenaResource.
                        (.createResource mod (keyword-to-string uri)))
                       (plaza.rdf.implementations.jena.JenaResource.
                        (.createResource mod (str *rdf-ns* (keyword-to-string uri)))))))
  (create-property [model ns local] (.createProperty mod (expand-ns ns local)))
  (create-property [model uri]
                   (if (instance? plaza.rdf.core.RDFResource uri)
                     (if (is-property uri)
                       uri
                       (plaza.rdf.implementations.jena.JenaProperty.
                        (.createProperty mod (str uri))))
                     (if (.startsWith (keyword-to-string uri) "http://")
                       (plaza.rdf.implementations.jena.JenaProperty.
                        (.createProperty mod (keyword-to-string uri)))
                       (plaza.rdf.implementations.jena.JenaProperty.
                        (.createProperty mod *rdf-ns* (keyword-to-string uri))))))
  (create-blank-node [model] (plaza.rdf.implementations.jena.JenaBlank.
                              (.createResource mod (com.hp.hpl.jena.rdf.model.AnonId.))))
  (create-blank-node [model id]
                     (let [anon-id (keyword-to-string id)]
                       (plaza.rdf.implementations.jena.JenaBlank.
                        (.createResource mod (com.hp.hpl.jena.rdf.model.AnonId. anon-id)))))
  (create-literal [model lit] (plaza.rdf.implementations.jena.JenaLiteral.
                               (.createLiteral mod lit false)))
  (create-literal [model lit lang]
                  (plaza.rdf.implementations.jena.JenaLiteral.
                   (.createLiteral mod lit lang)))
  (create-typed-literal [model lit] (plaza.rdf.implementations.jena.JenaTypedLiteral.
                                     (.createTypedLiteral mod lit)))
  (create-typed-literal [model lit type]
                        (let [dt (find-datatype model type)]
                          (if (instance? java.util.GregorianCalendar lit)
                            (plaza.rdf.implementations.jena.JenaTypedLiteral.
                             (.createTypedLiteral mod lit))
                            (plaza.rdf.implementations.jena.JenaTypedLiteral.
                             (.createTypedLiteral mod lit dt)))))
  (critical-write [model f]
                  (do
                    (.enterCriticalSection mod Lock/WRITE)
                    (let [res (f)]
                      (.leaveCriticalSection mod)
                      res)))
  (critical-read [model f]
                 (do
                   (.enterCriticalSection mod Lock/READ)
                   (let [res (f)]
                     (.leaveCriticalSection mod)
                     res)))
  (add-triples [model triples]
               (critical-write model
                               (fn []
                                 (loop [acum triples]
                                   (when (not (empty? acum))
                                     (let [[ms mp mo] (first acum)]
                                       (.add mod (to-java ms) (to-java (create-property model mp)) (to-java mo))
                                       (recur (rest acum))))))))
  (remove-triples [model triples]
                  (critical-write model
                                  (fn []
                                    (loop [acum triples]
                                      (when (not (empty? acum))
                                        (let [[ms mp mo] (first acum)]
                                          (.remove mod (first (iterator-seq (.listStatements mod (to-java ms) (to-java (create-property model mp)) (to-java mo)))))
                                          (recur (rest acum))))))))
  (walk-triples [model f]
                (critical-read model
                               (fn []
                                 (let [stmts (iterator-seq (.listStatements mod))]
                                   (map (fn [st]
                                          (let [s (let [subj (.getSubject st)]
                                                    (if (instance? com.hp.hpl.jena.rdf.model.impl.ResourceImpl subj)
                                                      (if (.isAnon subj)
                                                        (create-blank-node model (str (.getId subj)))
                                                        (create-resource model (str subj)))
                                                      (create-resource model (str subj))))
                                                p (create-property model (str (.getPredicate st)))
                                                o (let [obj (.getObject st)]
                                                    (if (instance? com.hp.hpl.jena.rdf.model.Literal obj)
                                                      (if (or (nil? (.getDatatypeURI obj))
                                                              (= (.getDatatypeURI obj) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
                                                        (create-literal model (.getValue obj) (.getLanguage obj))
                                                        (create-typed-literal model (.getValue obj) (.getDatatypeURI obj)))
                                                      (if (instance? com.hp.hpl.jena.rdf.model.impl.ResourceImpl obj)
                                                        (if (.isAnon obj)
                                                          (create-blank-node model (str (.getId obj)))
                                                          (create-resource model (str obj)))
                                                        (create-resource model (str obj)))))]
                                            (f s p o)))
                                        stmts)))))
  (to-string [model] (walk-triples model (fn [s p o] [(to-string s) (to-string p) (to-string o)])))
  (load-stream [model stream format]
               (let [format (parse-format format)]
                 (critical-write model
                                 (fn []
                                   (if (string? stream)
                                     (.read mod stream format)
                                     (.read mod stream *rdf-ns* format))))
                 model))
  (output-string  [model writer format]
                  (critical-read model (fn [] (.write mod writer (parse-format format)))))
  (output-string  [model format]
                  (output-string model *out* format))
  (find-datatype [model literal] (find-jena-datatype literal))
  (query [model query] (model-query-fn model query (str (build-query *sparql-framework* query))))
  (query-triples [model query] (model-query-triples-fn model query)))

(deftype JenaSparqlFramework [] SparqlFramework
  (parse-sparql-to-query [framework sparql] (parse-sparql-to-query-fn sparql))
  (parse-sparql-to-pattern [framework sparql] (parse-sparql-to-pattern-fn sparql))
  (build-filter [framework filter] (build-filter-fn framework filter))
  (build-query [framework query] (build-query-fn framework query))
  (is-var-expr [framework expr] (is-var-expr-fn expr))
  (var-to-keyword [framework var-expr]
                  (let [s (.getVarName var-expr)]
                    (if (.startsWith s "?")
                      (keyword s)
                      (keyword (str "?" s))))))


(defn parse-jena-object
  "Parses any Jena relevant object into its plaza equivalent type"
  ([model jena]
     (if (instance? com.hp.hpl.jena.rdf.model.impl.ResourceImpl jena)
       (if (.isAnon jena)
         (create-blank-node model (str (.getId jena)))
         (create-resource model (str jena)))
       (if (instance? com.hp.hpl.jena.rdf.model.impl.PropertyImpl jena)
         (create-property model (str jena))
         (if (instance? com.hp.hpl.jena.rdf.model.Literal jena)
           (if (or (nil? (.getDatatypeURI jena))
                   (= (.getDatatypeURI jena) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
             (create-literal model (.getValue jena) (.getLanguage jena))
             (create-typed-literal model (.getValue jena) (.getDatatypeURI jena)))
           (throw (Exception. (str "Unable to parse object " jena " of type " (class jena)))))))))

;; Initialization

(defmethod build-model [:jena]
  ([& options] (plaza.rdf.implementations.jena.JenaModel. (ModelFactory/createDefaultModel))))

(defn init-jena-framework
  "Setup all the root bindings to use Plaza with the Jena framework. This function must be called
   before start using Plaza"
  ([] (alter-root-model (build-model :jena))
     (alter-root-sparql-framework (plaza.rdf.implementations.jena.JenaSparqlFramework.))
     (alter-root-model-builder-fn :jena)))
