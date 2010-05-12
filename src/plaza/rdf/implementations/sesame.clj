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
(declare parse-sesame-object)


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

(defn- is-filter-expr
  "Tests if one Jena expression is a filter expression"
  ([expr]
     (or (instance? com.hp.hpl.jena.sparql.expr.ExprFunction expr)
         (instance? com.hp.hpl.jena.sparql.syntax.ElementFilter expr))))

(defn- is-var-expr-fn
  "Tests if one Jena expression is a var expression"
  ([expr]
     (or (and (keyword? expr)
              (.startsWith (keyword-to-string expr) "?"))
         (= (class expr) com.hp.hpl.jena.sparql.expr.ExprVar))))

(defn- parse-pattern-literal
  "Parses a literal value"
  ([lit]
     (if (nil? (.getLiteralDatatypeURI lit))
       (l (.getLiteralLexicalForm lit) (.getLiteralLanguage lit))
       (d (.getLiteralValue lit) (.getLiteralDatatypeURI lit)))))

(defn- parse-pattern-atom
  "Parses a single component of a pattern: variable, literal, URI, etc"
  ([atom pos]
     (cond
      (instance? com.hp.hpl.jena.sparql.core.Var atom) (keyword (str "?" (.getVarName atom)))
      (instance? com.hp.hpl.jena.graph.Node_URI atom) (cond
                                                       (= pos :subject) (rdf-resource (.getURI atom))
                                                       (= pos :predicate) (rdf-property (.getURI atom))
                                                       (= pos :object) (rdf-resource (.getURI atom)))
      (instance? com.hp.hpl.jena.graph.Node_Literal atom) (parse-pattern-literal atom)
      true atom)))


(defn- parse-literal-lexical
  ([lit]
     (let [parts-a (.split lit "\\^\\^")
           val-a (aget parts-a 0)
           datatype (if (= (alength parts-a) 2) (aget (.split (aget (.split (aget parts-a 1) "<") 1) ">") 0) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral")
           parts-b (.split val-a "@")
           val (let [val-tmp (aget parts-b 0)]
                 (if (and (.startsWith val-tmp "\"")
                          (.endsWith val-tmp "\""))
                   (aget (.split (aget (.split val-tmp "\"") 1) "\"") 0)
                   val-tmp))
           lang-tag (if (= (alength parts-b) 2) (aget parts-b 1) nil)]
       (if (nil? lang-tag)
         (rdf-typed-literal val datatype)
         (rdf-literal val lang-tag)))))

(declare parse-filter-expr)
(defn- parse-next-filter-expr
  ([expr]
     (cond
      (is-var-expr-fn expr) (keyword (.toString expr))
      (is-filter-expr expr) (parse-filter-expr expr)
      true (parse-literal-lexical (.toString expr)))))

(defn- parse-filter-expr-2
  ([expr symbol]
     {:expression (keyword symbol)
      :kind :two-parts
      :args [(parse-next-filter-expr (.. expr (getArg 1)))
             (parse-next-filter-expr (.. expr (getArg 2)))]}))

(defn- parse-filter-expr-1
  ([expr symbol]
     {:expression (keyword symbol)
      :kind :one-part
      :args [(parse-next-filter-expr (.. expr (getArg 1)))]}))


(defn- parse-filter-expr
  "Parses a filter expression"
  ([expr]
     (cond
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Str) (parse-filter-expr-1 "str")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Lang) (parse-filter-expr-1 expr "lang")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Datatype) (parse-filter-expr-1 expr "datatype")

      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Bound) (parse-filter-expr-1 expr "bound")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_IsIRI) (parse-filter-expr-1 expr "isIRI")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_IsURI) (parse-filter-expr-1 expr "isURI")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_IsBlank) (parse-filter-expr-1 expr "isBlank")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_IsLiteral) (parse-filter-expr-1 expr "isLiteral")

      (= (class expr) com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual) (parse-filter-expr-2 expr ">=")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_GreaterThan) (parse-filter-expr-2 expr ">")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual) (parse-filter-expr-2 expr "<=")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_LessThan) (parse-filter-expr-2 expr "<")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_NotEquals) (parse-filter-expr-2 expr "!=")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Equals) (parse-filter-expr-2 expr "=")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Subtract) (parse-filter-expr-2 expr "-")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Add) (parse-filter-expr-2 expr "+")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Multiply) (parse-filter-expr-2 expr "*")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Divide) (parse-filter-expr-2 expr "div"))))

(defn- sparql-to-pattern-filters
  "Parses a SPARQL query and transform it into a pattern and some filters"
  ([sparql-string-or-query]
     (let [query (if (string? sparql-string-or-query)
                   (com.hp.hpl.jena.query.QueryFactory/create sparql-string-or-query)
                   sparql-string-or-query)
           query-pattern-els (.. query getQueryPattern getElements)]
       (flatten-1
        (map (fn [elem]
               (let [pattern-els (if (instance? com.hp.hpl.jena.sparql.syntax.ElementOptional elem)
                                   (.. elem getOptionalElement getElements (get 0) patternElts)
                                   (if (instance? com.hp.hpl.jena.sparql.syntax.ElementFilter elem)
                                     (.iterator [elem])
                                     (.patternElts elem)))
                     is-optional (if (instance? com.hp.hpl.jena.sparql.syntax.ElementOptional elem)
                                   true
                                   false)]
                 (loop [should-continue (.hasNext pattern-els)
                        acum []]
                   (if should-continue
                     (let [next-elt (.next pattern-els)]
                       (if (instance? com.hp.hpl.jena.sparql.syntax.ElementFilter next-elt)
                         ;; This is a filter
                         (recur (.hasNext pattern-els)
                                (conj acum (with-meta (parse-filter-expr (.getExpr next-elt))
                                             {:filter true
                                              :optional is-optional})))
                         ;; A regular (maybe optional) expression
                         (recur (.hasNext pattern-els)
                                (conj acum (with-meta [(parse-pattern-atom (.getSubject next-elt) :subject)
                                                       (parse-pattern-atom (.getPredicate next-elt) :predicate)
                                                       (parse-pattern-atom (.getObject next-elt) :object)]
                                             {:optional is-optional
                                              :filter false})))))
                     acum))))
             query-pattern-els)))))

(defn- parse-sparql-to-pattern-fn
  "Parses a SPARQL query and transform it into a pattern"
  ([sparql-string-or-query]
     (filter (fn [x] (not (:filter (meta x)))) (sparql-to-pattern-filters sparql-string-or-query))))

(defn- parse-sparql-to-query-fn
  "Parses a SPARQL query and builds a whole query dictionary"
  ([sparql-string]
     (let [query (com.hp.hpl.jena.query.QueryFactory/create sparql-string)
           pattern-filters (sparql-to-pattern-filters query)]
       { :vars (vec (map (fn [v] (keyword v)) (.getResultVars query)))
        :filters (filter #(:filter (meta %1)) pattern-filters)
        :pattern (filter #(not (:filter (meta %1))) pattern-filters)
        :kind (let [kind (.getQueryType query)]
                (cond (= kind com.hp.hpl.jena.query.Query/QueryTypeAsk) :ask
                      (= kind com.hp.hpl.jena.query.Query/QueryTypeConstruct) :construct
                      (= kind com.hp.hpl.jena.query.Query/QueryTypeDescribe) :describe
                      (= kind com.hp.hpl.jena.query.Query/QueryTypeSelect) :select
                      true :unknown)) })))

;; bulding of queries and filters


(defn- build-filter-two-parts
  "Builds a filter with two parts"
  ([expression arg-0 arg-1]
     (cond
      (= expression :>=) (new com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual arg-0 arg-1)
      (= expression :>) (new com.hp.hpl.jena.sparql.expr.E_GreaterThan arg-0 arg-1)
      (= expression :<=) (new com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual arg-0 arg-1)
      (= expression :<) (new com.hp.hpl.jena.sparql.expr.E_LessThan arg-0 arg-1)
      (= expression :!=) (new com.hp.hpl.jena.sparql.expr.E_NotEquals arg-0 arg-1)
      (= expression :=) (new com.hp.hpl.jena.sparql.expr.E_Equals arg-0 arg-1)
      (= expression :-) (new com.hp.hpl.jena.sparql.expr.E_Subtract arg-0 arg-1)
      (= expression :+) (new com.hp.hpl.jena.sparql.expr.E_Add arg-0 arg-1)
      (= expression :*) (new com.hp.hpl.jena.sparql.expr.E_Multiply arg-0 arg-1)
      (= expression :div) (new com.hp.hpl.jena.sparql.expr.E_Divide arg-0 arg-1))))

(defn- build-filter-one-part
  ([expression arg]
     (cond
      (= expression :str) (new com.hp.hpl.jena.sparql.expr.E_Str arg)
      (= expression :lang) (com.hp.hpl.jena.sparql.expr.E_Lang arg)
      (= expression :datatype) (new com.hp.hpl.jena.sparql.expr.E_Datatype arg)
      (= expression :bound) (new com.hp.hpl.jena.sparql.expr.E_Bound arg)
      (= expression :isIRI) (com.hp.hpl.jena.sparql.expr.E_IsIRI arg)
      (= expression :isURI) (new com.hp.hpl.jena.sparql.expr.E_IsURI arg)
      (= expression :isBlank) (com.hp.hpl.jena.sparql.expr.E_IsBlank arg)
      (= expression :isLiteral) (new com.hp.hpl.jena.sparql.expr.E_IsLiteral arg))))

(defn- build-filter-arg
  ([builder arg]
     (cond
      (keyword? arg) (new com.hp.hpl.jena.sparql.expr.ExprVar (.replace (keyword-to-string arg) "?" ""))
      (map? arg) (build-filter builder arg)
      true (com.hp.hpl.jena.sparql.expr.NodeValue/makeNode (literal-lexical-form arg) (literal-language arg) (literal-datatype-uri arg)))))

(defn- build-filter-fn
  ([builder filter]
     (if (= :two-parts (:kind filter))
       (build-filter-two-parts (:expression filter)
                               (build-filter-arg builder (first (:args filter)))
                               (build-filter-arg builder (second (:args filter))))
       (build-filter-one-part (:expression filter)
                              (build-filter-arg builder (first (:args filter)))))))

(defn- build-query-atom
  "Transforms a query atom (subject, predicate or object) in the suitable Jena object for a Jena query"
  ([atom]
     (if (keyword? atom)
       (com.hp.hpl.jena.sparql.core.Var/alloc (keyword-to-variable atom))
       (if (is-literal atom)
         (com.hp.hpl.jena.graph.Node/createLiteral (literal-lexical-form atom) (literal-language atom) (plaza.rdf.implementations.jena/find-jena-datatype (literal-datatype-uri atom)))
         (com.hp.hpl.jena.graph.Node/createURI (if (is-resource atom) (to-string atom) (str atom)))))))

(defn- build-query-fn
  "Transforms a query representation into a Jena Query object"
  ([builder query]
     (let [built-query (com.hp.hpl.jena.query.Query.)
           pattern (:pattern query)
           built-patterns (reduce (fn [acum item]
                                    (let [building (:building acum)
                                          optional (:optional acum)]
                                      (if (:optional (meta item))
                                        ;; add it to the optional elem
                                        (.addTriplePattern optional (com.hp.hpl.jena.graph.Triple/create (build-query-atom (nth item 0))
                                                                                                         (build-query-atom (nth item 1))
                                                                                                         (build-query-atom (nth item 2))))
                                        ;; Is not an optional triple
                                        (.addTriplePattern building (com.hp.hpl.jena.graph.Triple/create (build-query-atom (nth item 0))
                                                                                                         (build-query-atom (nth item 1))
                                                                                                         (build-query-atom (nth item 2)))))
                                      {:building building
                                       :optional optional}))
                                  {:building (com.hp.hpl.jena.sparql.syntax.ElementGroup.)
                                   :optional (com.hp.hpl.jena.sparql.syntax.ElementGroup.)}
                                  pattern)
           built-pattern (do
                           (when (not (.isEmpty (:optional built-patterns)))
                             (.addElement (:building built-patterns) (com.hp.hpl.jena.sparql.syntax.ElementOptional. (:optional built-patterns))))
                           (:building built-patterns))
           built-filters (loop [bfs (map (fn [f] (build-filter builder f)) (if (nil? (:filters query)) [] (:filters query)))]
                           (if (not (empty? bfs))
                             (let [bf (first bfs)]
                               (.addElement built-pattern (com.hp.hpl.jena.sparql.syntax.ElementFilter. bf))
                               (recur (rest bfs)))))]
       (do
         (loop [idx 0]
           (when (< idx (count (:vars query)))
             (do
               (.addResultVar built-query (keyword-to-variable (nth (:vars query) idx)))
               (recur (+ idx 1)))))
         (.setQueryPattern built-query built-pattern)
         (.setQueryType built-query (cond (= :ask (:kind query)) com.hp.hpl.jena.query.Query/QueryTypeAsk
                                          (= :construct (:kind query)) com.hp.hpl.jena.query.Query/QueryTypeConstruct
                                          (= :describe (:kind query)) com.hp.hpl.jena.query.Query/QueryTypeDescribe
                                          (= :select (:kind query)) com.hp.hpl.jena.query.Query/QueryTypeSelect))
         (when (:limit query)
           (.setLimit built-query (:limit query)))
         (when (:offset query)
           (.setOffset built-query (:offset query)))
         (when (:distinct query)
           (.setDistinct built-query true))
         (when (:reduced query)
           (.setReduced built-query true))
         built-query))))


(defn- process-model-query-result
  "Transforms a query result into a dicitionary of bindings"
  ([model result]
     (let [vars (iterator-seq (.iterator result))]
       (reduce (fn [acum item] (assoc acum (keyword (str "?" (.getName item))) (parse-sesame-object model (.getValue item)))) {} vars))))

(defn- model-query-fn
  "Queries a model and returns a map of bindings"
  ([model connection query]
     (let [tuple-query (.prepareTupleQuery connection org.openrdf.query.QueryLanguage/SPARQL (.toString (build-query *sparql-framework* query)))
           result (.evaluate tuple-query)]
       (loop [acum []
              should-continue (.hasNext result)]
         (if should-continue
           (recur (conj acum (process-model-query-result model (.next result)))
                  (.hasNext result))
           acum)))))

(defn- model-query-triples-fn
  "Queries a model and returns a list of triple sets with results binding variables in que query pattern"
  ([model connection query]
     (let [results (model-query-fn model connection query)]
       (map #(pattern-bind (:pattern query) %1) results))))


;; Sesame implementation

(deftype SesameResource [res] RDFResource RDFNode JavaObjectWrapper RDFPrintable
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


(deftype SesameBlank [res] RDFResource RDFNode JavaObjectWrapper RDFPrintable
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


(deftype SesameLiteral [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper RDFPrintable
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
  (literal-lexical-form [resource] (.stringValue res))
  (find-datatype [resource literal] (find-sesame-datatype literal)))

(deftype SesameTypedLiteral [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper RDFPrintable
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
  (literal-lexical-form [resource] (str (literal-value resource)))
  (find-datatype [resource literal] (find-sesame-datatype literal)))

(deftype SesameProperty [res] RDFResource RDFNode RDFDatatypeMapper JavaObjectWrapper RDFPrintable
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


(deftype SesameModel [mod] RDFModel RDFDatatypeMapper JavaObjectWrapper RDFPrintable
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
                   (if (or (instance? plaza.rdf.implementations.sesame.SesameResource uri)
                           (instance? plaza.rdf.implementations.sesame.SesameProperty uri))
                     (plaza.rdf.implementations.sesame.SesameProperty. (.. ValueFactoryImpl getInstance (createURI (to-string uri))))
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
                                                           (str lit)
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
  (to-string [model] (walk-triples model (fn [s p o] [(to-string s) (to-string p) (to-string o)])))
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
                       (.export connection writer (into-array org.openrdf.model.Resource []))
                       (finally (.close connection)))))
                  model)
  (find-datatype [model literal] (translate-plaza-format (parse-format format)))
  (query [model query] (let [connection (.getConnection mod)]
                         (try
                          (model-query-fn model connection query)
                          (finally (.close connection)))))
  (query-triples [model query] (let [connection (.getConnection mod)]
                                 (try
                                  (model-query-triples-fn model connection query)
                                  (finally (.close connection))))))


(deftype SesameSparqlFramework [] SparqlFramework
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


(defn- parse-sesame-object
  "Parses any Sesame relevant object into its plaza equivalent type"
  ([model sesame]
     (cond (instance? org.openrdf.model.URI sesame) (create-resource model (str sesame))
           (instance? org.openrdf.model.BNode sesame) (create-blank-node model (str (.getID sesame)))
           (instance? org.openrdf.model.Literal sesame) (if (or (nil? (.getDatatype sesame))
                                                                (= (.getDatatype sesame) "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral"))
                                                          (create-literal model (.stringValue sesame) (.getLanguage sesame))
                                                          (create-typed-literal model (sesame-typed-literal-tojava sesame) (str (.getDatatype sesame))))
           true (throw (Exception. (str "Unable to parse object " sesame " of type " (class sesame)))))))


;; Initialization

(defmethod build-model [:sesame]
  ([& options] (let [repo (SailRepository. (MemoryStore.))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))

(defmethod build-model [nil]
  ([& options] (let [repo (SailRepository. (MemoryStore.))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))

(defmethod build-model :default
  ([& options] (let [repo (SailRepository. (MemoryStore.))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))

(defn init-sesame-framework
  "Setup all the root bindings to use Plaza with the Sesame framework. This function must be called
   before start using Plaza"
  ([] (alter-root-model (build-model :sesame))
     (alter-root-sparql-framework (plaza.rdf.implementations.sesame.SesameSparqlFramework.))))
