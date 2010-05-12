;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 09.05.2010

(ns plaza.rdf.implementations.jena
  (:use [plaza.utils]
        [plaza.rdf core sparql])
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

;; Shared functions

(defn find-jena-datatype
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
      (= (class atom) com.hp.hpl.jena.sparql.core.Var) (keyword (str "?" (.getVarName atom)))
      (= (class atom) com.hp.hpl.jena.graph.Node_URI) (cond
                                                       (= pos :subject) (rdf-resource (.getURI atom))
                                                       (= pos :predicate) (rdf-property (.getURI atom))
                                                       (= pos :object) (rdf-resource (.getURI atom)))
      (= (class atom) com.hp.hpl.jena.graph.Node_Literal) (parse-pattern-literal atom)
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
                   (QueryFactory/create sparql-string-or-query)
                   sparql-string-or-query)
           query-pattern-els (.. query getQueryPattern getElements)]
       (flatten-1
        (map (fn [elem]
               (let [pattern-els (if (= (class elem) com.hp.hpl.jena.sparql.syntax.ElementOptional)
                                   (.. elem getOptionalElement getElements (get 0) patternElts)
                                   (if (instance? com.hp.hpl.jena.sparql.syntax.ElementFilter elem)
                                     (.iterator [elem])
                                     (.patternElts elem)))
                     is-optional (if (= (class elem) com.hp.hpl.jena.sparql.syntax.ElementOptional)
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
     (let [query (QueryFactory/create sparql-string)
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
       (Var/alloc (keyword-to-variable atom))
       (if (is-literal atom)
         (Node/createLiteral (literal-lexical-form atom) (literal-language atom) (literal-datatype-obj atom))
         (Node/createURI (str (if (is-resource atom) (to-java atom) (to-java (rdf-resource atom)))))))))

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
                                        (.addTriplePattern optional (Triple/create (build-query-atom (nth item 0))
                                                                                   (build-query-atom (nth item 1))
                                                                                   (build-query-atom (nth item 2))))
                                        ;; Is not an optional triple
                                        (.addTriplePattern building (Triple/create (build-query-atom (nth item 0))
                                                                                   (build-query-atom (nth item 1))
                                                                                   (build-query-atom (nth item 2)))))
                                      {:building building
                                       :optional optional}))
                                  {:building (ElementGroup.)
                                   :optional (ElementGroup.)}
                                  pattern)
           built-pattern (do
                           (when (not (.isEmpty (:optional built-patterns)))
                             (.addElement (:building built-patterns) (ElementOptional. (:optional built-patterns))))
                           (:building built-patterns))
           built-filters (loop [bfs (map (fn [f] (build-filter builder f)) (if (nil? (:filters query)) [] (:filters query)))]
                           (if (not (empty? bfs))
                             (let [bf (first bfs)]
                               (.addElement built-pattern (ElementFilter. bf))
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
     (let [vars (iterator-seq (.varNames result))]
       (reduce (fn [acum item] (assoc acum (keyword (str "?" item)) (parse-jena-object model (.get result item)))) {} vars))))

(defn- model-query-fn
  "Queries a model and returns a map of bindings"
  ([model query]
     (model-critical-read model
                          (let [qexec (QueryExecutionFactory/create (.toString (build-query *sparql-framework* query)) (to-java model))
                                        ;     (let [qexec (QueryExecutionFactory/create (build-query query)  @model)
                                results (iterator-seq (cond (= (:kind query) :select) (.execSelect qexec)))]
                            (map #(process-model-query-result model %1) results)))))

(defn- model-query-triples-fn
  "Queries a model and returns a list of triple sets with results binding variables in que query pattern"
  ([model query]
     (let [results (model-query-fn model query)]
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
  (literal-lexical-form [resource] (resource-id resource)))


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
  (literal-lexical-form [resource] (str "_:" (resource-id resource))))


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
  (find-datatype [resource literal] (find-jena-datatype literal)))

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
  (find-datatype [resource literal] (find-jena-datatype literal)))

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
  (literal-lexical-form [resource] (to-string res)))


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
                               (.createLiteral mod lit true)))
  (create-literal [model lit lang]
                  (plaza.rdf.implementations.jena.JenaLiteral.
                   (.createLiteral mod lit lang)))
  (create-typed-literal [model lit] (plaza.rdf.implementations.jena.JenaTypedLiteral.
                                     (.createTypedLiteral mod lit)))
  (create-typed-literal [model lit type]
                        (let [dt (find-datatype model type)]
                          (plaza.rdf.implementations.jena.JenaTypedLiteral.
                           (.createTypedLiteral mod lit dt))))
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
  (output-string  [model format]
                  (critical-read model (fn [] (.write mod *out* (parse-format format)))))
  (find-datatype [model literal] (find-jena-datatype literal))
  (query [model query] (model-query-fn model query))
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

(defmethod build-model [nil]
  ([& options] (plaza.rdf.implementations.jena.JenaModel. (ModelFactory/createDefaultModel))))

(defmethod build-model :default
  ([& options] (plaza.rdf.implementations.jena.JenaModel. (ModelFactory/createDefaultModel))))

(defn init-jena-framework
  "Setup all the root bindings to use Plaza with the Jena framework. This function must be called
   before start using Plaza"
  ([] (alter-root-model (build-model :jena))
      (alter-root-sparql-framework (plaza.rdf.implementations.jena.JenaSparqlFramework.))))
