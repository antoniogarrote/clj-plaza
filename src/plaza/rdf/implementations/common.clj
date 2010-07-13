;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 12.05.2010

(ns plaza.rdf.implementations.common
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

(defn make-custom-type
  "Builds a datatype for a custom XSD datatype URI based on the String basic type"
  ([uri]
     (proxy [com.hp.hpl.jena.datatypes.BaseDatatype] [uri]
       (unparse [v] (.lexicalValue v))
       (parse [lf] lf)
       (isEqual [v1 v2] (= v1 v2)))))

(defn find-jena-datatype
  "Finds the right datatype object from the string representation"
  ([literal]
     (let [lit (let [literal-str (keyword-to-string literal)]
                 (if (.startsWith literal-str "http://")
                   (aget (.split literal-str "#") 1)
                   literal))]
       (cond
        (= "xmlliteral" (.toLowerCase (keyword-to-string lit))) XMLLiteralType/theXMLLiteralType
        (= "literal" (.toLowerCase (keyword-to-string lit))) XMLLiteralType/theXMLLiteralType
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
        (= "string" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDstring
        :else (make-custom-type literal)))))

(defn datatype-symbol
  "Transforms a XMLSchema datatype URI into a symbol representing the type"
  ([literal]
     (let [lit (let [literal-str (keyword-to-string literal)]
                 (if (and (.startsWith literal-str "http://") (not (= -1 (.indexOf literal-str "#"))))
                   (aget (.split literal-str "#") 1)
                   literal))]
       (condp = lit
         "xmlliteral" (keyword lit)
         "literal" (keyword lit)
         "anyuri" (keyword lit)
         "boolean" (keyword lit)
         "byte" (keyword lit)
         "date" (keyword lit)
         "datetime" (keyword lit)
         "decimal" (keyword lit)
         "double" (keyword lit)
         "float" (keyword lit)
         "int" (keyword lit)
         "integer" (keyword lit)
         "long" (keyword lit)
         "string" (keyword lit)
         nil))))


(defn supported-datatype?
  "Returns true if the datatype sym or URI string is supported"
  ([sym]
     (try (do (find-jena-datatype sym) true)
          (catch Exception ex false))))

(defn datatype-uri
  "Returns the URI for a datatype symbol like :int :decimal or :anyuri"
  ([sym]
     (.getURI (find-jena-datatype sym))))

(defn parse-dataype-string
  "Parses a string containing a datatype of type sym"
  ([sym data]
     (.parse (find-jena-datatype sym) data)))

(defn is-filter-expr
  "Tests if one Jena expression is a filter expression"
  ([expr]
     (or (instance? com.hp.hpl.jena.sparql.expr.ExprFunction expr)
         (instance? com.hp.hpl.jena.sparql.syntax.ElementFilter expr))))

(defn is-var-expr-fn
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


(defn parse-literal-lexical
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


(defn parse-filter-expr
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
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_Divide) (parse-filter-expr-2 expr "div")
      (= (class expr) com.hp.hpl.jena.sparql.expr.E_SameTerm) (parse-filter-expr-2 expr "sameTerm")
      :else (throw (Exception. (str "Trying to parse unknown/not supported filter: " expr))))))

(defn sparql-to-pattern-filters
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

(defn parse-sparql-to-pattern-fn
  "Parses a SPARQL query and transform it into a pattern"
  ([sparql-string-or-query]
     (filter (fn [x] (not (:filter (meta x)))) (sparql-to-pattern-filters sparql-string-or-query))))

(defn parse-sparql-to-query-fn
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
      (= expression :div) (new com.hp.hpl.jena.sparql.expr.E_Divide arg-0 arg-1)
      (= expression :sameTerm) (new com.hp.hpl.jena.sparql.expr.E_SameTerm arg-0 arg-1))))

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
      (is-resource arg) (com.hp.hpl.jena.sparql.expr.NodeValue/makeNode (com.hp.hpl.jena.graph.Node/createURI (resource-id arg)))
      true (com.hp.hpl.jena.sparql.expr.NodeValue/makeNode (literal-lexical-form arg) (literal-language arg) (literal-datatype-uri arg)))))

(defn build-filter-fn
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
         (if (= (find-jena-datatype (literal-datatype-uri atom)) (find-jena-datatype :xmlliteral))
           (com.hp.hpl.jena.graph.Node/createLiteral (literal-lexical-form atom) (literal-language atom) false)
           (com.hp.hpl.jena.graph.Node/createLiteral (literal-lexical-form atom) (literal-language atom) (find-jena-datatype (literal-datatype-uri atom))))
         (if (is-blank atom) (com.hp.hpl.jena.graph.Node/createAnon (com.hp.hpl.jena.rdf.model.AnonId. (resource-id atom)))
             (com.hp.hpl.jena.graph.Node/createURI  (if (is-resource atom) (to-string atom)
                                                        (str atom))))))))

(defn build-query-fn
  "Transforms a query representation into a Jena Query object"
  ([builder query]
     (let [built-query (com.hp.hpl.jena.query.Query.)
           pattern (:pattern query)
           built-patterns (reduce (fn [acum item]
                                    (let [building (:building acum)
                                          optional (:optional acum)]
                                      (if (:optional (meta item))
                                        ;; add it to the optional elem
                                        (let [optg (com.hp.hpl.jena.sparql.syntax.ElementGroup.)]
                                          (.addTriplePattern optg (com.hp.hpl.jena.graph.Triple/create (build-query-atom (nth item 0))
                                                                                                       (build-query-atom (nth item 1))
                                                                                                       (build-query-atom (nth item 2))))
                                          {:building building
                                           :optional (conj optional optg)})
                                        ;; Is not an optional triple
                                        (do (.addTriplePattern building (com.hp.hpl.jena.graph.Triple/create (build-query-atom (nth item 0))
                                                                                                             (build-query-atom (nth item 1))
                                                                                                             (build-query-atom (nth item 2))))
                                            {:building building
                                             :optional optional}))))
                                  {:building (com.hp.hpl.jena.sparql.syntax.ElementGroup.)
                                   :optional []}
                                  pattern)
           built-pattern (do
                           (when (not (.isEmpty (:optional built-patterns)))
                             (doseq [optg (:optional built-patterns)]
                               (.addElement (:building built-patterns) (com.hp.hpl.jena.sparql.syntax.ElementOptional. optg))))
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
         (when (:order-by query)
           (.addOrderBy built-query (keyword-to-variable (:order-by query)) 1))
         built-query))))
