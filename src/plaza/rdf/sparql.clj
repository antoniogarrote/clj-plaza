;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 04.05.2010

(ns plaza.rdf.sparql
  (:use (plaza.rdf core predicates)
        (plaza utils))
  (:import (com.hp.hpl.jena.rdf.model ModelFactory)
           (com.hp.hpl.jena.reasoner.rulesys RDFSRuleReasonerFactory)
           (com.hp.hpl.jena.vocabulary ReasonerVocabulary)
           (com.hp.hpl.jena.datatypes.xsd XSDDatatype)
           (com.hp.hpl.jena.sparql.core Var)
           (com.hp.hpl.jena.datatypes.xsd.impl XMLLiteralType)
           (com.hp.hpl.jena.query QueryFactory QueryExecutionFactory DatasetFactory)
           (com.hp.hpl.jena.sparql.syntax Element ElementGroup ElementOptional ElementFilter)
           (com.hp.hpl.jena.graph Node Triple)
           (com.hp.hpl.jena.sparql.expr E_Str E_Lang E_Datatype E_Bound E_IsIRI E_IsURI E_IsBlank E_IsLiteral E_GreaterThanOrEqual E_GreaterThan
                                        E_LessThanOrEqual E_LessThan E_NotEquals E_Equals E_Subtract E_Add E_Multiply E_Divide)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;                  SPARQL                     ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-pattern
  "Builds a new pattern representation"
  ([triples]
     (flatten-1
      (map
       (fn [triple]
         (let [tmp (rdf-triple triple)
               optional (:optional (meta triple))]
           (if (coll? (first tmp))
             (map #(with-meta %1 {:optional optional}) tmp)
             (with-meta tmp {:optional optional}))))
       triples))))

;; (defn make-query
;;   "Builds a new query representation"
;;   ([] {})
;;   ([kind vars pattern & args]
;;      (let [opts (apply hash-map args)]
;;        {:kind kind :vars vars :pattern (with-model (build-model)
;;                                          (make-pattern pattern))})))

;; root sparql query

(defmacro defquery
  "Sets up a default query for a set of form that describe the query incrementally"
  ([& rest]
     `(-> {} ~@rest)))

(defn query-set-type
  "Sets the type of a query"
  ([kind]
     (fn [query] (query-set-type query kind)))
  ([query kind]
     (assoc query :kind kind)))

(defn query-set-vars
  "Set a collection of vars for the query"
  ([query vars]
     (assoc query :vars vars))
  ([vars]
     (fn [query] (query-set-vars query vars))))

(defn query-set-filters
  "Set a collection of filters for the query"
  ([query filters]
     (assoc query :filters filters))
  ([filters]
     (fn [query] (query-set-filters query filters))))

(defn query-remove-var
  "Removes a var from the collection of vars in the query"
  ([query var]
     (assoc query :vars (remove #(= %1 var) (:vars query))))
  ([vars]
     (fn [query] (query-remove-var query vars))))

(defn query-add-var
  "Adds a var to the collection of vars in the query"
  ([query var]
     (assoc query :vars (conj (:vars query) var)))
  ([vars]
     (fn [query] (query-add-var query vars))))

(defn query-set-pattern
  "Sets the pattern for the query"
  ([query pattern]
     (assoc query :pattern pattern))
  ([pattern]
     (fn [query] (query-set-pattern query pattern))))

;; Parsing a SPARQL string query into a query pattern representation

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

(defn- is-filter-expr
  "Tests if one Jena expression is a filter expression"
  ([expr]
     (or (instance? com.hp.hpl.jena.sparql.expr.ExprFunction expr)
         (instance? com.hp.hpl.jena.sparql.syntax.ElementFilter expr))))

(defn- is-var-expr
  "Tests if one Jena expression is a var expression"
  ([expr]
     (= (class expr) com.hp.hpl.jena.sparql.expr.ExprVar)))


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
      (is-var-expr expr) (keyword (.toString expr))
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


(defn make-filter
  "Makes a new filter expression"
  ([name & args]
     {:expression (keyword name)
      :args args
      :kind (if  (= (count args) 1) :one-part :two-parts)}))



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

(declare build-filter)
(defn- build-filter-arg
  ([arg]
     (cond
      (keyword? arg) (new com.hp.hpl.jena.sparql.expr.ExprVar (.replace (keyword-to-string arg) "?" ""))
      (map? arg) (build-filter arg)
      true (com.hp.hpl.jena.sparql.expr.NodeValue/makeNode (literal-lexical-form arg) (literal-language arg) (literal-datatype-uri arg)))))

(defn build-filter
  ([filter]
     (if (= :two-parts (:kind filter))
       (build-filter-two-parts (:expression filter)
                               (build-filter-arg (first (:args filter)))
                               (build-filter-arg (second (:args filter))))
       (build-filter-one-part (:expression filter)
                              (build-filter-arg (first (:args filter)))))))


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

(defn sparql-to-pattern
  "Parses a SPARQL query and transform it into a pattern"
  ([sparql-string-or-query]
     (filter (fn [x] (not (:filter (meta x)))) (sparql-to-pattern-filters sparql-string-or-query))))

(defn sparql-to-query
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

(defn- keyword-to-variable
  "Transforms a symbol ':?t' into a variable name 't'"
  ([kw]
     (if (.startsWith (keyword-to-string kw) "?")
       (aget (.split (keyword-to-string kw) "\\?") 1)
       (keyword-to-string kw))))

(defn- build-query-atom
  "Transforms a query atom (subject, predicate or object) in the suitable Jena object for a Jena query"
  ([atom]
     (if (keyword? atom)
       (Var/alloc (keyword-to-variable atom))
       (if (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
         (Node/createLiteral (.getLexicalForm atom) (.getLanguage atom) (.getDatatype atom))
         (Node/createURI (str (if (is-resource atom) atom (rdf-resource atom))))))))

(defn build-query
  "Transforms a query representation into a Jena Query object"
  ([query]
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
           built-filters (loop [bfs (map (fn [f] (build-filter f)) (if (nil? (:filters query)) [] (:filters query)))]
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
         built-query))))

;; Triples <-> Pattern transformations


(defn pattern-bind
  "Binds variables in a pattern with some values"
  ([pattern binding-map]
     (vec
      (map (fn [t]
             (let [[s p o] t
                   sp (if (get binding-map s)
                        (triple-subject (get binding-map s))
                        s)
                   pp (if (get binding-map p)
                        (triple-predicate (get binding-map p))
                        p)
                   op (if (get binding-map o)
                        (triple-object (get binding-map o))
                        o)]
               (if (check (is-optional?) t)
                 (optional [sp pp op])
                 [sp pp op])))
           pattern))))

(defn- process-model-query-result
  "Transforms a query result into a dicitionary of bindings"
  ([result]
     (let [vars (iterator-seq (.varNames result))]
       (reduce (fn [acum item] (assoc acum (keyword (str "?" item)) (.get result item))) {} vars))))

(defn model-query
  "Queries a model and returns a map of bindings"
  ([model query]
     (let [;_model (println (str "MODEL: " (model-to-format model :ttl)))
           ;_test (println (.toString (build-query query)))
           qexec (QueryExecutionFactory/create (.toString (build-query query)) @model)
;     (let [qexec (QueryExecutionFactory/create (build-query query)  @model)
           results (iterator-seq (cond (= (:kind query) :select) (.execSelect qexec)))]
           ;_results (println (str "RESULTS " results))]
       (map #(process-model-query-result %1) results))))

(defn model-query-triples
  "Queries a model and returns a map of bindings"
  ([model query]
     (let [results (model-query model query)]
       (map #(pattern-bind (:pattern query) %1) results))))
