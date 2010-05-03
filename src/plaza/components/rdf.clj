;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 30.04.2010

(ns plaza.components.rdf
  (:use [plaza.utils])
  (:import (com.hp.hpl.jena.rdf.model ModelFactory)
           (com.hp.hpl.jena.reasoner.rulesys RDFSRuleReasonerFactory)
           (com.hp.hpl.jena.vocabulary ReasonerVocabulary)
           (com.hp.hpl.jena.datatypes.xsd XSDDatatype)
           (com.hp.hpl.jena.datatypes.xsd.impl XMLLiteralType)
           (com.hp.hpl.jena.query QueryFactory)
           (com.hp.hpl.jena.sparql.syntax Element ElementGroup ElementOptional)
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

(defmacro with-rdf-model
  "Sets up the default model for a set of forms"
  [model & rest]
  `(binding [*rdf-model* ~model]
     ~@rest))

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

(defn resource-uri
  "Returns the URI of a resource (subject, predicate or object)"
  ([resource]
     (.getURI resource)))

(defn resource-qname-prefix
  "Returns the prefix part of the qname of the resource"
  ([resource]
     (.getNameSpace resource)))

(defn resource-qname-local
  "Returns the local part of the qname of the resource"
  ([resource]
     (.getLocalName resource)))

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
        (= "dateTime" (.toLowerCase (keyword-to-string lit))) XSDDatatype/XSDdateTime
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
     (if (coll? subject)
       (let [[rdf-ns local] subject]
         (rdf-resource rdf-ns local))
       (if (.startsWith (keyword-to-string subject) "?")
         (keyword subject)
         (rdf-resource subject)))))

(defn triple-predicate
  "Defines the predicate of a statement"
  ([predicate]
     (if (coll? predicate)
       (let [[rdf-ns local] predicate]
         (rdf-property rdf-ns local))
       (if (.startsWith (keyword-to-string predicate) "?")
         (keyword predicate)
         (rdf-property predicate)))))

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


(defn out
  "Writes a model using the chosen format"
  ([]
     (.write @*rdf-model* *out* "N-TRIPLE"))
  ([format]
     (.write @*rdf-model* *out* format))
  ([format model]
     (.write model *out* format)))

;; model value extraction

(defn check
  "Applies a predicate to a concrete value"
  ([predicate val]
     (predicate val val)))

;; model manipulation predicates

(defn uri?
  "Matches a URI or curie against a triple atom"
  ([ns local]
     (uri? (expand-ns ns local)))
  ([uri]
     (fn [triple atom]
       (cond (or (= (class atom) clojure.lang.Keyword)
                 (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl))
             false
             true
             (= (.getURI atom) uri)))))

(defn qname-prefix?
  "Matches a URI or curie against a triple atom"
  ([prefix]
     (fn [triple atom]
       (cond (or (= (class atom) clojure.lang.Keyword)
                 (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl))
             false
             true
             (= (.getNameSpace atom) (if (nil? (find-ns-registry prefix)) (keyword-to-string prefix) (find-ns-registry prefix)))))))

(defn qname-local?
  "Matches a URI or curie against a triple atom"
  ([local]
     (fn [triple atom]
       (cond (or (= (class atom) clojure.lang.Keyword)
                 (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl))
             false
             true
             (= (.getLocalName atom) (keyword-to-string local))))))

(defn literal-value?
  "Matches a literal with a certain literal value"
  ([lit]
     (fn [triple atom]
       (cond (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
             (= (.toString atom) (.toString lit))
             true false))))

(defn is-literal?
  "Matches a literal with a certain literal value"
  ([]
     (fn [triple atom]
       (cond (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
             true
             true false))))

(defn literal-fn?
  "Applies a custom predicate function to a literal"
  ([f] (fn [triple atom] (f atom))))

(defn literal?
  "Matches the value or the value and language of a literal"
  ([val]
     (fn [triple atom]
       (if (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
         (= (literal-value atom) val)
         false)))
  ([val lang]
     (fn [triple atom]
       (if (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
         (and (= (literal-value atom) val)
              (= (literal-language atom) lang))
         false))))


(defn datatype?
  "Matches the value or the value and language of a literal"
  ([data-uri]
     (fn [triple atom]
       (if (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
         (= (find-datatype (literal-datatype-uri atom)) (find-datatype data-uri))
         false))))

(defn is-variable?
  "Matches a literal with a certain literal value"
  ([]
     (fn [triple atom]
       (cond (= (class atom) clojure.lang.Keyword)
             true
             true false))))

(defn is-resource?
  "Matches a literal with a certain literal value"
  ([]
     (fn [triple atom]
       (cond (= (class atom) com.hp.hpl.jena.rdf.model.impl.ResourceImpl)
             true
             true false))))

(defn is-optional?
  "Checks if a triple is an optional part of a query"
  ([]
     (fn [triple atom]
       (= true (:optional (meta triple))))))

(defn and?
  "Applies and to a series of matchers"
  ([& preds]
     (fn [triple atom]
       (reduce (fn [acum item] (and acum (item triple atom))) true preds))))

(defn or?
  "Applies or to a series of matchers"
  ([& preds]
     (fn [triple atom]
       (reduce (fn [acum item] (or acum (item triple atom))) false preds))))

(defn not?
  "Negates a clause query"
  ([pred]
     (fn [triple atom]
       (not (pred triple atom)))))

(defn triple-and?
  "Checks if one triple matches a set of conditions"
  ([& conds]
     (fn [triple] ((apply and? conds) triple triple))))

(defn triple?
  "Checks if one triple matches a set of conditions"
  ([cond]
     (fn [triple] ((triple-and? cond) triple))))

(defn subject-and?
  "Checks a condition over the subject of a triple"
  ([& conditions]
     (fn [triple atom]
       ((apply and? conditions) triple (nth triple 0)))))

(defn subject?
  "Checks a condition over the subject of a triple"
  ([condition]
     (fn [triple atom]
       ((subject-and? condition) triple atom))))

(defn predicate-and?
  "Checks a condition over the predicate of a triple"
  ([& conditions]
     (fn [triple atom]
       ((apply and? conditions) triple (nth triple 1)))))

(defn predicate?
  "Checks a condition over the predicate of a triple"
  ([condition]
     (fn [triple atom]
       ((predicate-and? condition) triple atom))))

(defn object-and?
  "Checks a condition over the object of a triple"
  ([& conditions]
     (fn [triple atom]
       ((apply and? conditions) triple (nth triple 2)))))

(defn object?
  "Checks a condition over the object of a triple"
  ([condition]
     (fn [triple atom]
       ((object-and? condition) triple atom))))

(defn triple-or?
  "Checks if one triple matches a set of conditions"
  ([& conds]
     (fn [triple] ((apply or? conds) triple triple))))

(defn subject-or?
  "Checks a condition over the subject of a triple"
  ([& conditions]
     (fn [triple atom]
       ((apply or? conditions) triple (nth triple 0)))))

(defn predicate-or?
  "Checks a condition over the predicate of a triple"
  ([& conditions]
     (fn [triple atom]
       ((apply or? conditions) triple (nth triple 1)))))

(defn object-or?
  "Checks a condition over the object of a triple"
  ([& conditions]
     (fn [triple atom]
       ((apply or? conditions) triple (nth triple 2)))))

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
;;        {:kind kind :vars vars :pattern (with-rdf-model (build-model)
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
     (= (class expr) com.hp.hpl.jena.sparql.syntax.ElementFilter)))

(defn- is-var-expr
  "Tests if one Jena expression is a var expression"
  ([expr]
     (= (class expr) com.hp.hpl.jena.sparql.expr.ExprVar)))

(declare parse-filter-expr)

(defn- parse-next-filter-expr
  ([expr]
     (cond
      (is-var-expr expr) (keyword (.toString expr))
      (is-filter-expr expr) (parse-filter-expr expr)
      true (.toString expr))))

(defn parse-filter-expr-2
  ([expr symbol]
     {:expression (keyword symbol)
      :kind :two-parts
      :args [(parse-next-filter-expr (.. expr (getArg 1)))
             (parse-next-filter-expr (.. expr (getArg 2)))]}))

(defn parse-filter-expr-1
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



(defn build-filter-two-parts
  "Builds a filter with two parts"
  ([expression arg-0 arg-1]
     (println (str "EXPRESSION " expression))
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

(defn build-filter-one-part
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
(defn build-filter-arg
  ([arg]
     (println (str "ARG: " (keyword-to-string arg) " " arg ))
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
       (Node/createVariable (keyword-to-variable atom))
       (if (= (class atom) com.hp.hpl.jena.rdf.model.impl.LiteralImpl)
         (Node/createLiteral (.toString atom))
         (Node/createURI (.toString atom))))))

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
                           (:building built-patterns))]
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
