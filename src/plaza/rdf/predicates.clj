;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 04.05.2010

(ns plaza.rdf.predicates
  (:use (plaza utils)
        (plaza.rdf core))
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

;; model value extraction

(defn check
  "Applies a predicate to a concrete value"
  ([predicate val]
     (predicate val val)))

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
