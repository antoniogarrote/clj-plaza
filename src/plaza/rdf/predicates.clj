;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 04.05.2010

(ns plaza.rdf.predicates
  (:use (plaza utils)
        (plaza.rdf core sparql)))

;; model value extraction

(defn triple-check-apply
  "Applies a predicate to a concrete value"
  ([predicate val]
     (predicate val val)))

(defn tca
  "Shortcut for triple-check-apply"
  ([& args]
     (apply triple-check-apply args)))

;; model testing
(declare triple-and?)

(defn triple-check
  "Checks if one triple matches a set of conditions"
  ([cond]
     (fn [triple] ((triple-and? cond) triple))))

(defn tc
  "Shortcut for triple-check"
  ([& args]
     (apply triple-check args)))

(defn triple-transform
  "Accepts a single argument function that will receive a triple and transform it"
  ([f]
     (fn [triple atom] (f triple))))

(defn tt
  "Shortcut for triple-transform"
  ([& args]
     (apply triple-transform args)))

;; predicates

(defn uri?
  "Matches a URI or curie against a triple atom"
  ([ns local]
     (uri? (expand-ns ns local)))
  ([uri]
     (fn [triple atom]
       (cond (or (instance? clojure.lang.Keyword atom)
                 (is-literal atom))
             false
             true
             (= (resource-id atom) uri)))))

(defn qname-prefix?
  "Matches a URI or curie against a triple atom"
  ([prefix]
     (fn [triple atom]
       (cond (or (instance? clojure.lang.Keyword)
                 (is-literal atom))
             false
             true
             (= (qname-prefix atom) (if (nil? (find-ns-registry prefix)) (keyword-to-string prefix) (find-ns-registry prefix)))))))

(defn qname-local?
  "Matches a URI or curie against a triple atom"
  ([local]
     (fn [triple atom]
       (cond (or (instance? clojure.lang.Keyword atom)
                 (is-literal atom))
             false
             true
             (= (qname-local atom) (keyword-to-string local))))))

(defn literal-value?
  "Matches a literal with a certain literal value"
  ([lit]
     (fn [triple atom]
       (cond (is-literal atom)
             (= (literal-lexical-form atom) (str lit))
             true false))))

(defn is-literal?
  "Matches a literal with a certain literal value"
  ([]
     (fn [triple atom]
       (cond (and (instance? plaza.rdf.core.RDFResource atom)
                  (is-literal atom))
             true
             true false))))

(defn literal-fn?
  "Applies a custom predicate function to a literal"
  ([f] (fn [triple atom] (f atom))))

(defn literal?
  "Matches the value or the value and language of a literal"
  ([val]
     (fn [triple atom]
       (if (and (instance? plaza.rdf.core.RDFResource atom)
                (is-literal atom))
         (= (literal-value atom) val)
         false)))
  ([val lang]
     (fn [triple atom]
       (if (and (instance? plaza.rdf.core.RDFResource atom)
                (is-literal atom))
         (and (= (literal-value atom) val)
              (= (literal-language atom) lang))
         false))))


(defn datatype?
  "Matches the value or the value and language of a literal"
  ([data-uri]
     (fn [triple atom]
       (if (and (instance? plaza.rdf.core.RDFResource atom)
                (is-literal atom))
         (= (find-datatype *rdf-model* (literal-datatype-uri atom)) (find-datatype *rdf-model* data-uri))
         false))))

(defn is-variable?
  "Matches a variable"
  ([]
     (fn [triple atom]
       (is-var-expr *sparql-framework* atom))))

(defn is-blank-node?
  "Matches a blank node"
  ([]
     (fn [triple atom]
       (if (or (string? atom)
               (keyword? atom))
         false
         (is-blank atom)))))

(defn blank-node?
  "Matches a blank node with a certain id"
  ([id]
     (fn [triple atom]
       (if (or (string? atom)
               (keyword? atom))
         false
         (if  (and (instance? plaza.rdf.core.RDFResource atom)
                   (is-blank atom))
           (= (keyword-to-string id) (str (resource-id atom)))
           false)))))

(defn is-resource?
  "Matches a literal with a certain literal value"
  ([]
     (fn [triple atom]
       (and (instance? plaza.rdf.core.RDFResource atom)
            (is-resource atom)))))

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

(defn regex?
  "Checks if a value matches a ceratin regular expression"
  ([regex]
     (fn [triple atom]
       (not (empty? (re-find regex (str atom)))))))

(defn fn-apply?
  "Applies a function to a value"
  ([f]
     (fn [triple atom] (f atom))))

(defn fn-triple-apply?
  "Applies a function to a value"
  ([f]
     (fn [triple atom] (f triple))))
