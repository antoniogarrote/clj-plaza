;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 12.07.2010

(ns plaza.rest.validations
  (:use
   [clojure.contrib.logging :only [log]]
   [clojure.contrib.seq-utils :only [includes?]]
   [plaza utils]
   [plaza.rdf core]
   [plaza.rdf.vocabularies plaza]
   [plaza.rest utils])
  (:require
   [clojure.contrib.json :as json]))


(defn validate-presence-property-urls
  "Checks that a list of RDF properties have received values after processing the HTTP request parameters"
  ([property-urls]
     (fn [request environment]
       (let [graph (:mapping environment)
             predicates-pre (map (fn [[p o]] (let [op (if (keyword? o) (name o) (str o))]
                                               (if (.startsWith op "?") nil (str p)))) graph)
             predicates (reduce (fn [acum p] (if (nil? p) acum (conj acum p))) [] predicates-pre)
             errors (reduce (fn [acum prop]
                              (if (includes? predicates (str prop))
                                acum
                                (conj acum [(rdf-resource prop) (rdf-property plz:validationError) (l "property not found")])))
                            [] property-urls)]
         (if (empty? errors)
           environment
           (-> environment
               (assoc :status 400)
               (assoc :body (render-triples errors (mime-to-format request) plz:RestResource request))))))))
