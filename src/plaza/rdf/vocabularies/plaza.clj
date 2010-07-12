;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 16.06.2010

(ns plaza.rdf.vocabularies.plaza
  (:use [plaza.rdf.core]
        [plaza.rdf.schemas]))

;; Plaza vocabulary and ontologies

(defonce plz "http://plaza.org/vocabularies")

(defonce plz:RestResource "http://plaza.org/vocabularies/restResource")
(defonce plz:restResourceId "http://plaza.org/vocabularies/restResourceId")
(defonce plz:validationError "http://plaza.org/vocabularies/validationError")

(register-rdf-ns :plz plz)

(declare-schemas-to-load
 (defonce plz:RestResource-schema
   (make-rdfs-schema plz:RestResource
                     :validationError {:uri plz:validationError :range :string})))
