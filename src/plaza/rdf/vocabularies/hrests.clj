;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 15.06.2010

(ns plaza.rdf.vocabularies.hrests
  (:use [plaza.rdf.core]
        [plaza.rdf.schemas]
        [plaza.rdf.vocabularies.wsmo-lite]))

;; hRESTS vocabulary
;; @see http://www.wsmo.org/ns/hrests/

(defonce hr "http://www.wsmo.org/ns/hrests#")

(defonce hr:hasAddress "http://www.wsmo.org/ns/hrests#hasAddress")
(defonce hr:hasMethod "http://www.wsmo.org/ns/hrests#hasMethod")
(defonce hr:URITemplate "http://www.wsmo.org/ns/hrests#URITemplate")

(register-rdf-ns :hr hr)


(defonce hr:Operation-schema
  (make-rdfs-schema wsl:Operation
                    :hasInputMessage  { :uri wsl:hasInputMessage  :range wsl:Message }
                    :hasOutputMessage { :uri wsl:hasOutputMessage :range wsl:Message }
                    :hasAddress       { :uri hr:hasAddress        :range :string } ; @todo this is really a hr:URITemplate
                    :hasMethod        { :uri hr:hasMethod         :range :string }))
(defonce hr:URITemplate
  (make-rdfs-schema hr:URITemplate))
