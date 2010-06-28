;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 15.06.2010

(ns plaza.rdf.vocabularies.wsmo-lite
  (:use [plaza.rdf.core]
        [plaza.rdf.schemas]))

;; WSMO Lite minimal vocabulary
;; @see http://www.wsmo.org/TR/d11/v0.2/

(defonce wsl "http://www.wsmo.org/ns/wsmo-lite#")

(defonce wsl:Service "http://www.wsmo.org/ns/wsmo-lite#Service")
(defonce wsl:Operation "http://www.wsmo.org/ns/wsmo-lite#Operation")
(defonce wsl:Message "http://www.wsmo.org/ns/wsmo-lite#Message")
(defonce wsl:hasInputMessage "http://www.wsmo.org/ns/wsmo-lite#hasInputMessage")
(defonce wsl:hasOutputMessage "http://www.wsmo.org/ns/wsmo-lite#hasOutputMessage")
(defonce wsl:hasOperation "http://www.wsmo.org/ns/wsmo-lite#hasOperation")

(register-rdf-ns :wsl wsl)


(declare-schemas-to-load
 (defonce wsl:Message-schema
   (make-rdfs-schema wsl:Message))

 (defonce wsl:Operation-schema
   (make-rdfs-schema wsl:Operation
                     :hasInputMessage  { :uri wsl:hasInputMessage  :range wsl:Message }
                     :hasOutputMessage { :uri wsl:hasOutputMessage :range wsl:Message }))

 (defonce wsl:Service-schema
   (make-rdfs-schema wsl:Service
                     :hasOperation { :uri wsl:hasOperation :range wsl:Operation })))



