;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 09.06.2010

(ns plaza.rdf.vocabularies.dublin-core
  (:use
   [plaza.rdf.core]
   [plaza.rdf.schemas]))

;; Dublic Core Metadata Initiative terms
;; @see http://dublincore.org/schemas/rdfs/

(defonce dc11 "http://purl.org/dc/elements/1.1/")

(defonce dc11:title "http://purl.org/dc/elements/1.1/title")
(defonce dc11:creator "http://purl.org/dc/elements/1.1/creator")
(defonce dc11:subject "http://purl.org/dc/elements/1.1/subject")
(defonce dc11:description "http://purl.org/dc/elements/1.1/description")
(defonce dc11:publisher "http://purl.org/dc/elements/1.1/publisher")
(defonce dc11:contributor "http://purl.org/dc/elements/1.1/contributor")
(defonce dc11:date "http://purl.org/dc/elements/1.1/date")
(defonce dc11:type "http://purl.org/dc/elements/1.1/type")
(defonce dc11:format "http://purl.org/dc/elements/1.1/format")
(defonce dc11:identifier "http://purl.org/dc/elements/1.1/identifier")
(defonce dc11:source "http://purl.org/dc/elements/1.1/source")
(defonce dc11:language "http://purl.org/dc/elements/1.1/language")
(defonce dc11:relation "http://purl.org/dc/elements/1.1/relation")
(defonce dc11:coverage "http://purl.org/dc/elements/1.1/coverage")
(defonce dc11:rights "http://purl.org/dc/elements/1.1/rights")


(defonce dct "http://purl.org/dc/terms/")

(defonce dct:title "http://purl.org/dc/terms/title")
(defonce dct:creator "http://purl.org/dc/terms/creator")
(defonce dct:subject "http://purl.org/dc/terms/subject")
(defonce dct:description "http://purl.org/dc/terms/description")
(defonce dct:publisher "http://purl.org/dc/terms/publisher")
(defonce dct:contributor "http://purl.org/dc/terms/contributor")
(defonce dct:date "http://purl.org/dc/terms/date")
(defonce dct:type "http://purl.org/dc/terms/type")
(defonce dct:format "http://purl.org/dc/terms/format")
(defonce dct:identifier "http://purl.org/dc/terms/identifier")
(defonce dct:source "http://purl.org/dc/terms/source")
(defonce dct:language "http://purl.org/dc/terms/language")
(defonce dct:relation "http://purl.org/dc/terms/relation")
(defonce dct:coverage "http://purl.org/dc/terms/coverage")
(defonce dct:rights "http://purl.org/dc/terms/rights")
(defonce dct:audience "http://purl.org/dc/terms/audience")
(defonce dct:alternative "http://purl.org/dc/terms/alternative")
(defonce dct:tableOfContents "http://purl.org/dc/terms/tableOfContents")
(defonce dct:abstract "http://purl.org/dc/terms/abstract")
(defonce dct:created "http://purl.org/dc/terms/created")
(defonce dct:valid "http://purl.org/dc/terms/valid")
(defonce dct:available "http://purl.org/dc/terms/available")
;; add more...

(register-rdf-ns :dc11 dc11)
(register-rdf-ns :dct dct)
