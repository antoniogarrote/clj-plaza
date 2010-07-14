;; @author Antonio Garrote
;; @date  30.06.2010

(ns scrummy-clone.resources.common
  (use [plaza.rdf core schemas]
       [plaza.rdf.vocabularies foaf dublin-core]))


; Namespace for all shared meta data in the application
(defonce scc "http://scrummyclone.com/vocabularies/common/")
(register-rdf-ns :scc scc)


; Classes
(defonce scc:Sprint "http://scrummyclone.com/vocabularies/Sprint")
(defonce scc:Story "http://scrummyclone.com/vocabularies/Story")
(defonce scc:Task "http://scrummyclone.com/vocabularies/Task")

; Properties
(defonce scc:uriId "http://scrummyclone.com/vocabularies/uriId")
(defonce scc:status "http://scrummyclone.com/vocabularies/status")
(defonce scc:priority "http://scrummyclone.com/vocabularies/priority")
(defonce scc:hasSprints "http://scrummyclone.com/vocabularies/hasSprints")
(defonce scc:belongsToProject "http://scrummyclone.com/vocabularies/belongsToProject")
(defonce scc:hasStories "http://scrummyclone.com/vocabularies/hasStories")
(defonce scc:belongsToSprint "http://scrummyclone.com/vocabularies/belongsToSprint")
(defonce scc:hasTasks "http://scrummyclone.com/vocabularies/hasTasks")
(defonce scc:belongsToStory "http://scrummyclone.com/vocabularies/belongsToStory")



; Schemas
(declare-schemas-to-load

 (defonce scc:Project-schema
   (make-rdfs-schema foaf:Project
                     :title      {:uri dct:title      :range :string}
                     :hasSprints {:uri scc:hasSprints :range scc:Sprint}))

 (defonce scc:Sprint-schema
   (make-rdfs-schema scc:Sprint
                     :created          {:uri dct:created          :range :date }
                     :hasStories       {:uri scc:hasStories       :range scc:Story}
                     :belongsToProject {:uri scc:belongsToProject :range foaf:Project}))

  (defonce scc:Story-schema
   (make-rdfs-schema scc:Story
                     :title            {:uri dct:title            :range :string}
                     :priority         {:uri scc:priority         :range :int }
                     :hasTasks         {:uri scc:hasTasks         :range scc:Task}
                     :belongsToSprint  {:uri scc:belongsToSprint  :range scc:Sprint}))

 (defonce scc:Task-schema
   (make-rdfs-schema scc:Task
                     :title           {:uri dct:title            :range :string}
                     :status          {:uri scc:status          :range :string}
                     :asignee         {:uri dct:contributor     :range :string}
                     :belongsToStory  {:uri scc:belongsToStory  :range scc:Sprint})))
