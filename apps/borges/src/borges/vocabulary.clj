;; @author Antonio Garrote
;; @date  14.07.2010

(ns borges.vocabulary
  (use [plaza.rdf core schemas]
       [plaza.rdf.vocabularies foaf dublin-core]))

(defonce sioc "http://rdfs.org/sioc/ns#")
(defonce siocacc "http://rdfs.org/sioc/access#")
(defonce brg "http://plaza.org/vocabularies/borges#")

(register-rdf-ns :sioc sioc)
(register-rdf-ns :siocacc siocacc)

;; a role class
(defonce sioc:Role "http://rdfs.org/sioc/ns#Role")
(defonce sioc:function_of "http://rdfs.org/sioc/ns#function_of")
(defonce sioc:has_scope "http://rdfs.org/sioc/ns#has_scope")
(defonce sioc:scope_of "http://rdfs.org/sioc/ns#scope_of")
(defonce siocacc:has_permission "http://rdfs.org/sioc/access#has_permission")

;; roles classes used in the CMS app
(defonce brg:Admin "http://plaza.org/vocabularies/borges#Admin")
(defonce brg:User "http://plaza.org/vocabularies/borges#User")
(defonce brg:Visitor "http://plaza.org/vocabularies/borges#Visitor")
(defonce brg:Owner "http://plaza.org/vocabularies/borges#Owner")
(defonce brg:Creator "http://plaza.org/vocabularies/borges#Creator")


;; SIOC User account
(defonce sioc:UserAccount "http://rdfs.org/sioc/ns#UserAccount")
(defonce sioc:has_function "http://rdfs.org/sioc/ns#has_function")

;; SIOC Access Permission
(defonce siocacc:Permission "http://rdfs.org/sioc/access#Permission")
(defonce siocacc:has_permission "http://rdfs.org/sioc/access#has_permission")

;; Borges permissions
(defonce brg:GetOperation "http://plaza.org/vocabularies/borges#getOperation")
(defonce brg:PostOperation "http://plaza.org/vocabularies/borges#postOperation")
(defonce brg:PutOperation "http://plaza.org/vocabularies/borges#putOperation")
(defonce brg:DeleteOperation "http://plaza.org/vocabularies/borges#deleteOperation")

; Schemas

(declare-schemas-to-load
 (defonce sioc:Role-schema
   (make-rdfs-schema sioc:Role
                     :function_of    {:uri sioc:function_of       :range sioc:UserAccount}
                     :has_scope      {:uri sioc:has_scope         :range rdfs:Resource}
                     :has_permission {:uri siocacc:has_permission :range siocacc:Permission}))

  (defonce brg:Admin-schema
   (extend-rdfs-schemas brg:Admin [sioc:Role-schema]))

  (defonce brg:User-schema
   (extend-rdfs-schemas brg:User [sioc:Role-schema]))

  (defonce brg:Visitor-schema
   (extend-rdfs-schemas brg:Visitor [sioc:Role-schema]))

  (defonce brg:Owner-schema
   (extend-rdfs-schemas brg:Owner [sioc:Role-schema]))

  (defonce brg:Creator-schema
   (extend-rdfs-schemas brg:Creator [sioc:Role-schema]))

  (defonce sioc:UserAccount-schema
    (extend-rdfs-schemas sioc:UserAccount [foaf:Person-schema]
                         :has_function {:uri sioc:has_function :range sioc:Role}))

 (defonce siocacc:Permission-schema
   (make-rdfs-schema siocacc:Permission))

 (defonce brg:GetOperation-schema
   (extend-rdfs-schemas brg:GetOperation [siocacc:Permission-schema]))

 (defonce brg:PostOperation-schema
   (extend-rdfs-schemas brg:PostOperation [siocacc:Permission-schema]))

 (defonce brg:PutOperation-schema
   (extend-rdfs-schemas brg:PutOperation [siocacc:Permission-schema]))

 (defonce brg:DeleteOperation-schema
   (extend-rdfs-schemas brg:DeleteOperation [siocacc:Permission-schema])))

