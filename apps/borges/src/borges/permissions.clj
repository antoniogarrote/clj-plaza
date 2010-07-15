;; @author Antonio Garrote
;; @date  14.07.2010

(ns borges.permissions
  (use [plaza.rdf core schemas sparql predicates]
       [plaza.triple-spaces core]
       [plaza.rdf.vocabularies foaf dublin-core]
       [clojure.contrib.logging :only [log]]
       [borges vocabulary]))

(defn- select-permission-role
  ([ts]
     (let [type-uri (object-from-triple (first (filter (triple-check (predicate? (uri? rdf:type))) ts)))]
       (condp = (str type-uri)
         brg:Admin   :admin
         brg:User    :user
         brg:Visitor :visitor
         brg:Owner   :owner
         brg:Creator :creator
         (throw (Exception. "unknown permission role"))))))

(defn- select-permission-type
  ([ts]
     (let [type-uri (object-from-triple (first (filter (triple-check (predicate? (uri? siocacc:has_permission))) ts)))]
       (condp = (str type-uri)
         brg:GetOperation    :get
         brg:PostOperation   :post
         brg:PutOperation    :put
         brg:DeleteOperation :delete
         (throw (Exception. "unknown permission type"))))))


(defn permissions-for-resource
  "Retrieves the permissions for a resource in the provided triple space"
  ([resource-uri permissions-ts]
     (let [pattern [[?s rdf:type ?t] [?s sioc:has_scope resource-uri] [?s siocacc:has_permission ?p]]
           permissionsp (rd permissions-ts pattern)
           permissions (filter #(not-any? (triple-and? (predicate? (uri? rdf:type))
                                                       (object? (uri? sioc:Role))) %1) permissionsp)]
       (reduce (fn [perm-map ts]
                 (try  (let [role (select-permission-role ts)
                             type (select-permission-type ts)
                             old-types (role perm-map)]
                         (if (nil? old-types)
                           (assoc perm-map role [type])
                           (assoc perm-map role (conj old-types type))))
                       (catch Exception ex
                         (log :error (str "Error parsing permissions for resource " resource-uri))
                         perm-map)))
               {} permissions))))


(defn role-key-to-uri
  ([key]
     (condp = key
       :admin brg:Admin
       :user brg:User
       :visitor brg:Visitor
       :owner brg:Owner
       :creator brg:Creator
       (throw (Exception. (str "unknow role for key " key))))))

(defn permission-keys-to-uris
  ([keys]
     (map #(condp = %1
             :get brg:GetOperation
             :post brg:PostOperation
             :put brg:PutOperation
             :delete brg:DeleteOperation
             (throw (Exception. (str "unknow permission for %1 " %1))))
          keys)))

(defn permissions-map-to-triples
  "Transforms a map of permissions into a set of triples that can be added to the triple set for a certain resource"
  ([resource perm-map]
     (reduce (fn [ts role-key]
               (let [role-subject (role-key-to-uri role-key)
                     permissions (permission-keys-to-uris (role-key perm-map))
                     role-triples (map (fn [perm] [role-subject siocacc:has_permission perm]) permissions)]
                 (concat ts (conj role-triples [resource sioc:scope_of role-subject]))))
             [] (keys perm-map))))

(defn permissions-triples-for-role
  "Transforms a map of permissions into a set of triples that can be added to the triple set for a certain resource"
  ([resource perm-map role]
     (let [perm-mapp {role (role perm-map)}]
       (permissions-map-to-triples resource perm-mapp))))


(defn default-resource-roles
  "Return a graph with the default permissions for the resources in the CMS system. These resources can be stored in a triple
   space and modified"
  ([]
     [;; sioc:Role resource
      ["http://plaza.org/vocabularies/borges#RoleAdminInst" [rdf:type brg:Admin
                                                             rdf:type sioc:Role
                                                             sioc:has_scope sioc:Role
                                                             siocacc:has_permission brg:GetOperation
                                                             siocacc:has_permission brg:PostOperation
                                                             siocacc:has_permission brg:PutOperation
                                                             siocacc:has_permission brg:DeleteOperation]]
      ["http://plaza.org/vocabularies/borges#RoleUserInst" [rdf:type brg:User
                                                            rdf:type sioc:Role
                                                            sioc:has_scope sioc:Role]]
      ["http://plaza.org/vocabularies/borges#RoleVisitorInst" [rdf:type brg:Visitor
                                                               rdf:type sioc:Role
                                                               sioc:has_scope sioc:Role]]
      ["http://plaza.org/vocabularies/borges#RoleOwnerInst" [rdf:type brg:Owner
                                                             rdf:type sioc:Role
                                                             sioc:has_scope sioc:Role]]
      ["http://plaza.org/vocabularies/borges#RoleCreatorInst" [rdf:type brg:Creator
                                                               rdf:type sioc:Role
                                                               sioc:has_scope sioc:Role]]
      ;; sioc:UseAccount resource
      ["http://plaza.org/vocabularies/borges#UserAccountAdminInst" [rdf:type brg:Admin
                                                                    rdf:type sioc:Role
                                                                    sioc:has_scope sioc:UserAccount
                                                                    siocacc:has_permission brg:GetOperation
                                                                    siocacc:has_permission brg:PostOperation
                                                                    siocacc:has_permission brg:PutOperation
                                                                    siocacc:has_permission brg:DeleteOperation]]
      ["http://plaza.org/vocabularies/borges#UserAccountUserInst" [rdf:type brg:User
                                                                   rdf:type sioc:Role
                                                                   sioc:has_scope sioc:UserAccount]]
      ["http://plaza.org/vocabularies/borges#UserAccountVisitorInst" [rdf:type brg:Visitor
                                                                      rdf:type sioc:Role
                                                                      sioc:has_scope sioc:UserAccount
                                                                      siocacc:has_permission brg:PostOperation]]
      ["http://plaza.org/vocabularies/borges#UserAccountOwnerInst" [rdf:type brg:Owner
                                                                    rdf:type sioc:Role
                                                                    sioc:has_scope sioc:UserAccount
                                                                    siocacc:has_permission brg:PutOperation
                                                                    siocacc:has_permission brg:DeleteOperation]]
      ["http://plaza.org/vocabularies/borges#UserAccountCreatorInst" [rdf:type brg:Creator
                                                                      rdf:type sioc:Role
                                                                      sioc:has_scope sioc:UserAccount
                                                                      siocacc:has_permission brg:PutOperation
                                                                      siocacc:has_permission brg:DeleteOperation]]]))
