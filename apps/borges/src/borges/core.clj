;; @author Antonio Garrote
;; @date  14.07.2010

(ns borges.core
  (:use [compojure core]
        [compojure response]
        [ring.adapter jetty]
        [ring.util.response :only (file-response)]
        [plaza.rdf core schemas]
        [plaza.rdf.implementations jena]
        [plaza.rdf.vocabularies plaza]
        [plaza.rest core utils]
        [plaza.triple-spaces core])
  (:require [compojure.route :as route]
            [clojure.contrib.str-utils2 :as str2]))

;; We are using Jena as the framework's backend
(init-jena-framework)
(init-vocabularies)

;; We register the resources in the TBox
;(tbox-register-schema :project scc:Project-schema)
;(tbox-register-schema :sprint  scc:Sprint-schema)
;(tbox-register-schema :story   scc:Story-schema)
;(tbox-register-schema :task    scc:Task-schema)


;; Triple spaces definition
(def-ts :usrs (make-basic-triple-space))


(defonce *server* "http://localhost:8081")

;; ;; Application routes
;; (defroutes borges

;;   (spawn-rest-resource! :user-account "/api/v1/user_account/:id" :users)
;;   (spawn-rest-collection-resource! :user-account "/api/v1/user_account" :users)

;;   ;; Roles
;;   (spawn-rest-resource! :role "/api/v1/roles/:id" :admin)
;;   (spawn-rest-collection-resource! :role "/api/v1/roles" :admin))

;;   ;; Static files
;; ;;   (route/files "/images/*" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})
;; ;;   (route/files "/css/*" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})
;; ;;   (route/files "/js/*" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})

;; ;;   ; This should lead to the index
;; ;;   (route/files "/" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})

;; ;;   ; Project canvas
;; ;;   (GET  "/:name" request (file-response "/Users/antonio.garrote/Development/old/scrummy-clone/client/project.html")))


;; ;; Server start
;; (run-jetty (var borges) {:port 8081})
