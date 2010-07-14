;; @author Antonio Garrote
;; @date  30.06.2010

(ns scrummy-clone.core
  (:use [compojure core]
        [compojure response]
        [ring.adapter jetty]
        [ring.util.response :only (file-response)]
        [plaza.rdf core schemas]
        [plaza.rdf.implementations jena]
        [plaza.rdf.vocabularies plaza]
        [plaza.rest core utils]
        [plaza.triple-spaces core]
        [scrummy-clone.resources common]
        [scrummy-clone utils])
  (:require [compojure.route :as route]
            [clojure.contrib.str-utils2 :as str2]))


;; We are using Jena as the framework's backend
(init-jena-framework)
(init-vocabularies)

;; We register the resources in the TBox
(tbox-register-schema :project scc:Project-schema)
(tbox-register-schema :sprint  scc:Sprint-schema)
(tbox-register-schema :story   scc:Story-schema)
(tbox-register-schema :task    scc:Task-schema)


;; Triple spaces definition
(def-ts :projects (make-basic-triple-space))


(defonce *server* "http://localhost:8081")

;; Application routes
(defroutes scrummy-clone

  ;; Resources
  (spawn-rest-resource! :project"/projects/:id" :projects)

  (spawn-rest-collection-resource! :project "/projects" :projects
                                   :id-gen-fn (fn [req env]
                                                (let [prefix (:resource-qname-prefix env)
                                                      local  (:resource-qname-local env)
                                                      id (beautify (get (:params req) "title"))]
                                                  {:id id :uri (str *server* "/projects/" id)})))

  (spawn-rest-resource! :sprint "/sprints/:id" :projects)

  (spawn-rest-collection-resource! :sprint "/sprints" :projects)

  (spawn-rest-resource! :story "/stories/:id" :projects)

  (spawn-rest-collection-resource! :story "/stories" :projects)

  (spawn-rest-resource! :task "/tasks/:id" :projects)

  (spawn-rest-collection-resource! :task "/tasks" :projects)

  ;; Static files
  (route/files "/images/*" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})
  (route/files "/css/*" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})
  (route/files "/js/*" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})

  ; This should lead to the index
  (route/files "/" {:root "/Users/antonio.garrote/Development/old/scrummy-clone/client"})

  ; Project canvas
  (GET  "/:name" request (file-response "/Users/antonio.garrote/Development/old/scrummy-clone/client/project.html")))


;; Server start
(run-jetty (var scrummy-clone) {:port 8081})
