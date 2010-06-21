;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 19.06.2010

(ns plaza.rdf.implementations.stores.agraph
  (:use (plaza.rdf core)
        (plaza.rdf.implementations jena))
  (:import [com.franz.agraph.repository AGServer]
           [com.franz.agraph.jena AGGraphMaker AGModel]))

;; Allegro Graph client java library and json.org JSON library required to use
;; these models

(defmethod build-model [:agraph]
  ([& opts]
     (let [options (apply hash-map (rest opts))
           agserver (AGServer. (:server-url options) (:agraph-user options) (:agraph-password options))
           catalog (.getCatalog agserver (:catalog options))
           repo (.createRepository catalog (:repository options))]
       (do (.initialize repo)
           (let [conn (.getConnection repo)
                 gm (AGGraphMaker. conn)
                 graph (.getGraph gm)
                 model (AGModel. graph)]
             (plaza.rdf.implementations.jena.JenaModel. model))))))

;;
;;  Sample use:
;;
;(build-model :agraph :server-url "http://192.168.1.37:10035" :agraph-user "test" :agraph-password "test" :catalog "java-catalog" :repository "test2")
