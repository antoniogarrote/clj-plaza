;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 28.05.2010

(ns plaza.rdf.implementations.stores.4store
  (:use (plaza.rdf core sparql)
        (plaza.rdf.implementations sesame))
  (:import [java.util UUID]
           [java.net URL]))


(defmethod build-model [:4store]
  ([& options] (let [opts (apply hash-map (rest options))
                     repo (com.clarkparsia.fourstore.sesame.FourStoreSailRepository. (URL. (:url opts)))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))
