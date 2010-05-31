;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 28.05.2010

(ns plaza.triple-spaces.server.4store
  (:use [saturnine]
	[saturnine.handler]
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [clojure.contrib.logging :only [log]]
        (plaza.triple-spaces core)
        (plaza.rdf core sparql)
        (plaza.rdf.implementations sesame))
  (:import [java.util UUID]
           [java.net URL]))


(defmethod build-model [:4store]
  ([& options] (let [opts (apply hash-map (rest options))
                     _test (println (str "URL:" (:url opts) " read from " opts))
                     repo (com.clarkparsia.fourstore.sesame.FourStoreSailRepository. (URL. (:url opts)))]
                 (.initialize repo)
                 (plaza.rdf.implementations.sesame.SesameModel. repo))))
