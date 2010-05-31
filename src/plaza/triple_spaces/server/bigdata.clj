;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 29.05.2010

(ns plaza.triple-spaces.bigdata
  (:use [saturnine]
	[saturnine.handler]
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [clojure.contrib.logging :only [log]]
        (plaza.triple-spaces core)
        (plaza.rdf core sparql)
        (plaza.rdf.implementations sesame))
  (:import [java.util UUID]
           [java.net URL]))

(defmethod build-model [:bigdata]
  ([& options] (let [opts (apply hash-map (rest options))
		     _test (println (str "fil:" (:file opts) " read from " opts " and props " (:properties opts)))
		     config (let [props (java.util.Properties.)]
			      (.load props (. (. ClassLoader (getSystemResource (:properties opts))) openStream)) props)
		     args (vec (.split (:file opts) "\\."))
		     journal (java.io.File/createTempFile (nth args 0) (nth args 1))]
		 (do (.setProperty config com.bigdata.rdf.sail.BigdataSail$Options/FILE (.getAbsolutePath journal))
		     (let [repo (com.bigdata.rdf.sail.BigdataSailRepository. (com.bigdata.rdf.sail.BigdataSail. config))]
		       (.initialize repo)
		       (plaza.rdf.implementations.sesame.SesameModel. repo))))))
