;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 28.05.2010

(ns plaza.triple-spaces.multi-remote-server
  (:use [saturnine]
	[saturnine.handler]
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [clojure.contrib.logging :only [log]]
        (plaza.triple-spaces core server)
        (plaza.triple-spaces.distributed-server auxiliary)
        (plaza.rdf core sparql)
        (plaza.rdf.implementations sesame))
  (:import [java.util UUID]
           [java.net URL]))



;;
;; MultiRemoteTripleSpace
;;

(defhandler #^{:doc
               "The MessageParser handler for parsing incoming messages"}
  MessageParser [name model redis-conn rabbit-conn options]
  (connect  [_] (println "Connecting"))
  (upstream [this msg] (let [parsed (plaza.triple-spaces.server.auxiliary/parse-message msg)]
                         (log :info (str "*** parsed: " parsed "\r\n\r\n"))
                         (log :info (str "*** " parsed " " name " " model " " redis-conn " " rabbit-conn " " options))
                         (send-down (apply-operation-dist parsed name model redis-conn rabbit-conn options))))
  (error [this msg]
         (log :error msg)
         (plaza.triple-spaces.server.auxiliary/failure msg)))


(defn start-multi-remote-triple-server
  "Starts a distributd triple server using rabbitmq, redis and a triple store as infrastructure"
  [name port model & options]
  (let [opt-map (apply array-map options)
        rabbit-conn (apply rabbit/connect options)
        redis-conn (build-redis-connection-hash opt-map)]

    ;; Declaring channels and exchanges for this triple space
    (make-channel rabbit-conn name)
    (declare-exchange rabbit-conn name (str "exchange-" name))
    (declare-exchange rabbit-conn name (str "exchange-out-" name))
    (declare-exchange rabbit-conn name (str "exchange-in-" name))

    ;; start the server
    (start-server port :string :print (MessageParser. name model redis-conn rabbit-conn opt-map))))
