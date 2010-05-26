(ns plaza.core
  (:use
   [plaza.triple-spaces server]
   [plaza.triple-spaces core]
   [plaza.rdf core]
   [plaza utils]
   [plaza.rdf.implementations jena]
   [plaza.examples chat])
  (:gen-class :main true))

(defn cmd-params-to-hash
  ([args]
     (apply hash-map (map cmd-param-to-keyword args))))

(defn start-triple-space [& args]
  (init-jena-framework)
  (let [name (first args)
        port (Integer/parseInt (second args))
        args (apply array-map (cmd-params-to-hash (rest (rest args))))]
    (apply plaza.triple-spaces.server/start-triple-server (cons name (cons port (cons (build-model :jena) args)) ))))

(defn start-example [example-name & args]
  (init-jena-framework)
  (condp = example-name
    "chat" (apply start-app (list (first args) (cmd-params-to-hash (rest args))))))


(defn process-command [command & args]
  (condp = command
    "start-triple-space" (apply start-triple-space args)
    "examples" (apply start-example args)
    :else    (println (str "unknown command" command))))

(defn show-help []
  (println "plaza-standalone syntax: java -jar plaza-standalone.jar COMMAND [ARGS]")
  (println "COMMAND: start-triple-space name port [-username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]")
  (println "COMMAND: examples chat nick [-ts-host host -ts-port port -username rabbit-username -password rabbit-password -host rabbit-host -port rabbit-port -virtual-host rabbit-vh]"))

(defn -main
  [& args]
  (println (str "hello from Plaza, ARGS:" args))
  (if (empty? args)
    (show-help)
    (apply process-command args)))
