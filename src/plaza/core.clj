(ns plaza.core
  (:use
   [plaza.triple-spaces server]
   [plaza.triple-spaces core]
   [plaza.rdf core]
   [plaza.rdf.implementations jena])
  (:gen-class :main true))


(defn start-triple-space [& args]

  (init-jena-framework)

  (let [name (first args)
        port (Integer/parseInt (second args))
        args (apply array-map (rest (rest args)))]
    (apply plaza.triple-spaces.server/start-triple-server (cons name (cons port (cons (build-model :jena) args)) ))))


(defn process-command [command & args]
  (condp = command
    "start-triple-space" (apply start-triple-space args)
    :else    (println (str "unknown command" command))))

(defn show-help []
  (println "plaza-standalone syntax: java -jar plaza-standalone.jar COMMAND [ARGS]")
  (println "COMMAND: start-triple-space name port [:username rabbit-username :password rabbit-password :host rabbit-host :port rabbit-port :virtual-host rabbit-vh]"))

(defn -main
  [& args]
  (println (str "hello from Plaza, ARGS:" args))
  (if (empty? args)
    (show-help)
    (apply process-command args)))
