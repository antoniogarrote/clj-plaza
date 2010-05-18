;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 16.05.2010

(ns plaza.triple-spaces.server
  (:use [saturnine]
	[saturnine.handler]
        [org.clojars.rabbitmq :as rabbit]
        [clojure.contrib.logging :only [log]]
        (plaza.triple-spaces core)
        (plaza.triple-spaces.server auxiliary)
        (plaza.rdf core sparql)
        (plaza.rdf.implementations jena)))

;; temporary
(init-jena-framework)


; handlers

(defhandler #^{:doc
               "The MessageParser handler for parsing incoming messages"}
  MessageParser [model queues rabbit-conn rabbit-chn options]
  (connect  [_] (println "Connecting"))
  (upstream [this msg] (let [parsed (parse-message msg)]
                         (log :info (str "parsed: " parsed "\r\n\r\n"))
                         (send-down (apply-operation parsed model queues rabbit-conn rabbit-chn options))))
  (error [this msg]
         (log :error msg)
         (failure msg)))



;; servers

(defn start-triple-server
  "Sum is a simple server that reads a stream of newline-delimited Integers and
responds with the cumulative sum. It is constructed of 3 handlers:

:string - emits strings at every new line
:clj - calls read & eval on each string, making \"1\" -> 1, for example.
Sum - The custom handler contains the cumulative sum state so far for a
single connection. Non-integers that are evalable will throw a
ClassCastException to the default handle, while unevalable ones
will display a parse error in the :clj handler as well"
  [port model & options]
  (let [opt-map (apply array-map options)
        [rabbit-conn rabbit-chn] (rabbit/connect opt-map)]
    (start-server port :string :print (MessageParser. model (ref []) rabbit-conn rabbit-chn options))))

;; client

;; client handlers


(defhandler #^{:doc
               "The MessageParser handler for parsing incoming messages"}
  DeliverInRef [mailbox]

  (downstream [this msg] (do (log :info (str "DOWNSTREAM " msg)) (assoc this :mailbox (fn [_] msg))))
  (error [this msg] (println (str "ERROR: " msg))))


;; (defn start-triple-server-client
;;   [host port ref]
;;   (let [c (start-client :blocking :string :print)]
;;     [c (open c host port)]))


(defn start-triple-server-client
  [host port]
  (let [s (java.net.Socket. host port)
        out (java.io.PrintWriter. (.getOutputStream s))
        in (java.io.BufferedReader. (java.io.InputStreamReader. (.getInputStream s)))]
    {:socket s :out out :in in}))


(defn send-rd-operation
  ([channel client-id pattern-or-vector filters]
     (let [pattern (if (:pattern (meta pattern-or-vector)) pattern-or-vector (make-pattern pattern-or-vector))
           vars (pattern-collect-vars pattern)
           query (if (empty? filters)
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars))
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars)
                     (query-set-filters filters)))
           query-string (query-to-string query)]
       (log :info (str "*** client rd : " query-string))
       (.write channel (format-message "rd" client-id query-string ""))
       (.flush channel))))

(defn send-rdb-operation
  ([channel client-id pattern-or-vector filters]
     (let [pattern (if (:pattern (meta pattern-or-vector)) pattern-or-vector (make-pattern pattern-or-vector))
           vars (pattern-collect-vars pattern)
           query (if (empty? filters)
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars))
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars)
                     (query-set-filters filters)))
           query-string (query-to-string query)]
       (log :info (str "*** client rdb : " query-string))
       (.write channel (format-message "rdb" client-id query-string ""))
       (.flush channel))))


(defn send-out-operation
  ([channel client-id triples-or-vector filters]
     (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))
           m (defmodel (model-add-triples triples))
           w (java.io.StringWriter.)
           rdf (do (output-string m w :xml) (.toString w))]
       (log :info (str "*** client out : " rdf))
       (.write channel (format-message "out" client-id "" rdf))
       (.flush channel))))

(defn send-in-operation
  ([channel client-id pattern-or-vector filters]
     (let [pattern (if (:pattern (meta pattern-or-vector)) pattern-or-vector (make-pattern pattern-or-vector))
           vars (pattern-collect-vars pattern)
           query (if (empty? filters)
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars))
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars)
                     (query-set-filters filters)))
           query-string (query-to-string query)]
       (log :info (str "*** client in : " query-string))
       (.write channel (format-message "in" client-id query-string ""))
       (.flush channel))))

(defn send-inb-operation
  ([channel client-id pattern-or-vector filters]
     (let [pattern (if (:pattern (meta pattern-or-vector)) pattern-or-vector (make-pattern pattern-or-vector))
           vars (pattern-collect-vars pattern)
           query (if (empty? filters)
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars))
                   (defquery
                     (query-set-pattern pattern)
                     (query-set-type :select)
                     (query-set-vars vars)
                     (query-set-filters filters)))
           query-string (query-to-string query)]
       (log :info (str "*** client nb : " query-string))
       (.write channel (format-message "inb" client-id query-string ""))
       (.flush channel))))

(deftype RemoteTripleSpace [connected-client rabbit-conn rabbit-chn options] TripleSpace
  (rd [this pattern] (rd this pattern []))
  (rd [this pattern filters] (do (send-rd-operation (:out connected-client) "test" pattern filters)
                                 (loop [ready (.ready (:in connected-client))]
                                   (if-not ready
                                     (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                 (parse-rd-response (:in connected-client))))
  (rdb [this pattern] (rdb this pattern []))
  (rdb [this pattern filters] (do (send-rdb-operation (:out connected-client) (:client-id options) pattern filters)
                                  (loop [ready (.ready (:in connected-client))]
                                    (if-not ready
                                      (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                  (parse-rdb-response (:in connected-client) rabbit-chn options)))
  (in [this pattern] (in this pattern []))
  (in [this pattern filters] (do (send-in-operation (:out connected-client) "test" pattern filters)
                                 (loop [ready (.ready (:in connected-client))]
                                   (if-not ready
                                     (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                 (parse-rd-response (:in connected-client))))
  (inb [this pattern] (inb this pattern []))
  (inb [this pattern filters] (do (send-inb-operation (:out connected-client) (:client-id options) pattern filters)
                                  (loop [ready (.ready (:in connected-client))]
                                    (if-not ready
                                      (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                  (parse-rdb-response (:in connected-client) rabbit-chn options)))
  (out [this triples] (do (send-out-operation (:out connected-client) "test" triples [])
                          (loop [ready (.ready (:in connected-client))]
                            (if-not ready
                              (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                        (let [result (parse-out-response (:in connected-client))]
                          (if (= :success result)
                            triples
                            (throw (Exception. (str "Error in out operation with client " connected-client)))))))
  (swap [this pattern triples] :not-yet)
  (swap [this pattern triples filters] :not-yet)
  (notify [this op pattern f] :not-yet)
  (notify [this op pattern filters f] :not-yet)
  (inspect [this] :not-yet))


(defn make-remote-triple-space
  "Creates a new remote triple space located at the provided host and port"
  ([& options]
     (let [opt-map (apply array-map options)
           [rabbit-conn rabbit-chn] (rabbit/connect opt-map)]
       (rabbit/bind-channel opt-map rabbit-chn)
       (plaza.triple-spaces.server.RemoteTripleSpace. (start-triple-server-client (:ts-host opt-map) (:ts-port opt-map))
                                                      rabbit-conn
                                                      rabbit-chn
                                                      (assoc opt-map :client-id (str (:exchange opt-map) ":" (:routing-key opt-map)))))))
