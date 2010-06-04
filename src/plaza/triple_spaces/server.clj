;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 16.05.2010

(ns plaza.triple-spaces.server
  (:use [saturnine]
	[saturnine.handler]
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [clojure.contrib.logging :only [log]]
        (plaza.triple-spaces core)
        (plaza.triple-spaces.server auxiliary)
        (plaza.rdf core sparql)
        (plaza.rdf.implementations jena))
  (:import [java.util UUID]))

;; temporary
(init-jena-framework)


; handlers

(defhandler #^{:doc
               "The MessageParser handler for parsing incoming messages"}
  MessageParser [name model queues rabbit-conn options]
  (connect  [_] (println "Connecting"))
  (upstream [this msg] (let [parsed (parse-message msg)]
                         (log :info (str "*** parsed: " parsed "\r\n\r\n"))
                         (log :info (str "*** " parsed " " name " " model " " queues " " rabbit-conn " " options))
                         (send-down (apply-operation parsed name model queues rabbit-conn options))))
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
  [name port model & options]
  (let [opt-map (apply array-map options)
        rabbit-conn (apply rabbit/connect options)]

    ;; Declaring channels and exchanges for this triple space
    (make-channel rabbit-conn name)
    (declare-exchange rabbit-conn name (str "exchange-" name))
    (declare-exchange rabbit-conn name (str "exchange-out-" name))
    (declare-exchange rabbit-conn name (str "exchange-in-" name))

    ;; start the server
    (start-server port :string :print (MessageParser. name model (ref []) rabbit-conn opt-map))))

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
  ([channel client-id pattern-or-vector filters-pre]
     (let [pattern-pre (if (:pattern (meta pattern-or-vector)) pattern-or-vector (make-pattern pattern-or-vector))
           vars-pre (pattern-collect-vars pattern-pre)
           vars (if-not (empty? vars-pre) vars-pre [:p])
           [pattern filters] (if-not (empty? vars-pre)
                               [pattern-pre filters-pre]
                               (let [s (nth (first pattern-pre) 0)
                                     p (nth (first pattern-pre) 1)
                                     o (nth (first pattern-pre) 2)]
                                 [(cons [s ?p o] (rest pattern-pre))
                                  (cons (f :sameTerm  ?p p) filters-pre)]))
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
       (log :info (str "*** client rd \r\n: " query-string))
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
       (log :info (str "*** client rdb \r\n: " query-string))
       (.write channel (format-message "rdb" client-id query-string ""))
       (.flush channel))))

(defn send-out-operation
  ([channel client-id triples-or-vector filters]
     (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))
           m (defmodel (model-add-triples triples))
           w (java.io.StringWriter.)
           rdf (do (output-string m w :xml) (.toString w))]
       (log :info (str "*** client out \r\n: " rdf))
       (.write channel (format-message "out" client-id "" rdf))
       (.flush channel)
       rdf)))

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
       (log :info (str "*** client in \r\n: " query-string))
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
       (log :info (str "*** client inb \r\n: " query-string))
       (.write channel (format-message "inb" client-id query-string ""))
       (.flush channel))))

(defn send-swap-operation
  ([channel client-id pattern-or-vector triples-or-vector filters]
     (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))
           pattern (if (:pattern (meta pattern-or-vector)) pattern-or-vector (make-pattern pattern-or-vector))
           m (defmodel (model-add-triples triples))
           w (java.io.StringWriter.)
           rdf (do (output-string m w :xml) (.toString w))
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
       (log :info (str "*** client swap \r\n: " query " \r\n" rdf))
       (.write channel (format-message "swap" client-id query-string rdf))
       (.flush channel))))

;;
;; RemoteTripleSpace
;;
(deftype RemoteTripleSpace [name connected-client rabbit-conn options] TripleSpace

  ;; rd operation
  (rd [this pattern] (rd this pattern []))

  (rd [this pattern filters] (do (send-rd-operation (:out connected-client) (:client-id options) pattern filters)
                                 (loop [ready (.ready (:in connected-client))]
                                   (if-not ready
                                     (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                 (parse-rd-response (:in connected-client) name false rabbit-conn)))

  ;; rdb operation
  (rdb [this pattern] (rdb this pattern []))

  (rdb [this pattern filters] (do (send-rdb-operation (:out connected-client) (:client-id options) pattern filters)
                                  (loop [ready (.ready (:in connected-client))]
                                    (if-not ready
                                      (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                  (let [prom (promise)]
                                    (parse-rdb-response (:in connected-client) name false rabbit-conn options prom)
                                    @prom)))


  ;; in operation
  (in [this pattern] (in this pattern []))

  (in [this pattern filters] (do (send-in-operation (:out connected-client) (:client-id options) pattern filters)
                                 (loop [ready (.ready (:in connected-client))]
                                   (if-not ready
                                     (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                 (parse-rd-response (:in connected-client) name true rabbit-conn)))


  ;; inb operation
  (inb [this pattern] (inb this pattern []))

  (inb [this pattern filters] (do (send-inb-operation (:out connected-client) (:client-id options) pattern filters)
                                  (loop [ready (.ready (:in connected-client))]
                                    (if-not ready
                                      (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                  (let [prom (promise)]
                                    (parse-rdb-response (:in connected-client) name true rabbit-conn options prom)
                                    @prom)))


  ;; out operation
  (out [this triples] (let [rdf (send-out-operation (:out connected-client) (:client-id options) triples [])]
                        (loop [ready (.ready (:in connected-client))]
                          (if-not ready
                            (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                        (let [result (parse-out-response (:in connected-client))]
                          (if (= :success result)
                            (do
                              (log :info "*** delivering notify out")
                              (deliver-notify rabbit-conn name "out" (pack-stanzas [rdf]))
                              triples)
                            (throw (Exception. (str "Error in out operation with client " connected-client)))))))


  ;; swap operation
  (swap [this pattern triples] (swap this pattern triples []))

  (swap [this pattern triples filters] (do (send-swap-operation (:out connected-client) (:client-id options) pattern triples filters)
                                           (loop [ready (.ready (:in connected-client))]
                                             (if-not ready
                                               (do (Thread/sleep 100) (recur (.ready (:in connected-client))))))
                                           (let [prom (promise)]
                                             (parse-rdb-response (:in connected-client) name true rabbit-conn options prom)
                                             @prom)))


  ;; notify operation
  (notify [this op pattern f] (notify this op pattern [] f))

  (notify [this op pattern filters f]
          (parse-notify-response name pattern  filters f (plaza.utils/keyword-to-string op) rabbit-conn options))


  ;; inspect
  (inspect [this] :not-yet)

  ;; clean
  (clean [this] (try (.close (:socket connected-client))
                     (catch Exception ex ""))
         (try (.close rabbit-conn)
              (catch Exception ex ""))))

(defn make-remote-triple-space
  "Creates a new remote triple space located at the provided host and port"
  ([name & options]
     (let [registry (ref {})]
       (fn []
         (let [id (plaza.utils/thread-id)
               ts (get @registry id)]
           (if (nil? ts)
             (let [uuid (.toString (UUID/randomUUID))
                   opt-map (-> (apply array-map options)
                               (assoc :routing-key uuid)
                               (assoc :queue (str "box-" uuid)))
                   rabbit-conn (apply rabbit/connect opt-map)]

               ;; Creating a channel and declaring exchanges
               (rabbit/make-channel rabbit-conn name)
               (rabbit/declare-exchange rabbit-conn name (str "exchange-" name))
               (rabbit/declare-exchange rabbit-conn name (str "exchange-out-" name))
               (rabbit/declare-exchange rabbit-conn name (str "exchange-in-" name))

               ;; Creating queues and bindings
               (rabbit/make-queue rabbit-conn name (str "queue-client-" uuid) (str "exchange-" name) uuid)

               ;; starting the server
               (let [tsp (plaza.triple-spaces.server.RemoteTripleSpace. name
                                                                        (start-triple-server-client (:ts-host opt-map) (:ts-port opt-map))
                                                                        rabbit-conn
                                                                        (assoc opt-map :client-id uuid))]
                 (dosync (alter registry (fn [old] (assoc old id tsp))))
                 tsp))
             ts))))))
