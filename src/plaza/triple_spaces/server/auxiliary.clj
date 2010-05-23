;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 16.05.2010

(ns plaza.triple-spaces.server.auxiliary
  (:use (plaza.rdf core sparql)
        (plaza utils)
        (plaza.rdf.implementations jena)
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.string :as string])
  (:import [java.util UUID]
           [com.rabbitmq.client
            ConnectionParameters
            Connection
            Channel
            AMQP
            ConnectionFactory
            Consumer
            QueueingConsumer]))

(defn parse-message
  "Parses a message sent to the server"
  ([msg-str]
     (let [parts (string/split #"\r\n\r\n" msg-str)]
       {:operation (nth parts 0)
        :client-id (nth parts 1)
        :pattern   (nth parts 2)
        :value     (if (= (count parts) 4) (nth parts 3) "")})))

(defn format-message
  "Encodes a new message"
  ([op client-id pattern value]
     (format-message {:operation op :client-id client-id :pattern pattern :value value}))
  ([msg]
     (str (:operation msg) "\r\n\r\n" (:client-id msg) "\r\n\r\n" (:pattern msg) "\r\n\r\n" (:value msg))))

(defn response
  "reates a response to be send down to the client"
  ([payload]
     (str "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>" payload "</ts:response>")))

(defn blocking-response
  "reates a response to be send down to the client"
  ([] (str "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>blocking</ts:response>")))

(defn success
  "reates a response to be send down to the client"
  ([payload]
     (str "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>" payload "</ts:response>"))
  ([]
     (str "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/></ts:response>")))

(defn failure
  "reates a response to be send down to the client"
  ([payload]
     (str "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>failure<ts:tokensep/>" payload "</ts:response>"))
  ([]
     (str "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>failure<ts:tokensep/></ts:response>")))

(defn pack-stanzas
  "pack a set of stanzas in a message interleaving them with token separators"
  ([stanzas]
     (reduce (fn [a i] (str a i)) "" (reverse (rest (reverse (reduce (fn [a i] (conj (conj a i) "<ts:tokensep/>")) [] stanzas)))))))

;; (defn serialize-binding-map
;;   "Transforms a binding map into a string that can be later deserialized"
;;   ([bindings]
;;      (let [w (java.io.StringWriter.)]
;;        (reduce (fn [w vars]
;;                  (.write w "result\r\n\r\n")
;;                  (reduce (fn [w v]
;;                            (let [value (get vars v)]
;;                              (.write w (str (keyword-to-string v) "\r\n"))
;;                              (cond (is-blank value) (do (.write w (str ":blank:" (resource-id value) "\r\n")))
;;                                    (is-resource value) (do (.write w (str ":uri:" (resource-id value) "\r\n")))
;;                                    (is-literal value) (do (.write w (str ":literal:" (resource-id value)"\r\n"))))
;;                              w))
;;                          w
;;                          (keys vars))
;;                  w)
;;                w
;;                bindings)
;;        (.toString w))))

;; Server Operations

(defn deliver-notify
  ([rabbit-conn name direction value]
     (log :info (str "*** Delivering with " rabbit-conn " " direction " " value))
     (rabbit/publish rabbit-conn name (str "exchange-" direction "-" name) "msgs" value)))

(defn- apply-rd-operation
  "Applies a TS rd operation over a model"
  ([pattern model]
     (let [w (java.io.StringWriter.)]
       (response
        (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                             (output-string m w :xml)
                             (.write w "<ts:tokensep/>") w))
                w (query-triples model pattern))))))

(defn- apply-rdb-operation
  "Applies a TS rd operation over a model"
  ([pattern model queues client-id]
     (let [w (java.io.StringWriter.)
           results (query-triples model pattern)]
       (if (empty? results)
         (dosync (alter queues (fn [old] (conj old [pattern :rdb client-id])))
                 (blocking-response))
         (response
          (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                               (output-string m w :xml)
                               (.write w "\r\n\r\n") w))
                  w results))))))

(defn- apply-out-operation
  "Applies a TS rd operation over a model"
  ([rdf-document model name rabbit-conn queues-ref]
     (with-model model (document-to-model (java.io.ByteArrayInputStream. (.getBytes rdf-document)) :xml))
     (model-critical-write model
                           (dosync
                            (loop [queues @queues-ref
                                   queuesp []]
                              (if (empty? queues)
                                (alter queues-ref (fn [old] queuesp))
                                (let [[pattern kind-op client-id] (first queues)
                                      results (query-triples model pattern)]
                                  (log :info (str "*** checking queued " kind-op " -> " pattern " ? " (empty? results)))
                                  (if (empty? results)
                                    (recur (rest queues)
                                           (conj queuesp (first queues)))
                                    (let [w (java.io.StringWriter.)
                                          respo (response
                                                 (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                      (output-string m w :xml)
                                                                      (.write w "\r\n\r\n") w))
                                                         w results))]
                                      (when (= kind-op :inb)
                                        (with-model model (model-remove-triples (flatten-1 results))))
                                      (log :info (str "*** queue to blocked client: \r\n" respo))
                                      (rabbit/publish rabbit-conn name (str "exchange-" name) client-id respo)
                                      (recur (rest queues)
                                             queuesp))))))
                            (success)))))

(defn- apply-in-operation
  "Applies a TS in operation over a model"
  ([pattern model name]
     (model-critical-write model
                           (let [w (java.io.StringWriter.)
                                 triples-to-remove (query-triples model pattern)
                                 flattened-triples (flatten-1 triples-to-remove)]
                             ;; deleting read triples
                             (with-model model (model-remove-triples flattened-triples))
                             ;; returning triple sets
                             (let [triples (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                (output-string m w :xml)
                                                                (.write w "\r\n\r\n") w))
                                                   w triples-to-remove)]
                               (response triples))))))

(defn- apply-inb-operation
  "Applies a TS inb operation over a model"
  ([pattern model name rabbit-conn queues client-id]
     (model-critical-write model
                           (let [w (java.io.StringWriter.)
                                 triples-to-remove (query-triples model pattern)]
                             (if (empty? triples-to-remove)
                               (dosync (log :info "*** Storing INB operation in the queue")
                                       (alter queues (fn [old] (conj old [pattern :inb client-id])))
                                       (blocking-response))
                               (do
                                 ;; deleting read triples
                                 (with-model model (model-remove-triples (flatten-1 triples-to-remove)))
                                 ;; returning triple sets
                                 (let [triples (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                    (output-string m w :xml)
                                                                    (.write w "\r\n\r\n") w))
                                                       w triples-to-remove)]
                                   (response triples))))))))

(defn apply-swap-operation
  "Applies a TS swap operation over a model"
  ([pattern rdf-document model name rabbit-conn queues-ref]
     (model-critical-write model
                           (let [w (java.io.StringWriter.)
                                 triples-to-remove (query-triples model pattern)
                                 flattened-triples (if (empty? triples-to-remove) [] (flatten-1 triples-to-remove))]
                             (when-not (empty? triples-to-remove)
                               (with-model model
                                 (model-remove-triples flattened-triples)
                                 (document-to-model (java.io.ByteArrayInputStream. (.getBytes rdf-document)) :xml))

                               ;; Processing queues
                               (dosync
                                (loop [queues @queues-ref
                                       queuesp []]
                                  (if (empty? queues)
                                    (alter queues-ref (fn [old] queuesp))
                                    (let [[pattern kind-op client-id] (first queues)
                                          results (query-triples model pattern)]
                                      (log :info (str "checking queued " kind-op " -> " pattern " ? " (empty? results)))
                                      (if (empty? results)
                                        (recur (rest queues)
                                               (conj queuesp (first queues)))
                                        (let [w (java.io.StringWriter.)
                                              respo (response
                                                     (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                          (output-string m w :xml)
                                                                          (.write w "\r\n\r\n") w))
                                                             w results))]
                                          (when (= kind-op :inb)
                                            (with-model model (model-remove-triples (flatten-1 results))))
                                          (rabbit/publish rabbit-conn name (str "exchange-" name) client-id respo)
                                          (recur (rest queues)
                                                 queuesp))))))))
                             (response
                              (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                   (output-string m w :xml)
                                                   (.write w "\r\n\r\n") w))
                                      w triples-to-remove))))))

(defn apply-operation
  "Applies an operation over a Plaza model"
  ([message name model queues rabbit-conn options]
     (log :info "*** about to apply operation")
     (condp = (:operation message)
       "rd" (apply-rd-operation (:pattern message) model)
       "rdb" (apply-rdb-operation (:pattern message) model queues (:client-id message))
       "out" (apply-out-operation (:value message) model name rabbit-conn queues)
       "in" (apply-in-operation (:pattern message) model name)
       "inb" (apply-inb-operation (:pattern message) model name rabbit-conn queues (:client-id message))
       "swap" (apply-swap-operation (:pattern message) (:value message) model name rabbit-conn queues)
       (throw (Exception. "Unsupported operation")))))

;; Parsers

(defmacro if-not-eos
  ([writer & body]
     `(if (.ready ~writer)
        (do ~@body)
        (throw (Exception. "Reached eof")))))

(defn buffer-ready?
  ([in]
     (and (.ready in)
          (do (.mark in 1)
              (if (= 1 (.read in (char-array 1) 0 1))
                (do (.reset in) true)
                false)))))

(defn read-from-buffer
  ([in len]
     (let [buffer (char-array len)]
       (do (.mark in len)
           (.read in buffer 0 len)
           (String. buffer)))))

(defn read-eos
  ([in]
     (if (not (buffer-ready? in))
       in
       (let [line (read-from-buffer in 1024)]
         (recur in)))))


(defn read-ts-response
  ([in]
     (if-not-eos in
                 (let [line (read-from-buffer in 52)]
                   (when (not (= line "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\">"))
                     (do
                       (.reset in)
                       (throw (Exception. (str "error parsing response: got '" line "' but '<ts:response xmlns:ts=\"http://plaza.org/tsresponse\">' was expected")))))
                   in))))

(defn read-blank-line [in] :foo)

(defn read-token-separator
  ([in]
     (if-not-eos in
                 (let [line (read-from-buffer in 14)]
                   (when (not (= line "<ts:tokensep/>"))
                     (.reset in)
                     (throw (Exception. (str "error parsing response: got '" line "' but token separator was expected"))))
                   in))))

(defn read-success
  ([in]
     (if-not-eos in
                 (let [line (read-from-buffer in 7)]
                   (when (not (= line "success"))
                     (.reset in)
                     (throw (Exception. (str "error parsing response: got '" line "' but 'success' was expected"))))
                   in))))

(defn return-succes-failure
  ([in]
     (if-not-eos in
                 (let [line (read-from-buffer in 7)]
                   (if (or (= line "success")
                           (= line "failure"))
                     line
                     (let [line (do (.reset in) (read-from-buffer in 8))]
                       (if (= line "blocking")
                         line
                         (do (.reset in)
                             (throw (Exception. (str "error parsing response: got <" line "> but <success|failure> expected")))))))))))

(defn- return-many
  ([parser in]
     (if-not-eos in
                 (loop [should-continue true
                        acum []]
                   (try
                    (let [result (parser in)]
                      (recur true (conj acum result)))
                    (catch Exception ex
                      acum))))))

(defn return-rdf-stanzas
  ([in]
     (if-not-eos in
                 (let [result (loop [writer (java.io.StringWriter.)]
                                (if (not (buffer-ready? in))
                                  (.toString writer)
                                  (do (.write writer (read-from-buffer in 1024))
                                      (recur writer))))
                       clean-result (first (seq (.split result "</ts:response>")))]
                   (filter #(not (= "" %1)) (seq (.split clean-result "<ts:tokensep/>")))))))

(defn parse-out-response
  "Process a response from the triple space to a previously requested OUT operation"
  ([in]
     (let [result (-> in
                      (read-ts-response)
                      (read-token-separator)
                      (return-succes-failure))]
       (read-eos in)
       (keyword result))))

(defn parse-rd-response
  "Process a response from the triple space to a previously requested RD operation"
  ([in name should-deliver rabbit-conn]
     (let [result (-> in
                      (read-ts-response)
                      (read-token-separator)
                      (read-success)
                      (read-token-separator)
                      (return-rdf-stanzas))]
       (read-eos in)
       (when should-deliver (deliver-notify rabbit-conn name "in" (pack-stanzas result)))
       (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result))))

(defn parse-rdb-response
  "Process a response from the triple space to a previously requested RDB operation"
  ([in name should-deliver rabbit-conn options prom]
     (let [should-block (-> in
                            (read-ts-response)
                            (read-token-separator)
                            (return-succes-failure))]
       (if (= should-block "blocking")
         (do (read-eos in)
             (log :info "about to block...")
             (let [read (rabbit/consume-n-messages rabbit-conn name (str "queue-client-" (:client-id options)) 1)
                   result (-> (java.io.BufferedReader. (java.io.StringReader. (first read)))
                              (read-ts-response)
                              (read-token-separator)
                              (read-success)
                              (read-token-separator)
                              (return-rdf-stanzas))]
               (when should-deliver (deliver-notify rabbit-conn name "in" (pack-stanzas result)))
               (deliver prom (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result))))
         (let [result (-> in
                          (read-token-separator)
                          (return-rdf-stanzas))]
           (read-eos in)
           (when should-deliver (deliver-notify rabbit-conn name "in" (pack-stanzas result)))
           (deliver prom (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result)))))))

(defn parse-notify-response
  "Process a response from the triple space to a previously requested RDB operation"
  ([name pattern filters f direction rabbit-conn options]
     (let [uuid (.toString (UUID/randomUUID))]
       ;; redefining exchange and declaring fresh queue
       (rabbit/declare-exchange rabbit-conn name (str "exchange-" direction "-" name))
       (rabbit/make-queue rabbit-conn name (str "queue-client-" direction "-" uuid) (str "exchange-" direction "-" name) "msgs")
       (log :info  (str "*** parse-notify " direction " -> about to block..."))
       (make-consumer rabbit-conn name (str "queue-client-" direction "-" uuid)
                      (fn [read]
                        (println (str "*** after " pattern " direction " direction ", unblocking with something: ----->" read "<----------"))
                        (let [flattened (flatten-1 (filter #(not (empty? %1))
                                                           (map #(let [model (defmodel
                                                                               (document-to-model (java.io.ByteArrayInputStream.
                                                                                                   (.getBytes (clojure.contrib.string/trim %1))) :xml))]
                                                                   (apply model-pattern-apply (cons model (cons pattern filters))))
                                                                (return-rdf-stanzas (java.io.BufferedReader. (java.io.StringReader. read))))))]
                          (when-not (empty? flattened)
                            (f flattened))))))))
