;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 16.05.2010

(ns plaza.triple-spaces.server.auxiliary
  (:use (plaza.rdf core sparql)
        (plaza utils)
        (plaza.rdf.implementations jena)
        [org.clojars.rabbitmq :as rabbit]
        [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.string :as string]))

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
     (str "TS response:\r\n\r\nsuccess\r\n\r\n" payload "\r\n\r\n")))

(defn blocking-response
  "reates a response to be send down to the client"
  ([] (str "TS response:\r\n\r\nblocking\r\n\r\n")))

(defn success
  "reates a response to be send down to the client"
  ([payload]
     (str "TS response:\r\n\r\nsuccess\r\n\r\n" payload "\r\n\r\n"))
  ([]
     (str "TS response:\r\n\r\nsuccess\r\n\r\n\r\n\r\n")))

(defn failure
  "reates a response to be send down to the client"
  ([payload]
     (str "TS response:\r\n\r\nfailure\r\n\r\n" payload "\r\n\r\n"))
  ([]
     (str "TS response:\r\n\r\nfailure\r\n\r\n\r\n\r\n")))


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

(defn- apply-rd-operation
  "Applies a TS rd operation over a model"
  ([pattern model]
     (let [w (java.io.StringWriter.)]
       (response
        (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                             (output-string m w :xml)
                             (.write w "\r\n\r\n") w))
                w (query-triples model pattern))))))

(defn- apply-rdb-operation
  "Applies a TS rd operation over a model"
  ([pattern model queues routing-info]
     (let [w (java.io.StringWriter.)
           results (query-triples model pattern)]
       (if (empty? results)
         (dosync (alter queues (fn [old] (conj old [pattern routing-info :rdb])))
                 (blocking-response))
         (response
          (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                               (output-string m w :xml)
                               (.write w "\r\n\r\n") w))
                  w results))))))

(defn- apply-out-operation
  "Applies a TS rd operation over a model"
  ([rdf-document model queues-ref rabbit-chn]
     (with-model model (document-to-model (java.io.ByteArrayInputStream. (.getBytes rdf-document)) :xml))
     (model-critical-write model
                           (dosync
                            (loop [queues @queues-ref
                                   queuesp []]
                              (if (empty? queues)
                                (alter queues-ref (fn [old] queuesp))
                                (let [[pattern routing-info kind-op] (first queues)
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
                                      (rabbit/publish routing-info rabbit-chn respo)
                                      (recur (rest queues)
                                             queuesp))))))
                            (success)))))

(defn- apply-in-operation
  "Applies a TS in operation over a model"
  ([pattern model]
     (model-critical-write model
                           (let [w (java.io.StringWriter.)
                                 triples-to-remove (query-triples model pattern)]
                             ;; deleting read triples
                             (with-model model (model-remove-triples (flatten-1 triples-to-remove)))
                             ;; returning triple sets
                             (response
                              (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                   (output-string m w :xml)
                                                   (.write w "\r\n\r\n") w))
                                      w triples-to-remove))))))

(defn- apply-inb-operation
  "Applies a TS inb operation over a model"
  ([pattern model queues routing-info]
     (model-critical-write model
                           (let [w (java.io.StringWriter.)
                                 triples-to-remove (query-triples model pattern)]
                             (if (empty? triples-to-remove)
                               (dosync (log :info "Storing INB operation in the queue")
                                       (alter queues (fn [old] (conj old [pattern routing-info :inb])))
                                       (blocking-response))
                               (do
                                 ;; deleting read triples
                                 (with-model model (model-remove-triples (flatten-1 triples-to-remove)))
                                 ;; returning triple sets
                                 (response
                                  (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                       (output-string m w :xml)
                                                       (.write w "\r\n\r\n") w))
                                          w triples-to-remove))))))))

(defn apply-operation
  "Applies an operation over a Plaza model"
  ([message model queues rabbit-conn rabbit-chn options]
     (let [[exch routing-key] (.split (:client-id message) ":")]
       (condp = (:operation message)
         "rd" (apply-rd-operation (:pattern message) model)
         "rdb" (apply-rdb-operation (:pattern message) model queues {:exchange exch :routing-key routing-key})
         "out" (apply-out-operation (:value message) model queues rabbit-chn)
         "in" (apply-in-operation (:pattern message) model)
         "inb" (apply-inb-operation (:pattern message) model queues {:exchange exch :routing-key routing-key})
         (throw (Exception. "Unsupported operation"))))))

;; Parsers

(defmacro if-not-eos
  ([writer & body]
     `(if (.ready ~writer)
        (do ~@body)
        (throw (Exception. "Reached eof")))))


(defn- read-eos
  ([in]
     (if (not (.ready in))
       in
       (let [line (.readLine in)]
         (if (= line "")
           (recur in)
           (throw (Exception. (str "error parsing response: got <" line "> but <EOS> was expected"))))))))

(defn- read-ts-response
  ([in]
     (if-not-eos in
                 (let [line (.readLine in)]
                   (when (not (= line "TS response:"))
                     (throw (Exception. (str "error parsing response: got <" line "> but <TS response:> was expected"))))
                   in))))

(defn- read-blank-line
  ([in]
     (if-not-eos in
                 (let [line (.readLine in)]
                   (when (not (= line ""))
                     (throw (Exception. (str "error parsing response: got <" line "> but blank line was expected"))))
                   in))))

(defn- read-success
  ([in]
     (if-not-eos in
                 (let [line (.readLine in)]
                   (when (not (= line "success"))
                     (throw (Exception. (str "error parsing response: got <" line "> but <success> was expected"))))
                   in))))

(defn- return-succes-failure
  ([in]
     (if-not-eos in
                 (let [line (.readLine in)]
                   (if (or (= line "success")
                           (= line "failure")
                           (= line "blocking"))
                     line
                     (throw (Exception. (str "error parsing response: got <" line "> but <success|failure> expected"))))))))

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

(defn- return-rdf-stanzas
  ([in]
     (if-not-eos in
                 (loop [writer (java.io.StringWriter.)
                        acum   []]
                   (if (not (.ready in))
                     acum
                     (let [line (.readLine in)]
                       (if (not (= "</rdf:RDF>" line))
                         (if (= "" line)
                           (recur writer acum)
                           (do (.write writer line)
                               (recur writer acum)))
                         (do (.write writer line)
                             (recur (java.io.StringWriter.)
                                    (conj acum (.toString writer)))))))))))

(defn parse-out-response
  "Process a response from the triple space to a previously requested OUT operation"
  ([in]
     (let [result (-> in
                      (read-ts-response)
                      (read-blank-line)
                      (return-succes-failure))]
       (read-eos in)
       (keyword result))))

(defn parse-rd-response
  "Process a response from the triple space to a previously requested RD operation"
  ([in]
     (str "reading response rd")
     (let [result (-> in
                      (read-ts-response)
                      (read-blank-line)
                      (read-success)
                      (read-blank-line)
                      (return-rdf-stanzas))]
       (read-eos in)
       (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result))))

(defn parse-rdb-response
  "Process a response from the triple space to a previously requested RDB operation"
  ([in rabbit-chn options]
     (str "reading response rd")
     (let [should-block (-> in
                            (read-ts-response)
                            (read-blank-line)
                            (return-succes-failure))]
       (if (= should-block "blocking")
         (do (read-eos in)
             (println (str "about to block..."))
             (let [result (rabbit/consume-poll options rabbit-chn)]
               (println (str "unblocking with something"))
               (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result)))
         (let [result (-> in
                          (read-blank-line)
                          (return-rdf-stanzas))]
           (read-eos in)
           (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result))))))
