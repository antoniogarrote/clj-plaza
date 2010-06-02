;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 31.05.2010

(ns plaza.triple-spaces.multi-remote-server.auxiliary
  (:use (plaza.rdf core sparql)
        (plaza utils)
        (plaza.rdf.implementations jena)
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [plaza.triple-spaces.server auxiliary]
        [clojure.contrib.logging :only [log]])
  (:require [clojure.contrib.string :as string]
            [redis])
  (:import [java.util UUID]
           [com.rabbitmq.client
            ConnectionParameters
            Connection
            Channel
            AMQP
            ConnectionFactory
            Consumer
            QueueingConsumer]))

(defn build-redis-connection-hash
  [opts-hash] {:host (:redis-host opts-hash) :port (:redis-port opts-hash) :db (:redis-db opts-hash)})

(defmacro fmt-out
  [& forms]
  `(pr-str ~@forms))

(defn fmt-in
  [msg]
  (read-string msg))

(defn- apply-rd-operation
  "Applies a TS rd operation over a model"
  ([pattern model]
     (let [w (java.io.StringWriter.)]
       (response
        (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                             (output-string m w :xml)
                             (.write w "<ts:tokensep/>") w))
                w (query-triples model pattern))))))

(defn queue-gen-key
  [[pattern type client-id]]
  (str "blk:" client-id))

(defn queue-key-pattern
  [] "blk:*")

(defn store-blocking-rd
  ([redis [pattern type client-id]]
     (redis/with-server redis (redis/set (queue-gen-key [pattern type client-id])
                                         (fmt-out [pattern type client-id])))))


(defn- apply-rdb-operation
  "Applies a TS rd operation over a model"
  ([pattern model queues client-id]
     (let [w (java.io.StringWriter.)
           results (query-triples model pattern)]
       (if (empty? results)
         (do (store-blocking-rd queues [pattern :rdb client-id])
             (blocking-response))
         (response
          (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                               (output-string m w :xml)
                               (.write w "</ts:response>") w))
                  w results))))))

(defn- apply-out-operation
  "Applies a TS rd operation over a model"
  ([rdf-document model name rabbit-conn queues-ref]
     (with-model model (document-to-model (java.io.ByteArrayInputStream. (.getBytes rdf-document)) :xml))
     (model-critical-write model
                           (redis/with-server queues-ref
                             (let [ks (redis/keys (queue-key-pattern))
                                   vs (map #(fmt-in %1) (if (empty? ks) [] (apply redis/mget ks)))
                                   queues-red (reduce (fn [acum [k v]] (if (= 0 (redis/del k)) acum (conj acum [k v]))) [] (zipmap ks vs))]
                               (redis/atomically
                                (loop [queues queues-red
                                       to-delete []
                                       keys-to-delete []]
                                  (if (empty? queues)
                                    (do (with-model model (model-remove-triples to-delete))
                                        (doseq [k keys-to-delete] (redis/del k)))
                                    (let [[key [pattern kind-op client-id]] (first queues)
                                          results (query-triples model pattern)]
                                      (log :info (str "*** checking queued " kind-op " -> " pattern " ? " (empty? results)))
                                      (if (empty? results)
                                        (recur (rest queues)
                                               to-delete
                                               keys-to-delete)
                                        (let [w (java.io.StringWriter.)
                                              respo (response
                                                     (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                          (output-string m w :xml)
                                                                          (.write w "</ts:response>") w))
                                                             w results))]
                                          (log :info (str "*** queue to blocked client: \r\n" respo))
                                          (rabbit/publish rabbit-conn name (str "exchange-" name) client-id respo)
                                          (recur (rest queues)
                                                 (if (= kind-op :inb) (conj to-delete (flatten-1 results)) to-delete)
                                                 (conj keys-to-delete key))))))))
                               (success))))))

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
                                                                (.write w "</ts:response>") w))
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
                                       (store-blocking-rd queues [pattern :rdb client-id])
                                       (blocking-response))
                               (do
                                 ;; deleting read triples
                                 (with-model model (model-remove-triples (flatten-1 triples-to-remove)))
                                 ;; returning triple sets
                                 (let [triples (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                    (output-string m w :xml)
                                                                    (.write w "</ts:response>") w))
                                                       w triples-to-remove)]
                                   (response triples))))))))

(defn apply-swap-operation-dist
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
                               (redis/with-server queues-ref
                                 (let [ks (redis/keys (queue-key-pattern))
                                       vs (map #(fmt-in %1) (if (empty? ks) [] (apply redis/mget ks)))
                                       queues-red (reduce (fn [acum [k v]] (if (= 0 (redis/del k)) acum (conj acum [k v]))) [] (zipmap ks vs))]
                                   (redis/atomically
                                    (loop [queues queues-red
                                           keys-to-delete []]
                                      (if (empty? queues)
                                        (doseq [k keys-to-delete] (redis/del k))
                                        (let [[key [pattern kind-op client-id]] (first queues)
                                              results (query-triples model pattern)]
                                          (log :info (str "checking queued " kind-op " -> " pattern " ? " (empty? results)))
                                          (if (empty? results)
                                            (recur (rest queues)
                                                   keys-to-delete)
                                            (let [w (java.io.StringWriter.)
                                                  respo (response
                                                         (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                                              (output-string m w :xml)
                                                                              (.write w "</ts:response>") w))
                                                                 w results))]
                                              (when (= kind-op :inb)
                                                (with-model model (model-remove-triples (flatten-1 results))))
                                              (rabbit/publish rabbit-conn name (str "exchange-" name) client-id respo)
                                              (recur (rest queues)
                                                     (conj keys-to-delete key)))))))))))

                             ;; Sending the response back to the original client
                             (response
                              (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                                   (output-string m w :xml)
                                                   (.write w "</ts:response>") w))
                                      w triples-to-remove))))))

(defn apply-operation-dist
  "Applies an operation over a Plaza model"
  ([message name model queues rabbit-conn options]
     (log :info "*** about to apply operation")
     (condp = (:operation message)
       "rd" (apply-rd-operation (:pattern message) model)
       "rdb" (apply-rdb-operation (:pattern message) model queues (:client-id message))
       "out" (apply-out-operation (:value message) model name rabbit-conn queues)
       "in" (apply-in-operation (:pattern message) model name)
       "inb" (apply-inb-operation (:pattern message) model name rabbit-conn queues (:client-id message))
       "swap" (apply-swap-operation-dist (:pattern message) (:value message) model name rabbit-conn queues)
       (throw (Exception. "Unsupported operation")))))
