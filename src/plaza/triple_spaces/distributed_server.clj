;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 28.05.2010

(ns plaza.triple-spaces.distributed-server
  (:use [saturnine]
	[saturnine.handler]
        [plaza utils]
        [plaza.triple-spaces.server.rabbit :as rabbit]
        [clojure.contrib.logging :only [log]]
        (plaza.triple-spaces core server)
        (plaza.triple-spaces.server auxiliary)
        (plaza.triple-spaces.multi-remote-server auxiliary)
        (plaza.rdf core sparql)
        (plaza.rdf.implementations sesame))
  (:import [java.util UUID]
           [java.net URL]))


(defn gen-query
  ([pattern-or-vector filters-pre]
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
                     (query-set-filters filters)))]
       query)))

(defn process-in-blocking-op
  "Process a response from the triple space to a previously requested RDB operation"
  ([name should-deliver rabbit-conn options prom]
     (do ;(log :info (str "about to block queue : " (:cliend-id options) " and options: " options))
       (let [read (rabbit/consume-n-messages rabbit-conn name (str "queue-client-" (:client-id options)) 1)
             result (-> (java.io.BufferedReader. (java.io.StringReader. (first read)))
                        (read-ts-response)
                        (read-token-separator)
                        (read-success)
                        (read-token-separator)
                        (return-rdf-stanzas))]
         (when should-deliver (deliver-notify rabbit-conn name "in" (pack-stanzas result)))
         (deliver prom (map #(with-model (defmodel) (model-to-triples (document-to-model (java.io.ByteArrayInputStream. (.getBytes %1)) :xml))) result))))))

(defn remove-and-notify
  "Removes triples from the model and notify all the queues that the triples have been extracted"
  ([model triples-to-remove rabbit-conn name]
     (let [flattened-triples (flatten-1 triples-to-remove)]
       ;; deleting read triples
       (with-model model (model-remove-triples flattened-triples))
       ;; delivering notifications
       (let [w (java.io.StringWriter.)
             triples (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                                          (output-string m w :xml)
                                          (.write w "<ts:tokensep/>") w))
                             w triples-to-remove)]
         (.write w "</ts:response>")
         (deliver-notify rabbit-conn name "in" (.toString w)))
       ;; returning triple sets
       triples-to-remove)))

(defn results-to-rdf-response
  ([results]
     (let  [w (java.io.StringWriter.)]
       (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                            (output-string m w :xml)
                            (.write w "<ts:tokensep/>") w))
               w results)
       (response (.toString w)))))

(defn results-to-rdf-stanzas
  ([results]
     (let  [w (java.io.StringWriter.)]
       (reduce (fn [w ts] (let [m (defmodel (model-add-triples ts))]
                            (output-string m w :xml)
                            (.write w "<ts:tokensep/>") w))
               w results)
       (.write w "</ts:response>")
       (.toString w))))
;;
;; RemoteTripleSpace
;;
(deftype DistributedTripleSpace [name model queue rabbit-conn options] TripleSpace

  ;; rd operation
  (rd [this pattern] (rd this pattern []))

  (rd [this pattern filters] (query-triples model (gen-query pattern filters)))

  ;; rdb operation
  (rdb [this pattern] (rdb this pattern []))

  (rdb [this pattern filters]
       (let [query (gen-query pattern filters)
             ts (query-triples model query)]
         (if (empty? ts)
           (let [prom (promise)]
             (log :info "storing remote state in queue for rdb")
             (plaza.triple-spaces.multi-remote-server.auxiliary/store-blocking-rd queue [(query-to-string query) :rdb (:client-id options)])
             (process-in-blocking-op name false rabbit-conn options prom)
             @prom) ts)))


  ;; in operation
  (in [this pattern] (in this pattern []))

  (in [this pattern filters]
      (model-critical-write model
                            (let [triples-to-remove (query-triples model (gen-query pattern filters))]
                              (if (empty? triples-to-remove) triples-to-remove
                                  (let [flattened-triples (flatten-1 triples-to-remove)]
                                    ;; deleting read triples
                                    (with-model model (model-remove-triples flattened-triples))
                                    ;; delivering notifications
                                    (deliver-notify rabbit-conn name "in" (results-to-rdf-stanzas triples-to-remove))
                                    ;; returning triple sets
                                    triples-to-remove)))))


  ;; inb operation
  (inb [this pattern] (inb this pattern []))

  (inb [this pattern filters]
       (let [query (gen-query pattern filters)
             res-inb (model-critical-write model
                                           (let [triples-to-remove (query-triples model query)]
                                             (if (empty? triples-to-remove)
                                               ;; no triples? block and wait for response
                                               :should-block
                                               ;; triples? delete and notify
                                               (remove-and-notify model triples-to-remove rabbit-conn name))))]
         (if (= :should-block res-inb)
           (let [prom (promise)]
             (log :info "storing remote state in queue for inb")
             (plaza.triple-spaces.multi-remote-server.auxiliary/store-blocking-rd queue [(query-to-string query) :inb (:client-id options)])
             (process-in-blocking-op name true rabbit-conn options prom)
                                        ; we try to read again to prevent possible race
                                        ; condition with writer process writing
                                        ; while we were registering the queue
             (let [triples-to-remove-2 (query-triples model query)]
               (if (empty? triples-to-remove-2)
                 @prom
                 (model-critical-write model (remove-and-notify model triples-to-remove-2 rabbit-conn name)))))
                                        ; we have read the right result, we don't need to block and wait
           res-inb)))

  ;; out operation
  (out [this triples]
       (model-critical-write model
                             ;; Adding the triples
                             (with-model model (model-add-triples triples))
                             ;; Processing pending queues for inb or rdb operations
                             (redis/with-server queue
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
                                          (let [respo (results-to-rdf-response results)]
                                            (log :info (str "*** queue to blocked client: \r\n" respo))
                                            (rabbit/publish rabbit-conn name (str "exchange-" name) client-id respo)
                                            (recur (rest queues)
                                                   (if (= kind-op :inb) (conj to-delete (flatten-1 results)) to-delete)
                                                   (conj keys-to-delete key))))))))
                                 ;; delivering notifications for notify op. readers
                                 (deliver-notify rabbit-conn name "out" (results-to-rdf-stanzas [triples]))
                                 triples))))


  ;; swap operation
  (swap [this pattern triples] (swap this pattern triples []))

  (swap [this pattern triples filters]
        (model-critical-write model
                              (let [triples-to-remove (query-triples model (gen-query pattern filters))
                                    flattened-triples (if (empty? triples-to-remove) [] (flatten-1 triples-to-remove))]
                                (when-not (empty? triples-to-remove)
                                  (with-model model
                                    (model-remove-triples flattened-triples)
                                    (model-add-triples triples))

                                  ;; Processing queues
                                  (redis/with-server queue
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
                                               (do (when (= kind-op :inb)
                                                     (with-model model (model-remove-triples (flatten-1 results))))
                                                   (rabbit/publish rabbit-conn name (str "exchange-" name) client-id (results-to-rdf-response results))
                                                   (recur (rest queues)
                                                          (conj keys-to-delete key)))))))))))
                                triples-to-remove)))


  ;; notify operation
  (notify [this op pattern f] (notify this op pattern [] f))

  (notify [this op pattern filters f]
          (parse-notify-response name pattern  filters f (plaza.utils/keyword-to-string op) rabbit-conn options))


  ;; inspect
  (inspect [this] :not-yet)

  ;; clean
  (clean [this] :ok))

(defn make-distributed-triple-space
  "Creates a new distributed triple space"
  ([name model & options]
     (let [registry (ref {})]
       (fn []
         (let [id (plaza.utils/thread-id)
               ts (get @registry id)]
           (if (nil? ts)
             (let [uuid (.toString (UUID/randomUUID))
                   opt-map (-> (apply array-map options)
                               (assoc :routing-key uuid)
                               (assoc :queue (str "box-" uuid)))
                   rabbit-conn (rabbit/connect opt-map)]

               ;; Creating a channel and declaring exchanges
               (rabbit/make-channel rabbit-conn name)
               (rabbit/declare-exchange rabbit-conn name (str "exchange-" name))
               (rabbit/declare-exchange rabbit-conn name (str "exchange-out-" name))
               (rabbit/declare-exchange rabbit-conn name (str "exchange-in-" name))

               ;; Creating queues and bindings
               (rabbit/make-queue rabbit-conn name (str "queue-client-" uuid) (str "exchange-" name) uuid)

               ;; startint the server
               (plaza.triple-spaces.distributed-server.DistributedTripleSpace. name
                                                                               model
                                                                               (build-redis-connection-hash opt-map)
                                                                               rabbit-conn
                                                                               (assoc opt-map :client-id uuid)))
             ts))))))
