;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 07.05.2010

(ns plaza.triple-spaces.core
  (:use (plaza.rdf core predicates sparql)
        (plaza.utils))
  (:import (java.util.concurrent LinkedBlockingQueue)))

(defprotocol TripleSpace
  "Triple Space operations protocol"
  (rd [ts pattern] [ts pattern filters] "Read some triples from the triple space ts matching the provided pattern. Triples are not removed from the triple space")
  (rdb [ts pattern] [ts pattern filters] "Blocking version of the rd operation")
  (in [ts pattern] [ts pattern filters] "Extracts some triples from the triple space ts matching the provided pattern. Triples are removed from the triple space")
  (inb [ts pattern] [ts pattern filters] "Blocking version of the in operation")
  (out [ts triples] "Add some triples to the triple space ts.")
  (swap [ts pattern triples] [ts pattern triples filters] "Remove triples matching pattern and replace them with the new triples in single atomic operation")
  (notify [ts op pattern f][ts op pattern filters f] "Blocks until some other process performs an op operation in the triple space matching the provided pattern")
  (clean [ts] "Clean the resources associated to this triple space")
  (inspect [ts] "Testing"))

;; Ahead declarations
(declare check-notify-queues)


(defn- splice-args
  ([a b rest]
     (cons a (cons b rest))))

(defn- process-rdb-inb
  [model pattern filters sync kind-op]
  (let [res (apply model-pattern-apply (splice-args model pattern filters))
        flattened (vec (plaza.utils/flatten-1 res))]
    (if (empty? res)
      [false [] [] nil nil]
      (if (= kind-op :rdb)
        [true res [] {:res res :sync sync}]
        [true res flattened {:res res :sync sync}]))))

(defn- process-queues-out
  [model queues notify-queues]
  (loop [queues queues
         queuesp []
         notify-queues notify-queues
         to-delete []
         deliveries []]
    (if (empty? queues)
      (do (with-model model (model-remove-triples to-delete))
          (loop [remaining deliveries]
            (when-not (empty? remaining)
              (do (deliver (:sync (first remaining)) (:res (first remaining)))
                  (recur (rest remaining)))))
          ;; @todo
          ;; why is this not working?
          ;;(for [delivery deliveries] (deliver (:sync delivery) (:res delivery))))
          [queuesp notify-queues])
      (let [[pattern filters sync kind-op] (first queues)
            [matched? triples-read triples-to-delete delivery] (process-rdb-inb model pattern filters sync kind-op)]
        (if matched?
          (recur (rest queues)
                 queuesp
                 (check-notify-queues triples-read kind-op notify-queues)
                 (reduce conj to-delete triples-to-delete)
                 (conj deliveries delivery))
          (recur (rest queues)
                 (conj queuesp (first queues))
                 (check-notify-queues triples-read kind-op notify-queues)
                 (reduce conj to-delete triples-to-delete)
                 deliveries))))))

(defn- make-queue
  ([op pattern filters]
     {:operation op
      :queue (LinkedBlockingQueue.)
      :pattern pattern
      :filters filters}))

(defn check-queue
  ([models op queue-data]
     (let [queue (:queue queue-data)
           pattern (:pattern queue-data)
           filters (:filters queue-data)
           opp (if (or (= op :in) (= op :inb) (= op :rd) (= op :rdb)) :in :out)]
       (let [should-finish (.peek queue)]
         (if (= should-finish :finish)
           nil
           (if (= opp (:operation queue-data))
             (let [res (plaza.utils/flatten-1 (pmap #(apply model-pattern-apply (splice-args %1 pattern filters)) models))]
               (if (empty? res)
                 queue-data
                 (do (loop [rs res]
                       (when-not (empty? rs)
                         (.put queue (first rs))
                         (recur (rest rs))))
                     queue-data)))
                 queue-data))))))

(defn check-notify-queues
  ([triples op queues]
     (if (or  (empty? queues)
              (empty? triples))
       queues
       (let [models (pmap #(defmodel (model-add-triples %1)) triples)]
         (filter #(not (nil? %1)) (pmap #(check-queue models op %1) queues))))))



(deftype BasicTripleSpace [ts-agent]
  TripleSpace

;;;;
;;;; rd operation
;;;;
  (rd [this pattern filters]
      (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (let [m (:model ag)
                               spliced (splice-args m pattern filters)
                               to-deliver (apply model-pattern-apply spliced)]
                           (deliver prom to-deliver)
                           (let [notify-queuesp (check-notify-queues to-deliver :rd (:notify-queues ag))]
                             (assoc ag :notify-queues notify-queuesp))))
              cell)
        @cell))
  (rd [this pattern] (rd this pattern []))

;;;;
;;;; rdb operation
;;;;
  (rdb [this pattern filters]
       (let [cell (promise)]
         (send ts-agent (fn [ag prom]
                          (model-critical-read (:model ag)
                                               (let [m (:model ag)
                                                     triples (apply model-pattern-apply (splice-args m pattern filters))]
                                                 (if (empty? triples)
                                                   (let [sync (promise)
                                                         to-return {:sync sync}
                                                         oldqueues (:queues ag)
                                                         agp (assoc ag :queues (conj oldqueues [pattern filters sync :rdb]))]
                                                     (deliver prom to-return)
                                                     agp)
                                                   (do
                                                     (deliver prom triples)
                                                     (let [notify-queuesp (check-notify-queues triples :rdb (:notify-queues ag))]
                                                       (assoc ag :notify-queues notify-queuesp)))))))
               cell)
         (let [returned @cell]
           (if (map? returned)
             (let [sync (:sync returned)] @sync)
             returned))))
  (rdb [this pattern] (rdb this pattern []))

;;;;
;;;; in operation
;;;;
  (in [this pattern filters]
      (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (model-critical-write (:model ag)
                                               (let [m (:model ag)
                                                     triples (apply model-pattern-apply (splice-args m pattern filters))
                                                     flattened (plaza.utils/flatten-1 triples)]
                                                 (with-model m
                                                   (model-remove-triples flattened))
                                                 (deliver prom triples)
                                                 (let [notify-queuesp (check-notify-queues triples :in (:notify-queues ag))]
                                                       (assoc ag :notify-queues notify-queuesp)))))
              cell)
        @cell))
  (in [this pattern] (in this pattern []))

;;;;
;;;; inb operation
;;;;
  (inb [this pattern filters]
       (let [cell (promise)]
         (send ts-agent (fn [ag prom]
                          (model-critical-write (:model ag)
                                                (let [m (:model ag)
                                                      triples (apply model-pattern-apply (splice-args m pattern filters))
                                                      flattened (plaza.utils/flatten-1 triples)]
                                                  (if (empty? flattened)
                                                    (let [sync (promise)
                                                          to-return {:sync sync}
                                                          oldqueues (:queues ag)
                                                          agp (assoc ag :queues (conj oldqueues [pattern filters sync :inb]))]
                                                      (deliver prom to-return)
                                                      agp)
                                                    (do
                                                      (with-model m
                                                        (model-remove-triples flattened))
                                                      (deliver prom triples)
                                                      (let [notify-queuesp (check-notify-queues triples :inb (:notify-queues ag))]
                                                        (assoc ag :notify-queues notify-queuesp)))))))
               cell)
         (let [returned @cell]
           (if (map? returned)
             (let [sync (:sync returned)] @sync)
             returned))))
  (inb [this pattern] (inb this pattern []))

;;;;
;;;; out operation
;;;;
  (out [this triples-or-vector]
       (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))]
         (send ts-agent (fn [ag]
                          (model-critical-write (:model ag)
                                                (let [m (:model ag)
                                                      queues (:queues ag)
                                                      notify-queues (:notify-queues ag)]
                                                  (with-model m
                                                    (model-add-triples triples))
                                                  (let [[queuesp notify-queuesp] (process-queues-out m queues notify-queues)
                                                        notify-queuespp (check-notify-queues [triples] :out notify-queuesp)
                                                        agp (assoc ag :queues queuesp)]
                                                    (assoc agp :notify-queues notify-queuespp))))))
         triples))

;;;;
;;;; swap operation
;;;;
  (swap [ts pattern triples-or-vector filters]
        (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))
              cell (promise)]
          (send ts-agent (fn [ag prom]
                           (model-critical-write (:model ag)
                                                 (let [m (:model ag)
                                                       queues (:queues ag)
                                                       notify-queues (:notify-queues ag)
                                                       triples-out (apply model-pattern-apply (splice-args m pattern filters))
                                                       flattened (plaza.utils/flatten-1 triples-out)]
                                                   (with-model m
                                                     (model-remove-triples flattened)
                                                     (if-not (empty? flattened)
                                                       (do (model-add-triples triples)
                                                           (let [[queuesp notify-queuesp] (process-queues-out m queues notify-queues)]
                                                             (deliver prom flattened)
                                                             (let [notify-queuespp (check-notify-queues triples-out :rd notify-queuesp)
                                                                   notify-queuesppp (check-notify-queues [triples] :out notify-queuespp)
                                                                   agp (assoc ag :queues queuesp)]
                                                               (assoc agp :notify-queues notify-queuesppp))))
                                                       (do (deliver prom flattened) ag))))))
                cell)
          @cell))
  (swap [ts pattern triples-or-vector] (swap ts pattern triples-or-vector []))

;;;;
;;;; notify operation
;;;;
  (notify [ts op pattern filters f]
          (let [cell (promise)
                queue-data (make-queue op pattern filters)]
            (send ts-agent (fn [ag prom]
                             (model-critical-write (:model ag)
                                                   (deliver prom :created)
                                                   (assoc ag :notify-queues (conj (:notify-queues ag) queue-data))))
                  cell)
            (println "handshake...")
            @cell
            (loop [queue (:queue queue-data)]
              (let [results (.take queue)
                    should-continue (f results)]
                (if (= should-continue :finish)
                  (.put queue :finish)
                  (recur queue))))))
  (notify [ts op pattern f] (notify ts op pattern [] f))
  (inspect [this] ts-agent)
  (clean [this] :clean))

(defn make-basic-triple-space
  "Builds a basic triple space that can be shared among different
   threads in the same node"
  ([]
     (let [m (defmodel)
           id-gen (let [id (ref 0)] (fn [] (dosync (alter id #(+ %1 1)) @id)))
           ts (BasicTripleSpace. (agent {:model m :id-gen id-gen :queues [] :notify-queues []}))]
       (fn [] ts))))


;; Common agent operations


;; Shared has where every defined agent will be registered
(def *plaza-agent-registry* (ref {}))
(def *plaza-triple-spaces* (ref {}))

(defn register-agent
  "Registers an agent definition in the registry"
  ([name agent-fn]
     (dosync (alter plaza.triple-spaces.core/*plaza-agent-registry* (fn [registry] (assoc registry name agent-fn))))))

(defn unregister-agent
  "Unregisters an agent from the registry"
  ([name]
     (dosync (alter plaza.triple-spaces.core/*plaza-agent-registry* (fn [registry] (dissoc registry name))))))

(defn def-ts
  "Defines a triple space"
  ([name ts]
     (dosync (alter plaza.triple-spaces.core/*plaza-triple-spaces* (fn [registry] (assoc registry name ts))))))

(defn unregister-ts
  "Unregisters a triple space"
  ([name]
     (dosync (alter plaza.triple-spaces.core/*plaza-triple-spaces* (fn [registry] (dissoc registry name))))))

(defn ts
  "Retrieves a triple space from the registry"
  [name]
  (apply (dosync (get (deref plaza.triple-spaces.core/*plaza-triple-spaces*) name)) []))

(defmacro def-agent
  "Defines a new agent"
  ([agent-name & body]
     `(register-agent ~agent-name
                      (fn []
                        (with-model (build-model)
                          ~@body)))))

(defmacro spawn!
  "Starts a new agent"
  ([& args]
     (if (list? (first args))
       `(.start (Thread. (fn [] (with-model (build-model) ~@args))))
       (let [name (first args)]
         `(.start (Thread. (get (deref plaza.triple-spaces.core/*plaza-agent-registry*) ~name)))))))
