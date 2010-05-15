;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 15.05.2010

;; Use example:

; user> (use 'plaza.examples.workers)
; user> (status)
; user> (start-workers 3)
;   Registering factorial worker 0
;   Registering factorial worker 1
;   Registering factorial worker 2
; user> (status)
;   function: http://plaza.org/ontologies/factorial queued: 0 workers: 3
; user> (compute-task "factorial" (d 5))
;   *** task http://plaza.org/examples/gearman/tasks/4 starting request...
;   *** task http://plaza.org/examples/gearman/tasks/4 with payload "5"^^<http://www.w3.org/2001/XMLSchema#int> result: 120

(ns plaza.examples.workers
  (:use (plaza.rdf core predicates sparql)
        (plaza.triple-spaces core)
        (plaza.rdf.implementations sesame)
        (plaza utils)))


;; we start Plaza using Sesame as the implementation
(init-sesame-framework)


;; ontology
(def *task* "http://plaza.org/examples/gearman/Task")
(def *payload* "http://plaza.org/examples/gearman/payload")
(def *result* "http://plaza.org/examples/gearman/result")
(def *provides* "http://plaza.org/examples/gearman/providesFunction")
(def *requires* "http://plaza.org/examples/gearman/requiresFunction")
(def *worker* "http://plaza.org/examples/gearman/Worker")


;; declaration of triple spaces
(def-ts :tasks (make-basic-triple-space))
(def-ts :workers (make-basic-triple-space))


;; auxiliary functions

(defn find-property-value
  ([property triples]
     (let [triples-matched (filter (fn [t]
                                     ((triple-check (predicate? (qname-local? property))) t)) triples)]
       (let [obj (object-from-triple (first triples-matched))]
         (if (is-literal obj) (literal-value obj) (resource-id obj))))))


(defn find-task-uri
  ([token] (subject-from-triple (first token))))

(def *id-counter* (ref 0))

(defn id-gen
  ([] (dosync (alter *id-counter* #(+ %1 1)) @*id-counter*)))

(defn register-worker
  ([function-name f]
     (let [uri (str "http://plaza.org/examples/gearman/workers/" (id-gen))]
       (spawn!
        (println (str "*** worker " uri " starting for function: " function-name))
        (out (ts :workers) [[uri rdf:type *worker*]
                            [uri *provides* function-name]])
        (loop []
            ;; any tasks? we block until there is at least new one
            (println (str "*** worker " uri ": grab job"))
            (rdb (ts :tasks) [[?s rdf:type *task*]
                              [?s *requires* function-name]])
          ;; one task! let's try to retrieve some tasks with a non-blocking 'in' operation
          (let [tasks (in (ts :tasks) [[?s rdf:type *task*]
                                       [?s *requires* function-name]
                                       [?s *payload* ?o]])]
            ;; only one of the registered workers, will retrieve the triples
            ;; with the in operation. The others will retrieve an empty triple set
            (if (empty? tasks)
              ;; other agent retrieved the task, we call ourselves recursively
              (do (println (str "*** worker " uri ": no job"))
                  (recur))
              ;; we have the tasks, lets compute the result
              ;; and write it back in the triple space
              (do (println (str "*** worker " uri ": work started"))
                  (doseq [task tasks]
                    (let [payload (find-property-value :payload task)
                          task-uri (find-task-uri task)
                          result (f payload)]
                      (println (str "*** worker " uri ": work finished"))
                      (out (ts :tasks) [[task-uri *result* (d result)]])))
                  (recur)))))))))


(defn compute-task
  ([function-name payload]
     (let [task-uri (str "http://plaza.org/examples/gearman/tasks/" (id-gen))]
       (println (str "*** task " task-uri " starting request..."))
       (out (ts :tasks) [[task-uri rdf:type *task*]
                         [task-uri *requires* function-name]
                         [task-uri *payload* payload]])
       (let [result-triples (first (inb (ts :tasks) [[task-uri *result* ?r]]))]
         (println (str "*** task " task-uri " with payload " payload " result: " (find-property-value :result result-triples)))))))


;; Creates some sample workers computing the factorial function
(defn start-workers [num-worker]
  (let [factorial-fn (fn [n] (reduce #(* %2 %1) 1 (range n 1 -1)))]
    (doseq [i (range 0 num-worker)]
      (println (str "Registering factorial worker " i))
      (register-worker "factorial" factorial-fn))))


;; Similar to the 'status' administrative command from the Gearman protocol
(defn status []
  (let [workers (rd (ts :workers) [[?w rdf:type *worker*]
                                   [?w *provides* ?f]])
        tasks (rd (ts :tasks) [[?t rdf:type *task*]
                               [?t *requires* ?f]])
        tasks-map (reduce (fn [acum task]
                            (let [fname (find-property-value :requiresFunction task)
                                  queued-count (if (nil? (get acum fname)) 0 (get acum fname))]
                              (assoc acum fname (inc queued-count))))
                          {} tasks)
        workers-map (reduce (fn [acum worker]
                              (let [fname (find-property-value :providesFunction worker)
                                    workers-count (if (nil? (get acum fname)) 0 (get acum fname))]
                                (assoc acum fname (inc workers-count))))
                            {} workers)]
    (doseq [fname (keys workers-map)]
      (let [tasks (or (get tasks-map fname) 0)
            workers (get workers-map fname)]
        (println (str "function: " fname " queued: " tasks " workers: " workers))))))
