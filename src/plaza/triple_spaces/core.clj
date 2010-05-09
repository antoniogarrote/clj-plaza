;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 07.05.2010

(ns plaza.triple-spaces.core
  (:use (plaza.rdf core predicates sparql)
        (plaza.utils)))

(defprotocol TripleSpace
  "Triple Space operations protocol"
  (rd [ts pattern] "Read some triples from the triple space ts matching the provided pattern. Triples are not removed from the triple space")
  (rdb [ts pattern] "Blocking version of the rd operation")
  (in [ts pattern] "Extracts some triples from the triple space ts matching the provided pattern. Triples are removed from the triple space")
  (inb [ts pattern] "Blocking version of the in operation")
  (out [ts triples] "Add some triples to the triple space ts.")
  (swap [ts pattern triples] "Remove triples matching pattern and replace them with the new triples in single atomic operation")
  (show [ts] "Testing"))

;  (swap [ts pattern triples] "Atomically retrieves some triples from the triple space, matching the provided pattern, and replace them with new triples")
;  (notify [ts op pattern] "Blocks until some other process performs an op operation in the triple space matching the provided pattern"))

;(deftype FooTripleSpace [] TripleSpace
;  (rd [this pattern] :foo)
;  (rdb [this pattern] :foo)
;  (in [this pattern] :foo)
;  (inb [this pattern] :foo)
;  (out [this triples] :foo)
;  (swap [this pattern triples] :foor)
;  (notify [this op pattern] :foo))

(deftype FooTripleSpace [] TripleSpace
  (in [this pattern] :foo)
  (inb [this pattern] :foo)
  (out [this triples] :foo)
  (show [this] :foo))


;(defprotocol P
;  (foo [x])
;  (bar-me [x] [x y]))
;
;(deftype Foo [a b c]
;  P
;  (foo [x] a)
;  (bar-me [x] b)
;  (bar-me [x y] (+ c y)))



(deftype BasicTripleSpace [identifier ts-agent]
  TripleSpace
  (rd [this pattern]
      (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (let [m (:model ag)]
                           (deliver prom (model-pattern-apply m pattern))
                           ag))
              cell)
        ;;(plaza.utils/probe-agent! ts-agent (str "Error reading triples from triple space " identifier) true)
        @cell))
  (rdb [this pattern]
       (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (model-critical-read (:model ag)
                           (let [m (:model ag)
                                 triples (model-pattern-apply m pattern)]
                             (if (empty? triples)
                               (let [sync (promise)
                                     to-return {:sync sync}
                                     oldqueues (:queues ag)
                                     agp (assoc ag :queues (conj oldqueues [pattern sync :rdb]))]
                                 (deliver prom to-return)
                                 agp)
                               (do
                                 (deliver prom triples)
                                 ag)))))
              cell)
        (println (str "ABOUT TO PROBE " ts-agent))
        ;;(plaza.utils/probe-agent! ts-agent (str "Error removing triples from triple space " identifier) true)
        (let [returned @cell]
          (println (str "RETURNED " returned))
          (if (map? returned)
            (let [sync (:sync returned)]
              (println "ABOUT TO BLOCK")
              @sync
              (println "UNBLOCKED!")
              @sync)
            returned))))
  (in [this pattern]
      (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (model-critical-write (:model ag)
                         (let [m (:model ag)
                               triples (model-pattern-apply m pattern)
                               flattened (plaza.utils/flatten-1 triples)]
                           (with-model m
                             (model-remove-triples flattened))
                           (deliver prom triples)
                           ag)))
              cell)
        ;;(plaza.utils/probe-agent! ts-agent (str "Error removing triples from triple space " identifier) true)
        @cell))
  (inb [this pattern]
       (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (model-critical-write (:model ag)
                           (let [m (:model ag)
                                 triples (model-pattern-apply m pattern)
                                 flattened (plaza.utils/flatten-1 triples)]
                             (if (empty? flattened)
                               (let [sync (promise)
                                     to-return {:sync sync}
                                     oldqueues (:queues ag)
                                     agp (assoc ag :queues (conj oldqueues [pattern sync :inb]))]
                                 (deliver prom to-return)
                                 agp)
                               (do
                                 (with-model m
                                   (model-remove-triples flattened))
                                 (deliver prom triples)
                                 ag)))))
              cell)
        ;;(plaza.utils/probe-agent! ts-agent (str "Error removing triples from triple space " identifier) true)
        (let [returned @cell]
          (if (map? returned)
            (let [sync (:sync returned)]
              (println "ABOUT TO BLOCK")
              @sync
              (println "UNBLOCKED!")
              @sync)
            returned))))
  (out [this triples-or-vector]
       (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))]
         (send ts-agent (fn [ag]
                          (let [m (:model ag)
                                queues (:queues ag)]
                            (println (str "HAY " (count queues) " queues"))
                            (with-model m
                              (model-add-triples triples))
                            (println (str "ABOUT to CHECK queues: " (count queues)))
                            (let [queuesp (filter
                                           (fn [[p sync kindb]]
                                             (let [res (model-pattern-apply m p)]
                                               (println "..checking..")
                                               (if (empty? res)
                                                 true
                                                 (let [flattened (plaza.utils/flatten-1 res)]
                                                   (println "ABOUT TO DELIVER")
                                                   (when (= kindb :inb)
                                                     (with-model m (model-remove-triples flattened)))
                                                   (deliver sync res)
                                                   false))))
                                           (reverse queues))]
                              (println (str "AHORA HAY " (count queuesp) " queues"))
                              (assoc ag :queues queuesp)))))
         triples))
  (swap [ts pattern triples-or-vector]
        (let [triples (if (:triples (meta triples-or-vector)) triples-or-vector (make-triples triples-or-vector))
              cell (promise)]
        (send ts-agent (fn [ag prom]
                         (model-critical-write (:model ag)
                         (let [m (:model ag)
                               queues (:queues ag)
                               triples-out (model-pattern-apply m pattern)
                               flattened (plaza.utils/flatten-1 triples-out)]
                           (println (str "TO REMOVE " flattened))
                           (with-model m
                             (model-remove-triples flattened)
                             (model-add-triples triples)
                             (println (str "ABOUT to CHECK queues: " (count queues)))
                             (let [queuesp (filter
                                            (fn [[p sync kindb]]
                                              (let [res (model-pattern-apply m p)]
                                                (println "..checking..")
                                                (if (empty? res)
                                                  true
                                                  (let [flattened (plaza.utils/flatten-1 res)]
                                                    (println "ABOUT TO DELIVER")
                                                    (when (= kindb :inb)
                                                      (with-model m (model-remove-triples flattened)))
                                                    (deliver sync res)
                                                    false))))
                                            (reverse queues))]
                               (println (str "AHORA HAY " (count queuesp) " queues"))
                               (deliver prom triples)
                              (assoc ag :queues queuesp))))))
              cell)
        @cell))
  (show [this] ts-agent))

(defn make-basic-triple-space
  "Builds a basic triple space that can be shared among different
   threads in the same node"
  ([identifier]
     (let [m (defmodel)
           id-gen (let [id (ref 0)] (fn [] (dosync (alter id #(+ %1 1)) @id)))
           queues []]
       (BasicTripleSpace. identifier (agent {:model m :id-gen id-gen :queues queues})))))
