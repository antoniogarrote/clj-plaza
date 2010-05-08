;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 07.05.2010

(ns plaza.triple-spaces.core
  (:use (plaza.rdf core predicates sparql)
        (plaza.utils)))

(defprotocol TripleSpace
  "Triple Space operations protocol"
  (rd [ts pattern] "Read some triples from the triple space ts matching the provided pattern. Triples are not removed from the triple space")
;  (rdb [ts pattern] "Blocking version of the rd operation")
  (in [ts pattern] "Extracts some triples from the triple space ts matching the provided pattern. Triples are removed from the triple space")
  (inb [ts pattern] "Blocking version of the in operation")
  (out [ts triples] "Add some triples to the triple space ts.")
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
        (plaza.utils/probe-agent! ts-agent (str "Error reading triples from triple space " identifier) true)
        @cell))
  (in [this pattern]
      (let [cell (promise)]
        (send ts-agent (fn [ag prom]
                         (model-critical-write (:model ag)
                         (let [m (:model ag)
                               triples (model-pattern-apply m pattern)
                               flattened (plaza.utils/flatten-1 triples)]
                           (with-model m
                             (model-remove-triples triples))
                           (deliver prom triples)
                           ag)))
              cell)
        (plaza.utils/probe-agent! ts-agent (str "Error removing triples from triple space " identifier) true)
        @cell))
  (inb [this pattern] :foo)
  (out [this triples]
       (let []
        (send ts-agent (fn [ag]
                         (let [m (:model ag)]
                           (with-model m
                             (model-add-triples triples))
                           ag)))
        (plaza.utils/probe-agent! ts-agent (str "Error writing triples int triple space " identifier) true)
        triples))
  (show [this] ts-agent))

(defn make-basic-triple-space
  "Builds a basic triple space that can be shared among different
   threads in the same node"
  ([identifier]
     (let [m (defmodel)
           id-gen (let [id (ref 0)] (fn [] (dosync (alter id #(+ %1 1)) @id)))
           queues {}]
       (BasicTripleSpace. identifier (agent {:model m :id-gen id-gen :queues queues})))))
