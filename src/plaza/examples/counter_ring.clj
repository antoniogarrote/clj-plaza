;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 15.05.2010

(ns plaza.examples.counter-ring
  (:use (plaza.rdf core predicates sparql)
        (plaza.triple-spaces core)
        (plaza.rdf.implementations jena)
        (plaza utils)))


;; we start Plaza using Jena as the implementation
(init-jena-framework)


;; constants
(def *max-agents* 10)
(def *token-uri* "http://plaza.org/examples/token-ring/id")
(def *sleep-time* 1000)


;; declaration of triple spaces
(def-ts :counter (make-basic-triple-space))


;; we store some constrains in the triple space
(out (ts :counter) [[:constrains :max-agents (d *max-agents*)]])
(out (ts :counter) [[:constrains :should-finish (d false)]])


;; auxiliary functions
(defn find-property-value
  ([triples property]
     (literal-value
      (object-from-triple (first (filter
                                  (triple-check (predicate? (qname-local? property)))
                                  triples))))))

(defn find-token-uri
  ([token] (subject-from-triple (first token))))


;; declaration of agents in the ring
(doseq [i (range 0 *max-agents*)]
  (def-agent i
    (println (str "*** " i " hello!"))
    (loop []
      (let [token (first
                   (inb (ts :counter) [[?s rdf:type :Counter]
                                       [?s :next (d i)]
                                       [?s :value ?v]]))
            constrains (first
                        (rd (ts :counter) [[:constrains :max-agents ?m]
                                           [:constrains :should-finish ?n]]))]
        (println (str "*** " i " found the token, sleeping " *sleep-time* " millisecs"))
        (Thread/sleep *sleep-time*)
        (if-not (find-property-value constrains :should-finish)
          (let [old-value (find-property-value token :value)
                max-agents (find-property-value constrains :max-agents)
                next-value (inc old-value)
                next-agent (if (< (inc i) max-agents) (inc i) 0) ]
            (println (str "*** " i " storing value " next-value))
            (out (ts :counter) [[*token-uri* rdf:type :Counter]
                                [*token-uri* :next (d next-agent)]
                                [*token-uri* :value (d next-value)]])
            (recur))
          (println (str "*** " i " finishing execution")))))))



(defn start-ring []
  ;; we insert the token and start the computation
  (out (ts :counter) [[*token-uri* rdf:type :Counter]
                      [*token-uri* :next (d 0)]
                      [*token-uri* :value (d 0)]])
  (println "token inserted")

  ;; we start the agents in the ring
  (doseq [i (range 0 *max-agents*)]
    (spawn! i))
  (println "initialization finished"))

(defn stop-ring []
  (swap (ts :counter) [[:constrains :should-finish ?o]] [[:constrains :should-finish (d true)]]))
