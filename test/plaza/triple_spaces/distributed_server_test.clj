(ns plaza.triple-spaces.distributed-server-test
  (:use
   [plaza.rdf core]
   [plaza.triple-spaces distributed-server]
   [plaza.triple-spaces server]
   [plaza.triple-spaces core]
   [plaza.rdf core sparql predicates]
   [plaza.rdf.implementations jena] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)

(defn- clean-ts
  ([ts] (in ts [[?s ?p ?o]])))

(defn- breathe
  ([] (Thread/sleep 2000)))

(defn- build-mulgara
  ([] (build-model :mulgara :rmi "rmi://localhost/server1")))

(def *should-test* true)

(when *should-test*

  (deftest test-out-rd
    (let [m (build-mulgara)]
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c]])
      (let [res (rd (ts :ts) [[?s ?p ?o]])]
        (is (= (resource-id (first (ffirst (rd (ts :ts) [[?s ?p ?o]])))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (ffirst (rd (ts :ts) [[?s ?p ?o]])))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (ffirst (rd (ts :ts) [[?s ?p ?o]])) 2)) "http://plaza.org/ontologies/c")))
      (clean (ts :ts))
      (unregister-ts :ts)
      (breathe)))

  (deftest test-out-rd-2
    (let [m (build-mulgara)]
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c]])
      (out (ts :ts) [[:a :b :d]])
      (let [res (rd (ts :ts) [[?s ?p ?o]])]
        (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/d"))
        (is (= (resource-id (first (first (second res)))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (first (second res)))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (first (second res)) 2)) "http://plaza.org/ontologies/c")))
      (unregister-ts :ts))
    (breathe))

  (deftest test-out-in-rd
    (let [m (build-mulgara)]
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c]])
      (let [res (in (ts :ts) [[?s ?p ?o]])
            resb (rd (ts :ts) [[?s ?p ?o]])]
        (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
        (is (empty? resb)))
      (clean (ts :ts))
      (unregister-ts :ts)
      (breathe)))

  (deftest test-swap
    (let [m (build-mulgara)]
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c]])
      (let [res (swap (ts :ts) [[?s ?p :c]] [[:e :f :g]])
            resb (rd (ts :ts) [[?s ?p ?o]])]
        (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
        (is (= (resource-id (first (ffirst resb))) "http://plaza.org/ontologies/e"))
        (is (= (resource-id (second (ffirst resb))) "http://plaza.org/ontologies/f"))
        (is (= (resource-id (nth (ffirst resb) 2)) "http://plaza.org/ontologies/g")))
      (clean (ts :ts))
      (unregister-ts :ts)
      (breathe)))

  (deftest test-out-rdb
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (def-ts :ts2 (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (let [prom (promise)]
        (.start (Thread. (fn [] (let [res (rdb (ts :ts) [[?s ?p ?o]])]  (deliver prom [res (ts :ts)])))))
        (breathe)
        (out (ts :ts2) [[:a :b :c]])
        (let [[res tsp] @prom]
          (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
          (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
          (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
          (clean (ts :ts))
          (unregister-ts :ts)
          (unregister-ts :ts2)
          (breathe)))))

  (deftest test-out-inb
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (def-ts :ts2 (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (let [prom (promise)]
        (.start (Thread. (fn [] (let [res (inb (ts :ts) [[?s ?p ?o]])]  (deliver prom [res (ts :ts)])))))
        (out (ts :ts) [[:a :b :c]])
        (let [[res tsp] @prom]
          (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
          (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
          (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
          (let [res (rd (ts :ts) [[?s ?p ?o]])]
            (is (empty? res)))
          (clean (ts :ts))
          (unregister-ts :ts)
          (unregister-ts :ts2)
          (breathe)))))

(deftest test-notify-out
  (let [m (build-mulgara)
        m2 (build-mulgara)]
    (def-ts :ts (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
    (def-ts :ts2 (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
    (clean-ts (ts :ts))
    (let [q (java.util.concurrent.LinkedBlockingQueue.)]
      (.start (Thread. #(notify (ts :ts2) :out [[?s :e ?o]] (fn [triples]  (.put q triples)))))

      (out (ts :ts) [[:a :b :c]])
      (out (ts :ts) [[:a :e :c]])
      (out (ts :ts) [[:a :b :f]])
      (out (ts :ts) [[:a :e :f]])

      (is (not (nil? (.take q))))
      (is (not (nil? (.take q))))

      (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
        (is (empty? res))))
    (clean (ts :ts))
    (unregister-ts :ts)
    (unregister-ts :ts2)
    (breathe)))

  ); end *should-test*

;
;(deftest test-notify-in
;  (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
;    (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
;    (clean-ts (ts :ts))
;    (let [q (java.util.concurrent.LinkedBlockingQueue.)]
;      (out (ts :ts) [[:a :e :c]])
;      (out (ts :ts) [[:a :e :d]])
;      (out (ts :ts) [[:a :b :c]])
;
;      (.start (Thread. #(notify (ts :ts) :in [[?s :e ?o]] (fn [triples] (.put q triples)))))
;
;      (in (ts :ts) [[?s :e :c]])
;      (in (ts :ts) [[?s :b :c]])
;      (in (ts :ts) [[?s :e :d]])
;
;      (is (not (nil? (.take q))))
;      (is (not (nil? (.take q))))
;
;      (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
;        (is (empty? res))))
;    (unregister-ts :ts)
;    (stop-server *s*)
;    (breathe)))
;
