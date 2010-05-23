(ns plaza.triple-spaces.server-test
  (:use
   [clojure.contrib.logging :only [log]]
   [saturnine]
   [plaza.triple-spaces server]
   [plaza.triple-spaces core]
   [plaza.rdf core sparql predicates]
   [plaza.rdf.implementations jena]
   [plaza.triple-spaces.server auxiliary] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)

(defn clean-ts
  ([ts] (in ts [[?s ?p ?o]])))

(defn breathe
  ([] (Thread/sleep 2000)))

(deftest test-out-rd
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (out *ts* [[:a :b :c]])
    (let [res (rd *ts* [[?s ?p ?o]])]
      (is (= (resource-id (first (ffirst (rd *ts* [[?s ?p ?o]])))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (ffirst (rd *ts* [[?s ?p ?o]])))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (ffirst (rd *ts* [[?s ?p ?o]])) 2)) "http://plaza.org/ontologies/c")))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

(deftest test-out-rd-2
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (out *ts* [[:a :b :c]])
    (out *ts* [[:a :b :d]])
    (let [res (rd *ts* [[?s ?p ?o]])]
      (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/d"))
      (is (= (resource-id (first (first (second res)))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (first (second res)))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (first (second res)) 2)) "http://plaza.org/ontologies/c")))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*))
  (breathe))

(deftest test-out-in-rd
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (out *ts* [[:a :b :c]])
    (let [res (in *ts* [[?s ?p ?o]])
          resb (rd *ts* [[?s ?p ?o]])]
      (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
      (is (empty? resb)))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

(deftest test-swap
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (out *ts* [[:a :b :c]])
    (let [res (swap *ts* [[?s ?p :c]] [[:e :f :g]])
          resb (rd *ts* [[?s ?p ?o]])]
      (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
      (is (= (resource-id (first (ffirst resb))) "http://plaza.org/ontologies/e"))
      (is (= (resource-id (second (ffirst resb))) "http://plaza.org/ontologies/f"))
      (is (= (resource-id (nth (ffirst resb) 2)) "http://plaza.org/ontologies/g")))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

(deftest test-out-rdb
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (let [prom (promise)]
      (.start (Thread. (fn [] (let [res (rdb *tsp* [[?s ?p ?o]])]  (deliver prom res)))))
      (out *ts* [[:a :b :c]])
      (is (= (resource-id (first (ffirst @prom))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (ffirst @prom))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (ffirst @prom) 2)) "http://plaza.org/ontologies/c")))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

(deftest test-out-inb
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (let [prom (promise)]
      (.start (Thread. (fn [] (let [res (inb *tsp* [[?s ?p ?o]])]  (deliver prom res)))))
      (out *ts* [[:a :b :c]])
      (is (= (resource-id (first (ffirst @prom))) "http://plaza.org/ontologies/a"))
      (is (= (resource-id (second (ffirst @prom))) "http://plaza.org/ontologies/b"))
      (is (= (resource-id (nth (ffirst @prom) 2)) "http://plaza.org/ontologies/c"))
      (let [res (rd *ts* [[?s ?p ?o]])]
        (is (empty? res))))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

(deftest test-notify-out
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (let [q (java.util.concurrent.LinkedBlockingQueue.)]
      (.start (Thread. #(notify *tsp* :out [[?s :e ?o]] (fn [triples]  (.put q triples)))))

      (out *ts* [[:a :b :c]])
      (out *ts* [[:a :e :c]])
      (out *ts* [[:a :b :f]])
      (out *ts* [[:a :e :f]])

      (is (not (nil? (.take q))))
      (is (not (nil? (.take q))))

      (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
        (is (empty? res))))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

(deftest test-notify-in
  (let [*s* (start-triple-server "test" 7555 (build-model :jena))
        *ts* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)
        *tsp* (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555)]

    (clean-ts *ts*)
    (let [q (java.util.concurrent.LinkedBlockingQueue.)]
      (out *ts* [[:a :e :c]])
      (out *ts* [[:a :e :d]])
      (out *ts* [[:a :b :c]])

      (.start (Thread. #(notify *tsp* :in [[?s :e ?o]] (fn [triples] (.put q triples)))))

      (in *ts* [[?s :e :c]])
      (in *ts* [[?s :b :c]])
      (in *ts* [[?s :e :d]])

      (is (not (nil? (.take q))))
      (is (not (nil? (.take q))))

      (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
        (is (empty? res))))
    (clean *ts*)
    (clean *tsp*)
    (stop-server *s*)
    (breathe)))

