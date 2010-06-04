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

(defn- clean-ts
  ([ts] (in ts [[?s ?p ?o]])))

(defn- breathe
  ([] (Thread/sleep 2000)))

(defonce *should-test* false)

(when-not *should-test*
  (println "********* remote triple space tests DISABLED *********")
  (println " To enable these tests start a RabbitMQ server with default configuraion")
  (println " change the value of the *should-test* symbol in the test file")
  (println "******************************************************"))


(when *should-test*

  (println "********* remote triple space tests ENABLED *********")
  (println " A triple space with name test will be started at port 7555. A RabbitMQ server must be running.")
  (println " Change the value of the *should-test* symbol in the test file to disable")
  (println "*****************************************************")


  (deftest test-out-rd
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c]])
      (let [res (rd (ts :ts) [[?s ?p ?o]])]
        (is (= (resource-id (first (ffirst (rd (ts :ts) [[?s ?p ?o]])))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (ffirst (rd (ts :ts) [[?s ?p ?o]])))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (ffirst (rd (ts :ts) [[?s ?p ?o]])) 2)) "http://plaza.org/ontologies/c")))
      (clean (ts :ts))
      (unregister-ts :ts)
      (stop-server *s*)
      (breathe)))

  (deftest test-out-rd-2
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
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
      (unregister-ts :ts)
      (stop-server *s*))
    (breathe))

  (deftest test-out-in-rd
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
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
      (stop-server *s*)
      (breathe)))

  (deftest test-swap
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
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
      (stop-server *s*)
      (breathe)))

  (deftest test-out-rdb
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
      (clean-ts (ts :ts))
      (let [prom (promise)]
        (.start (Thread. (fn [] (let [res (rdb (ts :ts) [[?s ?p ?o]])]  (deliver prom [res (ts :ts)])))))
        (out (ts :ts) [[:a :b :c]])
        (let [[res tsp] @prom]
          (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
          (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
          (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
          (clean (ts :ts))
          (clean tsp)
          (unregister-ts :ts)
          (stop-server *s*)
          (breathe)))))

  (deftest test-out-inb
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
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
          (clean tsp)
          (unregister-ts :ts)
          (stop-server *s*)
          (breathe)))))

  (deftest test-notify-out
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
      (clean-ts (ts :ts))
      (let [q (java.util.concurrent.LinkedBlockingQueue.)]
        (.start (Thread. #(notify (ts :ts) :out [[?s :e ?o]] (fn [triples]  (.put q triples)))))

        (out (ts :ts) [[:a :b :c]])
        (out (ts :ts) [[:a :e :c]])
        (out (ts :ts) [[:a :b :f]])
        (out (ts :ts) [[:a :e :f]])

        (is (not (nil? (.take q))))
        (is (not (nil? (.take q))))

        (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
          (is (empty? res))))
      (unregister-ts :ts)
      (stop-server *s*)
      (breathe)))

  (deftest test-notify-in
    (let [*s* (start-triple-server "test" 7555 (build-model :jena))]
      (def-ts :ts (make-remote-triple-space "test" :ts-host "localhost" :ts-port 7555))
      (clean-ts (ts :ts))
      (let [q (java.util.concurrent.LinkedBlockingQueue.)]
        (out (ts :ts) [[:a :e :c]])
        (out (ts :ts) [[:a :e :d]])
        (out (ts :ts) [[:a :b :c]])

        (.start (Thread. #(notify (ts :ts) :in [[?s :e ?o]] (fn [triples] (.put q triples)))))

        (in (ts :ts) [[?s :e :c]])
        (in (ts :ts) [[?s :b :c]])
        (in (ts :ts) [[?s :e :d]])

        (is (not (nil? (.take q))))
        (is (not (nil? (.take q))))

        (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
          (is (empty? res))))
      (unregister-ts :ts)
      (stop-server *s*)
      (breathe)))

  )
