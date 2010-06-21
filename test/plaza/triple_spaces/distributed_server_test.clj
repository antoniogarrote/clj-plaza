(ns plaza.triple-spaces.distributed-server-test
  (:use
   [plaza.rdf core]
   [plaza.triple-spaces distributed-server]
   [plaza.triple-spaces server]
   [plaza.triple-spaces core]
   [plaza.rdf core sparql predicates]
   [plaza.rdf.implementations.stores.mulgara]
   [plaza.rdf.implementations jena] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)

(defn- clean-ts
  ([ts] (in ts [[?s ?p ?o]])))

(defn- breathe
  ([] (Thread/sleep 2000)))

(defn- build-mulgara
  ([] (build-model :mulgara :rmi "rmi://localhost/server1")))

(defn- clean-redis
  [] (redis/with-server {:host "localhost" :port 6379 :db "testdist"} (redis/flushdb)))

(defonce *should-test* false)

(when-not *should-test*
  (println "********* distributed triple space tests DISABLED *********")
  (println " To enable these tests start a redis localhost instance at port 6379 \n and a default Mulgara triple repository,")
  (println " change the value of the *should-test* symbol in the test file")
  (println "**********************************************************"))

(when *should-test*

  (println "********* distributed triple space tests ENABLED *********")
  (println " A redis localhost instance must be running at port 6379 \n and a default Mulgara triple repository,")
  (println " change the value of the *should-test* symbol in the test file to disable")
  (println "**********************************************************")

  (deftest test-out-rd
    (println "***************************************************\n 1 \n******************************************************")
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
    (println "***************************************************\n 2 \n******************************************************")
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

  (deftest test-out-rd3
    (println "***************************************************\n 3 \n******************************************************")
    (let [m (build-mulgara)]
      (clean-redis)
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c] [:d :e :f]])
      (let [res (rd (ts :ts) [[?s ?p ?o]])]
        (is (= 2 (count res)))
        (is (= 2 (count (plaza.utils/flatten-1 res)))))
      (clean (ts :ts))
      (unregister-ts :ts)
      (breathe)))

  (deftest test-out-in-rd
    (println "***************************************************\n 4 \n******************************************************")
    (let [m (build-mulgara)]
      (clean-redis)
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (out (ts :ts) [[:a :b :c]])
      (let [res (in (ts :ts) [[?s ?p ?o]])
            resb (rd (ts :ts) [[?s ?p ?o]])]
        (println (str "NI PUTA IDEA: " (nth (ffirst res) 0) " , " (nth (ffirst res) 1) " , " (nth (ffirst res) 2)))
        (is (= (resource-id (first (ffirst res))) "http://plaza.org/ontologies/a"))
        (is (= (resource-id (second (ffirst res))) "http://plaza.org/ontologies/b"))
        (is (= (resource-id (nth (ffirst res) 2)) "http://plaza.org/ontologies/c"))
        (is (empty? resb)))
      (clean (ts :ts))
      (unregister-ts :ts)
      (breathe)))

  (deftest test-swap
    (println "***************************************************\n 5 \n******************************************************")
    (let [m (build-mulgara)]
      (clean-redis)
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
    (println "***************************************************\n 6 \n******************************************************")
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (clean-redis)
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
    (println "***************************************************\n 7 \n******************************************************")
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (clean-redis)
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (def-ts :ts2 (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (let [prom (promise)]
        (.start (Thread. (fn [] (let [res (inb (ts :ts) [[?s ?p ?o]])]  (deliver prom [res (ts :ts)])))))
;        (breathe)
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

  (deftest test-out-inb2
    (println "***************************************************\n 8 \n******************************************************")
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (clean-redis)
      (def-ts :ts (make-distributed-triple-space "test" m :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (let [prom (promise)]
        (.start (Thread. (fn [] (let [res (inb (ts :ts) [[?s ?p ?o]])]  (deliver prom [res (ts :ts)])))))
        (breathe)
        (out (ts :ts) [[:a :b :c] [:d :e :f]])
        (let [[res tsp] @prom]
          (doseq [t res] (println (str "READ ->" t "<-")))
          (is (= 2 (count res)))
          (clean (ts :ts))
          (unregister-ts :ts)
          (breathe)))))

  (deftest test-notify-out
    (println "***************************************************\n 9 \n******************************************************")
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (clean-redis)
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

  (deftest test-notify-in
    (println "***************************************************\n 10 \n******************************************************")
    (let [m (build-mulgara)
          m2 (build-mulgara)]
      (clean-redis)
      (def-ts :ts (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (def-ts :ts2 (make-distributed-triple-space "test" m2 :redis-host "localhost" :redis-db "testdist" :redis-port 6379))
      (clean-ts (ts :ts))
      (let [q (java.util.concurrent.LinkedBlockingQueue.)]
        (out (ts :ts) [[:a :e :c]])
        (out (ts :ts) [[:a :e :d]])
        (out (ts :ts) [[:a :b :c]])

        (.start (Thread. #(notify (ts :ts2) :in [[?s :e ?o]] (fn [triples] (.put q triples)))))

        (in (ts :ts) [[?s :e :c]])
        (in (ts :ts) [[?s :b :c]])
        (in (ts :ts) [[?s :e :d]])

        (is (not (nil? (.take q))))
        (is (not (nil? (.take q))))

        (let [res (.poll q 1 java.util.concurrent.TimeUnit/SECONDS)]
          (is (empty? res))))
      (clean (ts :ts))
      (unregister-ts :ts)
      (unregister-ts :ts2)
      (breathe)))

  ); end *should-test*
