(ns plaza.triple-spaces.core-jena-test
  (:use [plaza.rdf core sparql] :reload-all)
  (:use [plaza utils] :reload-all)
  (:use [plaza.rdf.implementations jena] :reload-all)
  (:use [plaza.triple-spaces core] :reload-all)
  (:use [clojure.test]))

;; Testing with Jena
(init-jena-framework)

(deftest test-basic-write-read
  (let [*ts* ((make-basic-triple-space))]
    (out *ts* [[:a :b :c] [:d :e (l "test")] [:g :h (d 2)]])
    (is (= 3 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p :c]])))))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p (l "test")]])))))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p (d 2)]])))))))

(deftest test-basic-write-read-blank
  (let [*ts* ((make-basic-triple-space))]
    (out *ts* [[:a :b (b :m)]])
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p (b :m)]])))))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p (b :n)]])))))))

(deftest test-basic-write-read-filter
  (let [*ts* ((make-basic-triple-space))]
    (out *ts* [[:a :b (d 5)]])
    (out *ts* [[:a :b (d 10)]])
    (is (= 2 (count (flatten-1 (rd *ts* [[?s ?p ?o]] [(f :> ?o (d 1))])))))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p ?o]] [(f :> ?o (d 1)) (f :> ?o (d 7))])))))))

(deftest test-basic-write-in
  (let [*ts* ((make-basic-triple-space))]
    (out *ts* [[:a :b :c]])
    (is (= 1 (count (flatten-1 (in *ts* [[?s ?p ?o]])))))
    (is (= 0 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))))

(deftest test-basic-in-2
  (let [*ts* ((make-basic-triple-space))]
    (out *ts* [[:a :b :c] [:a :f :g]])
    (is (= 2 (count (flatten-1 (in *ts* [[?s ?p ?o] [?s :f :g]])))))
    (is (= 0 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))))

(deftest test-rdb-1
  (let [*ts* ((make-basic-triple-space))
        sync (promise)]
    (.start (Thread. (fn [] (do (rdb *ts* [[?s ?p :c]]) (deliver sync :done)))))
    (out *ts* [[:d :e :c]])
    (is (= :done @sync))
    (is (= 1 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))))

(deftest test-inb-1
  (let [*ts* ((make-basic-triple-space))
        sync (promise)]
    (.start (Thread. (fn [] (do (inb *ts* [[?s ?p :c]]) (deliver sync :done)))))
    (out *ts* [[:d :e :c]])
    (is (= :done @sync))
    (is (= 0 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))))

(deftest test-swap-1
  (let [*ts* ((make-basic-triple-space))]
    (out *ts* [[:d :e :c]])
    (is (= 1 (count (flatten-1 (swap *ts* [[:d ?p ?o]] [[:a :b :c]])))))
    (let [res (flatten-1 (rd *ts* [[?s ?p ?o]]))]
      (is (= (count res)))
      (is (.endsWith (resource-uri (first (first res))) "a")))))

(deftest test-swap-2
  (let [*ts* ((make-basic-triple-space))
        sync (promise)]
    (.start (Thread. (fn [] (do (inb *ts* [[?s ?p :c]]) (deliver sync :done)))))
    (out *ts* [[:e :f :g]])
    (swap *ts* [[?s ?p ?o]] [[:a :b :c]])
    (is (= :done @sync))
    (is (= 0 (count (flatten-1 (rd *ts* [[?s ?p ?o]])))))))

(deftest test-notify-1
  (let [*ts* ((make-basic-triple-space))
        counter (ref 0)
        sync-before (promise)
        sync-after (promise)]
    (out *ts* [[:a :b :c]])
    (.start (Thread. (fn [] (do (deliver sync-before :done)
                                (notify *ts* :in [[?s ?p ?o]]
                                        (fn [results]
                                          (dosync
                                           (if (= @counter 1)
                                             (do (deliver sync-after :done)
                                                 :finish)
                                             (alter counter (fn [old] (+ old 1)))))))))))
    @sync-before
    (Thread/sleep 2000)
    (rd *ts* [[?s ?p :c]])
    (rd *ts* [[?s ?p :c]])
    @sync-after
    (dosync (is (= @counter 1)))))

(deftest test-notify-2
  (let [*ts* ((make-basic-triple-space))
        counter (ref 0)
        sync-before (promise)
        sync-after (promise)]
    (out *ts* [[:a :b :c] [:a :b :d]])
    (.start (Thread. (fn [] (do (deliver sync-before :done)
                                (notify *ts* :in [[?s ?p ?o]]
                                        (fn [results]
                                          (dosync
                                           (if (= @counter 1)
                                             (do (deliver sync-after :done)
                                                 :finish)
                                             (alter counter (fn [old] (+ old 1)))))))))))
    @sync-before
    (Thread/sleep 2000)
    (in *ts* [[?s ?p :c]])
    (in *ts* [[?s ?p :d]])
    @sync-after
    (dosync (is (= @counter 1)))))

(deftest test-notify-3
  (let [*ts* ((make-basic-triple-space))
        counter (ref 0)
        sync-before (promise)
        sync-after (promise)]
    (out *ts* [[:a :b :c]])
    (.start (Thread. (fn [] (do (deliver sync-before :done)
                                (notify *ts* :in [[?s ?p ?o]]
                                        (fn [results]
                                          (dosync
                                           (if (= @counter 1)
                                             (do (deliver sync-after :done)
                                                 :finish)
                                             (alter counter (fn [old] (+ old 1)))))))))))
    @sync-before
    (Thread/sleep 2000)
    (rdb *ts* [[?s ?p :c]])
    (rdb *ts* [[?s ?p :c]])
    @sync-after
    (dosync (is (= @counter 1)))))

(deftest test-swap-3
  (let [*ts* ((make-basic-triple-space))
        sync-before (promise)
        sync-after (promise)]
    (out *ts* [[:a :b :c]])
    (.start (Thread. (fn [] (deliver sync-before :done) (rdb *ts*  [[?s ?p :d]]) (deliver sync-after :done))))
    @sync-before
    (Thread/sleep 2000)
    (swap *ts* [[?s ?p :c]] [[:a :b :d]])
    @sync-after
    (is true)))

(deftest test-swap-4
  (let [*ts* ((make-basic-triple-space))
        sync-before-1 (promise)
        sync-before-2 (promise)
        sync-after-1 (promise)
        sync-after-2 (promise)]
    (out *ts* [[:a :b :c]])
    (.start (Thread. (fn [] (deliver sync-before-1 :done) (rdb *ts*  [[?s ?p :d]]) (deliver sync-after-1 :done))))
    (.start (Thread. (fn [] (deliver sync-before-2 :done) (rdb *ts*  [[?s ?p :d]]) (deliver sync-after-2 :done))))
    @sync-before-1
    @sync-before-2
    (Thread/sleep 2000)
    (swap *ts* [[?s ?p :c]] [[:a :b :d]])
    @sync-after-1
    @sync-after-2
    (is true)))

(deftest test-notify-4
  (let [*ts* ((make-basic-triple-space))
        sync-before (promise)
        sync-after (promise)]
    (.start (Thread. (fn [] (do (deliver sync-before :done)
                                (notify *ts* :out [[?s ?p ?o]]
                                        (fn [results]
                                          (do (deliver sync-after :done)
                                              :finish)))))))
    @sync-before
    (Thread/sleep 2000)
    (out *ts* [[:a :b :c]])
    @sync-after
    (is true)))
