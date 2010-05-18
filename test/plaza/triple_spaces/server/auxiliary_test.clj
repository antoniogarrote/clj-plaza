(ns plaza.triple-spaces.server.auxiliary-test
  (:use [plaza.triple-spaces.server auxiliary] :reload-all)
  (:use [clojure.test]))

(deftest test-parse-message
  (let [msg1 (parse-message (format-message {:operation "rdb" :client-id "test-client" :pattern "test-pattern" :value "test-value"}))
        msg2 (parse-message (format-message {:operation "rdb" :client-id "test-client" :pattern "" :value "test-value"}))
        msg3 (parse-message (format-message {:operation "rdb" :client-id "test-client" :pattern "test-pattern" :value ""}))]
    (is (= "rdb" (:operation msg1)))
    (is (= "test-client" (:client-id msg1)))
    (is (= "test-pattern" (:pattern msg1)))
    (is (= "test-value" (:value msg1)))

    (is (= "rdb" (:operation msg2)))
    (is (= "test-client" (:client-id msg2)))
    (is (= "" (:pattern msg2)))
    (is (= "test-value" (:value msg2)))

    (is (= "rdb" (:operation msg3)))
    (is (= "test-client" (:client-id msg3)))
    (is (= "test-pattern" (:pattern msg3)))
    (is (= "" (:value msg3)))))
