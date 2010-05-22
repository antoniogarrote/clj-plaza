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


(deftest test-create-response
  (let [resp (response "test")]
    (is (= "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>test</ts:response>"
           resp))))

(deftest test-read-eos
  (do (read-eos (java.io.BufferedReader. (java.io.StringReader. "")))
      (is true)))

(deftest test-res-ts-response
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>test</ts:response>"))]
    (try
     (read-ts-response in)
     (is true)
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-res-ts-response-fail
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponsefoo\"><ts:tokensep/>success<ts:tokensep/>test</ts:response>"))]
    (try
     (read-ts-response in)
     (is false)
     (catch Exception ex (do (println (.getMessage ex))
                             (is (= (let [buffer (char-array 3)
                                          line (do (.mark in 3)
                                                   (.read in buffer 0 3)
                                                   (String. buffer))] line)
                                    "<ts"))
                             (is true))))))

(deftest test-res-ts-token-sep
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>test</ts:response>"))]
    (try
     (-> in
         (read-ts-response)
         (read-token-separator))
     (is true)
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-res-ts-response-fail
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep>success<ts:tokensep/>test</ts:response>"))]
    (try
     (-> in
         (read-ts-response)
         (read-token-separator))
     (is false)
     (catch Exception ex (do (println (.getMessage ex))
                             (is (= "<ts:to" (read-from-buffer in 6)))
                             (is true))))))

(deftest test-res-ts-success
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>test</ts:response>"))]
    (try
     (-> in
         (read-ts-response)
         (read-token-separator)
         (read-success))
     (is true)
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-res-ts-success-fail
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>failure<ts:tokensep/>test</ts:response>"))]
    (try
     (-> in
         (read-ts-response)
         (read-token-separator)
         (read-success))
     (is false)
     (catch Exception ex (do (println (.getMessage ex))
                             (is (= "failur" (read-from-buffer in 6)))
                             (is true))))))

(deftest test-res-return-success
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>test</ts:response>"))]
    (try
     (is (= (-> in
                (read-ts-response)
                (read-token-separator)
                (return-succes-failure))
            "success"))
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-res-return-failure
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>failure<ts:tokensep/>test</ts:response>"))]
    (try
     (is (=(-> in
               (read-ts-response)
               (read-token-separator)
               (return-succes-failure))
           "failure"))
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-res-return-blocking
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>blocking<ts:tokensep/>test</ts:response>"))]
    (try
     (is (=(-> in
               (read-ts-response)
               (read-token-separator)
               (return-succes-failure))
           "blocking"))
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-res-return-stanzas
  (let [in (java.io.BufferedReader. (java.io.StringReader. "<ts:response xmlns:ts=\"http://plaza.org/tsresponse\"><ts:tokensep/>success<ts:tokensep/>stanza<ts:tokensep/>stanza<ts:tokensep/>stanza</ts:response>"))]
    (try
     (is (=(-> in
               (read-ts-response)
               (read-token-separator)
               (read-success)
               (return-rdf-stanzas))
           '("stanza" "stanza" "stanza")))
     (catch Exception ex (do (println (.getMessage ex)) (is false))))))

(deftest test-pack-stanas
  (is (= (pack-stanzas ["a" "b"]) "a<ts:tokensep/>b")))
