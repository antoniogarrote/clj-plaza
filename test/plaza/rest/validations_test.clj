;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 12.07.2010

(ns plaza.rest.validations-test
  (:use
   [clojure.contrib.logging :only [log]]
   [plaza.rdf core]
   [plaza.rdf.implementations jena]
   [plaza.rest validations]
   [clojure.test])
  (:require
   [clojure.contrib.json :as json]))

(init-jena-framework)

(defonce test1 "http://test.com/test1")
(defonce test2 "http://test.com/test2")

(deftest test-validate-presence-property-urls
  (let [validation (validate-presence-property-urls [test1 test2])]
    (let [fake-request {:params {"format" "js3"}}
          fake-environment {:mapping [[test1 "b"] [test2 "b"]] :status 200 }
          fake-environmentp (validation fake-request fake-environment)]
      (is (= (:status fake-environmentp) 200))
      (is (nil? (:body fake-environmentp))))

    (let [fake-request {:params {"format" "js3"}}
          fake-environment {:status 200 :mapping []}
          fake-environmentp (validation fake-request fake-environment)
          body (json/read-json (:body fake-environmentp))]
      (is (= (:status fake-environmentp 403)))
      (is (= 2 (count body))))

    (let [fake-request {:params {"format" "js3"}}
          fake-environment {:status 200 :mapping [[test1 "b"]]}
          fake-environmentp (validation fake-request fake-environment)
          body (json/read-json (:body fake-environmentp))]
      (is (= (:status fake-environmentp 403)))
      (is (= 1 (count body)))
      (is (= test2 (str (first (first body))))))))
