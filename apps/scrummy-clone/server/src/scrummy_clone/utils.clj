;; @author Antonio Garrote
;; @date  30.06.2010

(ns scrummy-clone.utils
  (:require [clojure.contrib.seq-utils :as seq-utils]))


(defn beautify
  ([sentence]
     (let [hyphened (.toLowerCase (.replaceAll sentence " " "-"))
           valid "abcdefghijklmnopqrstuvwxyz-1234567890"]
       (reduce (fn [a i] (str a i)) (map #(if (seq-utils/includes? valid %1) %1 "") hyphened)))))

(defn add-property-post
  ([property-arg property-uri]
     (fn [request environment]
       (if (= (:method request) :post)
         (let [query (:query environment)
               id (:id environment)
               value (get (:params request) property-arg)
               query-p (conj query [id property-uri value])]
           (-> environment (assoc :query query-p)))
         environment))))
