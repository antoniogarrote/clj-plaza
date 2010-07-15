(ns borges.permissions-test
  (:use [plaza.rdf core sparql schemas] :reload-all)
  (:use [plaza utils] :reload-all)
  (:use [plaza.rdf.implementations jena] :reload-all)
  (:use [plaza.triple-spaces core] :reload-all)
  (:use [borges.permissions] :reload-all)
  (:use [borges.vocabulary] :reload-all)
  (:use [clojure.test]))

(init-jena-framework)
(init-vocabularies)

(def-ts :test (make-basic-triple-space))

(defn clean-ts
  ([] (in (ts :test) [[?s ?p ?o]])))

(defonce *test-resource* "http://plaza.tests.org/TestResource")
(defonce *test-resource-b* "http://plaza.tests.org/TestResourceB")

(defn setup-permissions
  ([] (out (ts :test)  [[:a  rdf:type brg:Admin]
                        [:a  rdf:type sioc:Role]
                        [:a  sioc:has_scope *test-resource*]
                        [:a  siocacc:has_permission brg:GetOperation]
                        [:a  siocacc:has_permission brg:PostOperation]
                        [:a  siocacc:has_permission brg:PutOperation]
                        [:a  siocacc:has_permission brg:DeleteOperation]
                        [:b  rdf:type brg:User]
                        [:b  rdf:type sioc:Role]
                        [:b  sioc:has_scope *test-resource*]
                        [:b  siocacc:has_permission brg:GetOperation]
                        [:b  siocacc:has_permission brg:PutOperation]
                        [:c  rdf:type brg:Visitor]
                        [:c  rdf:type sioc:Role]
                        [:c  sioc:has_scope *test-resource*]
                        [:c  siocacc:has_permission brg:GetOperation]
                        [:ab rdf:type brg:Admin]
                        [:ab rdf:type sioc:Role]
                        [:ab sioc:has_scope *test-resource-b*]
                        [:ab siocacc:has_permission brg:GetOperation]
                        [:ab siocacc:has_permission brg:PostOperation]
                        [:ab siocacc:has_permission brg:PutOperation]
                        [:ab siocacc:has_permission brg:DeleteOperation]
                        [:bb rdf:type brg:User]
                        [:bb rdf:type sioc:Role]
                        [:bb sioc:has_scope *test-resource-b*]
                        [:bb siocacc:has_permission brg:GetOperation]
                        [:bb siocacc:has_permission brg:PutOperation]
                        [:cb rdf:type brg:Visitor]
                        [:cb rdf:type sioc:Role]
                        [:cb sioc:has_scope *test-resource-b*]
                        [:cb siocacc:has_permission brg:GetOperation]])))


(deftest test-permissions-for
  (clean-ts)
  (setup-permissions)
  (is (= (permissions-for-resource *test-resource* (ts :test))
         {:admin [:delete :put :post :get], :user [:put :get], :visitor [:get]})))

(deftest test-load-default-permissions
  (clean-ts)
  (out (ts :test) (default-resource-roles))
  (is (= (permissions-for-resource sioc:Role (ts :test))
         {:admin [:delete :put :post :get]}))
  (is (= (permissions-for-resource sioc:UserAccount (ts :test))
         {:admin [:delete :put :post :get], :visitor [:post], :owner [:delete :put], :creator [:delete :put]})))
