(defproject plaza "0.0.5-SNAPSHOT"
  :description "Plaza framework for semantic distributed applications"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
                 [com.hp.hpl.jena/jena "2.6.2"]
                 [com.hp.hpl.jena/arq "2.8.3"]
                 [net.rootdev/java-rdfa "0.3"]
                 [compojure "0.4.0-RC3"]
                 [hiccup "0.2.5"]
                 [ring/ring-jetty-adapter "0.2.0"]
                 [nu.validator.htmlparser/htmlparser "1.2.0"]
                 [com.franz/openrdf-sesame-onejar "2.2"]
                 [commons-pool "1.5.4"]
                 [org.jboss.netty/netty       "3.2.0.BETA1"]
                 [com.rabbitmq/amqp-client "1.7.2"]
                 [log4j/log4j "1.2.14"]]
  :repositories { "jboss" "http://repository.jboss.org/nexus/content/groups/public/" }
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]
                     [clojure-http-client "1.1.0-SNAPSHOT"]
                     [autodoc "0.7.1"]
                     [lein-clojars         "0.5.0-SNAPSHOT"]]
  :main     plaza.core
  :autodoc {:name "clj-plaza", :page-title "clj-plaza distributed semantic systems library"
            :author "Antonio Garrote <antoniogarrote@gmail.com> <agarrote@usal.es>"
            :copyright "2010 (c) Antonio Garrote, under the MIT license"
            :web-home "http://antoniogarrote.github.com/clj-plaza/api"})
