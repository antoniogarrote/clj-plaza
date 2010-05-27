;; @author Antonio Garote
;; @email antoniogarrote@gmail.com
;; @date 25.05.2010

(ns plaza.examples.chat
  (:gen-class)
  (:import (javax.swing JFrame JTextField JButton JPanel BoxLayout BorderFactory)
           (java.awt.event ActionListener WindowAdapter)
           (java.awt GridLayout BorderLayout Dimension))
  (:use (plaza.rdf core predicates sparql)
        (plaza.triple-spaces core server)
        (plaza.rdf.implementations jena)
        (plaza utils)))

;; we start Plaza using Jena as the implementation
(init-jena-framework)

;; ontology
(defonce message "http://plaza.org/examples/Message")
(defonce content "http://plaza.org/examples/content")
(defonce author "http://plaza.org/examples/author")
(defonce user "http://plaza.org/examples/User")
(defonce nick "http://plaza.org/examples/nick")

;; Configuration and shared objects
(defonce *login* (ref "my nick"))
(defonce chatArea (javax.swing.JTextArea.))
(defonce set-chat-text (agent chatArea))
(defonce userList (javax.swing.JTextArea.))
(defonce set-userlist-text (agent userList))

(defn- update-users
  "Updates the list of connected users"
  ([triples]
     (doseq [ts triples]
       (let [author (literal-value (o (first (filter (tc (predicate? (uri? nick))) ts))))]
         (println (str "*** agent user-login - got triples: " author))
         (send set-userlist-text (fn [ta] (let [old-text (.getText ta)]
                                            (.setText ta (str old-text "\r\n " author)) ta)))))))


;; Local triple spaces
(def-ts :to-publish (make-basic-triple-space))
;; Remote triple space
(defn start-connection
  ([host port rabbit-host rabbit-port rabbit-user rabbit-password]
     (def-ts :chat (make-remote-triple-space "chat" :ts-host host :ts-port port
                                             :username rabbit-user :password rabbit-password
                                             :host rabbit-host :port rabbit-password))))

;; Agents
(def-agent :publisher
  (loop []
    (println "*** agent publisher - starting")
    (let [msgs (inb (ts :to-publish) [[?s rdf:type message] [?s content ?c]])]
      (doseq [m msgs]
        (let [msg-txt (literal-value (o (first (filter (tc (predicate? (uri? content))) m))))
              blank (b)]
          (println (str "*** agent publisher - sending: " msg-txt))
          (out (ts :chat) [[blank rdf:type message]
                           [blank author (l @*login*)]
                           [blank content (l msg-txt)]])))
      (recur))))

(def-agent :updater
  (println "*** agent updater - starting")
  (notify (ts :chat) :out [[?s rdf:type message]
                           [?s author ?a]
                           [?s content ?c]]
          #(let [msg-txt (literal-value (o (first (filter (tc (predicate? (uri? content))) (first %1)))))
                 author (literal-value (o (first (filter (tc (predicate? (uri? author))) (first %1)))))]
             (println (str "*** agent updater - got triples: " %1))
             (send set-chat-text (fn [ta] (let [old-text (.getText ta)]
                                            (.setText ta (str old-text "\r\n " author ": " msg-txt)) ta))))))

(def-agent :user-login
  (println "*** agent user-login - starting")
  (notify (ts :chat) :out [[?s rdf:type user]
                           [?s nick ?u]]
          update-users))

(def-agent :user-logout
  (println "*** agent user-logout - starting")
  (notify (ts :chat) :in [[?s rdf:type user]
                          [?s nick ?u]]
          #(let [author (literal-value (o (first (filter (tc (predicate? (uri? nick))) (first %1)))))]
             (println (str "*** agent user-logout - got triples: " %1))
             (send set-userlist-text (fn [ta] (let [old-text (clojure.contrib.str-utils2/split-lines (.getText ta))
                                                    new-text (reduce (fn [ac some-nick]  (if (not (= (.indexOf some-nick author) -1)) ac (str ac some-nick "\r\n"))) "" old-text)]
                                                (.setText ta new-text) ta))))))

;; GUI
(defn build-gui
  "Main function, sets up the frame and glues everything together"
  []
  (let [frame (JFrame. "Chat")
        frameUsers (JFrame. "Users")
        mainPanel  (javax.swing.JPanel. (BorderLayout. 0 1))
        bottomPanel (javax.swing.JPanel.)
        inputText (javax.swing.JTextField. "write here...")
        sendText (javax.swing.JButton. "send")
        jScrollPane1 (javax.swing.JScrollPane.)]

    ;; send button
    (.addActionListener sendText
                        (proxy [ActionListener] []
                          (actionPerformed [e]
                                           (let [blank-node (b)]
                                             (out (ts :to-publish) [[blank-node content (l (.getText inputText))]
                                                                    [blank-node rdf:type message]])
                                             (.setText inputText "")))))

    ;; bottom panel
    (doto bottomPanel
      (.setLayout (BorderLayout. 5 0))
      (.add inputText BorderLayout/CENTER)
      (.add sendText BorderLayout/LINE_END))

    ;; chat area
    (doto chatArea
      (.setSize 417 150))

    ;; chat frame
    (.addWindowListener frame
                        (proxy [WindowAdapter] []
                          (windowClosing [we]
                                         (println "*** loging user out")
                                         (in (ts :chat) [[(str "http://plaza.org/examples/chat/users/" @*login*) rdf:type user]
                                                         [(str "http://plaza.org/examples/chat/users/" @*login*) nick ?o]])
                                         (System/exit 0))))
    (doto frame
      (.setLayout (BorderLayout. 0 1))
      (.add chatArea BorderLayout/CENTER)
      (.add bottomPanel BorderLayout/PAGE_END)
      (.setResizable false)
      (.setSize 417 318)
      (.setVisible true))

    ;; user list
    (doto userList
      (.setSize 200 250))

    ;; user list frame
    (doto frameUsers
      (.setLayout (BorderLayout. 0 1))
      (.add userList BorderLayout/CENTER)
      (.setResizable false)
      (.setSize 200 250)
      (.setVisible true))))

(defonce *default-args* {:ts-host "localhost" :ts-port "7555" :host "localhost" :port "5672" :username "guest" :password "guest"})

(defn start-app [user-nick orig-args]
  ;; setup the app
  (send set-chat-text #(do (.setText %1 "") %1))
  (dosync (ref-set *login* user-nick))
  (let [final-args (check-default-values orig-args *default-args*)]

    (start-connection (:ts-host final-args) (Integer/parseInt (:ts-port final-args)) (:host final-args) (Integer/parseInt (:port final-args)) (:username final-args) (:password final-args)))

  ;; we insert our user in the triplespace and load the initial list of users
  (out (ts :chat) [[(str "http://plaza.org/examples/chat/users/" @*login*) rdf:type user]
                   [(str "http://plaza.org/examples/chat/users/" @*login*) nick (l @*login*)]])
  (update-users (rd (ts :chat) [[?s rdf:type user] [?s nick ?u]]))

  ;; starting the agents
  (spawn! :publisher)
  (spawn! :updater)
  (spawn! :user-logout)
  (spawn! :user-login)

  ;; building the gui
  (build-gui))
