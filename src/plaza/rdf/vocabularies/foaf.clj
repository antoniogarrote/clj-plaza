;; @author Antonio Garrote
;; @email antoniogarrote@gmail.com
;; @date 09.06.2010

(ns plaza.rdf.vocabularies.foaf
  (:use
   [plaza.rdf.core]
   [plaza.rdf.schemas]))

;; FOAF vocabulary
;; @see http://xmlns.com/foaf/spec/

(defonce foaf "http://xmlns.com/foaf/0.1/")

(defonce foaf:Agent "http://xmlns.com/foaf/0.1/Agent")
(defonce foaf:Person "http://xmlns.com/foaf/0.1/Person")
(defonce foaf:name "http://xmlns.com/foaf/0.1/name")
(defonce foaf:gender "http://xmlns.com/foaf/0.1/gender")
(defonce foaf:holdsAccount "http://xmlns.com/foaf/0.1/holdsAccount")
(defonce foaf:birthday "http://xmlns.com/foaf/0.1/birthday")
(defonce foaf:openid "http://xmlns.com/foaf/0.1/openid")
(defonce foaf:status "http://xmlns.com/foaf/0.1/status")
(defonce foaf:nick "http://xmlns.com/foaf/0.1/nick")
(defonce foaf:title "http://xmlns.com/foaf/0.1/title")
(defonce foaf:homepage "http://xmlns.com/foaf/0.1/homepage")
(defonce foaf:mbox "http://xmlns.com/foaf/0.1/mbox")
(defonce foaf:mbox_sha1sum "http://xmlns.com/foaf/0.1/mbox_sha1sum")
(defonce foaf:img "http://xmlns.com/foaf/0.1/img")
(defonce foaf:depiction "http://xmlns.com/foaf/0.1/depiction")
(defonce foaf:surname "http://xmlns.com/foaf/0.1/surname")
(defonce foaf:familyName "http://xmlns.com/foaf/0.1/familyName")
(defonce foaf:givenName "http://xmlns.com/foaf/0.1/givenName")
(defonce foaf:firstName "http://xmlns.com/foaf/0.1/firstName")
(defonce foaf:lastName "http://xmlns.com/foaf/0.1/lastName")
(defonce foaf:weblog "http://xmlns.com/foaf/0.1/weblog")
(defonce foaf:knows "http://xmlns.com/foaf/0.1/knows")
(defonce foaf:interest "http://xmlns.com/foaf/0.1/interest")
(defonce foaf:currentProject "http://xmlns.com/foaf/0.1/currentProject")
(defonce foaf:pastProject "http://xmlns.com/foaf/0.1/pastProject")
(defonce foaf:plan "http://xmlns.com/foaf/0.1/plan")
(defonce foaf:based_near "http://xmlns.com/foaf/0.1/based_near")
(defonce foaf:age "http://xmlns.com/foaf/0.1/age")
(defonce foaf:workplaceHomepage "http://xmlns.com/foaf/0.1/workplaceHomepage")
(defonce foaf:workInfoHomepage "http://xmlns.com/foaf/0.1/workInfoHomepage")
(defonce foaf:schoolHomepage "http://xmlns.com/foaf/0.1/schoolHomepage")
(defonce foaf:topic_interest "http://xmlns.com/foaf/0.1/topic_interest")
(defonce foaf:publications "http://xmlns.com/foaf/0.1/publications")
(defonce foaf:geekcode "http://xmlns.com/foaf/0.1/geekcode")
(defonce foaf:myersBriggs "http://xmlns.com/foaf/0.1/myersBriggs")
(defonce foaf:dnaChecksum "http://xmlns.com/foaf/0.1/dnaChecksum")
(defonce foaf:OnlineAccount "http://xmlns.com/foaf/0.1/OnlineAccount")
(defonce foaf:OnlineChatAccount "http://xmlns.com/foaf/0.1/OnlineChatAccount")
(defonce foaf:OnlineEcommerceAccount "http://xmlns.com/foaf/0.1/OnlineEcommerceAccount")
(defonce foaf:OnlineGamingAccount "http://xmlns.com/foaf/0.1/OnlineGamingAccount")
(defonce foaf:account "http://xmlns.com/foaf/0.1/account")
(defonce foaf:accountServiceHomepage "http://xmlns.com/foaf/0.1/accountServiceHomepage")
(defonce foaf:accountName "http://xmlns.com/foaf/0.1/accountName")
(defonce foaf:icqChatID "http://xmlns.com/foaf/0.1/icqChatID")
(defonce foaf:msnChatID "http://xmlns.com/foaf/0.1/msnChatID")
(defonce foaf:aimChatID "http://xmlns.com/foaf/0.1/aimChatID")
(defonce foaf:jabberID "http://xmlns.com/foaf/0.1/jabberID")
(defonce foaf:yahooChatID "http://xmlns.com/foaf/0.1/yahooChatID")
(defonce foaf:skypeID "http://xmlns.com/foaf/0.1/skypeID")
(defonce foaf:Project "http://xmlns.com/foaf/0.1/Project")
(defonce foaf:Organization "http://xmlns.com/foaf/0.1/Organization")
(defonce foaf:Group "http://xmlns.com/foaf/0.1/Group")
(defonce foaf:member "http://xmlns.com/foaf/0.1/member")
(defonce foaf:membershipClass "http://xmlns.com/foaf/0.1/membershipClass")
(defonce foaf:Document "http://xmlns.com/foaf/0.1/Document")
(defonce foaf:Image "http://xmlns.com/foaf/0.1/Image")
(defonce foaf:PersonalProfileDocument "http://xmlns.com/foaf/0.1/PersonalProfileDocument")
(defonce foaf:topic "http://xmlns.com/foaf/0.1/topic")
(defonce foaf:primaryTopicOf "http://xmlns.com/foaf/0.1/primaryTopicOf")
(defonce foaf:primaryTopic "http://xmlns.com/foaf/0.1/primaryTopic")
(defonce foaf:tipjar "http://xmlns.com/foaf/0.1/tipjar")
(defonce foaf:sha1 "http://xmlns.com/foaf/0.1/sha1")
(defonce foaf:made "http://xmlns.com/foaf/0.1/made")
(defonce foaf:thumbnail "http://xmlns.com/foaf/0.1/thumbnail")
(defonce foaf:logo "http://xmlns.com/foaf/0.1/logo")

(register-rdf-ns :foaf foaf)


(defonce foaf:Agent-schema
  (make-rdfs-schema foaf:Agent
                    :weblog           {:uri foaf:weblog       :range foaf:Document }
                    :gender           {:uri foaf:gender       :range :string }
                    :holdsAccount     {:uri foaf:holdsAccount :range foaf:OnlineAccount }
                    :birthday         {:uri foaf:birthday     :range :date }
                    :age              {:uri foaf:age          :range :integer }
                    :made             {:uri foaf:made         :range "http://www.w3.org/2002/07/owl#Thing"}
                    :skypeID          {:uri foaf:skypeID      :range :string}
                    :msnChatID        {:uri foaf:msnChatID    :range :string}
                    :icqChatID        {:uri foaf:icqChatID    :range :string}
                    :mbox             {:uri foaf:mbox         :range "http://www.w3.org/2002/07/owl#Thing"}
                    :status           {:uri foaf:status       :range :string}
                    :mbox_sha1sum     {:uri foaf:mbox_sha1sum :range :string}
                    :account          {:uri foaf:account      :range foaf:OnlineAccount}
                    :yahooChatID      {:uri foaf:yahooChatID  :range :string}
                    :aimChatID        {:uri foaf:aimChatID    :range :string}
                    :jabberID         {:uri foaf:jabberID     :range :string}
                    :openid           {:uri foaf:openid       :range :string }
                    :tipjar           {:uri foaf:tipjar       :range :string}))


(defonce foaf:OnlineAccount-schema
  (make-rdfs-schema foaf:OnlineAccount
                    :accountName {:uri foaf:accountName :range :string }
                    :accountServiceHomepage {:uri foaf:accountServiceHomepage :range foaf:Document }))


(defonce foaf:Document-schema
  (make-rdfs-schema foaf:Document
                    :sha1         {:uri foaf:sha1          :range "http://www.w3.org/2002/07/owl#Thing"}
                    :topic        {:uri foaf:topic         :range "http://www.w3.org/2002/07/owl#Thing"}
                    :primaryTopic {:uri foaf:primaryTopic  :range "http://www.w3.org/2002/07/owl#Thing"}))
