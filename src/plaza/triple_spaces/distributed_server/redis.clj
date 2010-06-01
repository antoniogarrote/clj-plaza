;; @author amitrahtore
;; @from http://github.com/amitrathore/redis-clojure/blob/master/src/redis.clj
;; @date 01.06.2010

(ns plaza.triple-spaces.distributed-server.redis
  (:refer-clojure :exclude [get set type keys sort])
  (:use  redis.internal))

(defmacro atomically
  "Execute all redis commands in body atomically, ie. sandwiched in a
  MULTI/EXEC statement. If an exception is thrown the EXEC command
  will be terminated by a DISCARD, no operations will be performed and
  the exception will be rethrown."
  [& body]
  `(do
     (plaza.triple-spaces.distributed-server.redis/multi)
     (try
      (do
        ~@body
        (plaza.triple-spaces.distributed-server.redis/exec))
      (catch Exception e#
        (plaza.triple-spaces.distributed-server.redis/discard)
        (throw e#)))))
;;
;; Commands
;;
(defcommands
  ;; MULTI/EXEC/DISCARD
  (multi       [] :inline)
  (exec        [] :inline)
  (discard     [] :inline)
)
