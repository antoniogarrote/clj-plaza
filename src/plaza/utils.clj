(ns plaza.utils)

(defn keyword-to-string
  "transforms a keyword into a string"
  ([k]
     (if (= (class k) String)
       k
       (let [sk (str k)]
         (.substring sk 1)))))

(defn fold-list
  "Transforms a list: [1 2 3 4] -> [[1 2] [3 4]]"
  ([c]
     (loop [xs c
            odd 0
            tmp []
            acum []]
       (if (empty? xs)
         acum
         (let [x (first xs)]
           (if (= odd 1)
             (recur (rest xs)
                    0
                    []
                    (conj acum (conj tmp x)))
             (recur (rest xs)
                    1
                    (conj tmp x)
                    acum)))))))

(defn show-java-methods
  ([obj]
     (let [ms (.. obj getClass getDeclaredMethods)
           max (alength ms)]
       (loop [count 0]
         (when (< count max)
           (let [m (aget ms count)
                 params (.getParameterTypes m)
                 params-max (alength params)
                 return-type (.getReturnType m)
                 to-show (str (loop [acum (str (.getName m) "(")
                                     params-count 0]
                                (if (< params-count params-max)
                                  (recur (str acum " " (aget params params-count))
                                         (+ params-count 1))
                                  acum))
                              " ) : " return-type)]
             (println (str to-show))
             (recur (+ 1 count))))))))

(defn flatten-1
  ([seq]
     (reduce (fn [acum item]
               (if (and (coll? item) (coll? (first item)))
                    (vec (concat acum item))
                    (conj acum item)))
             []
             seq)))
