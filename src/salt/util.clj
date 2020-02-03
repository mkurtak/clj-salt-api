(ns salt.util)

(defn throw-err
  "Simple wrapper that throws exception if e is Throwable, otherwise just return e."
  [e]
  (if (instance? Throwable e)
    (throw e)
    e))

(defmacro <? [ch]
  `(throw-err (clojure.core.async/<! ~ch)))

(defmacro <?? [ch]
  `(throw-err (clojure.core.async/<!! ~ch)))

(defmacro go-try
  "Asynchronously executes the body in a go block. Returns a channel which
   will receive the result of the body when completed or an exception if one
   is thrown."
  [& body]
  `(go (try ~@body (catch Throwable e# e#))))

(defn ?assoc
  "Same as assoc, but skip the assoc if v is nil"
  [m & kvs]
  (->> kvs
       (partition 2)
       (filter second)
       (map vec)
       (into m)))

(defn submap?
  "Is m1 a subset of m2?"
  [m1 m2]
  (if (and (map? m1) (map? m2))
    (every? (fn [[k v]] (and (contains? m2 k)
                             (submap? v (get m2 k))))
            m1)
    (= m1 m2)))
