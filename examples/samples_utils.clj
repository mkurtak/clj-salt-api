;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns samples-utils
  (:require [clojure.core.async :as a]
            [salt.client :as c]))

(defn throw-err
  "Simple wrapper that throws exception if e is Throwable, otherwise just return e."
  [e]
  (if (instance? Throwable e) (throw e) e))

(defmacro <?
  "Like <! but throws errors."
  [ch]
  `(throw-err (a/<! ~ch)))

(defn <??
  "Like <!! but throws errors."
  [ch]
  (throw-err (a/<!! ch)))

(defmacro go-try
  "Like go but catches the first thrown error and returns it."
  [& body]
  (let [e (gensym)]
    `(a/go
       (try
         ~@body
         (catch Throwable ~e ~e)))))

(defn- print-vals
  [ch]
  (<?? (go-try (loop []
                 (when-let [res (<? ch)]
                   (println res)
                   (recur))))))

(defn print-and-close
  [client ch]
  (try
    (print-vals ch)
    (finally (c/close client))))
