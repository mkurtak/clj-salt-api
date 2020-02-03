;; Copyright (c) Cognitect, Inc.
;; All rights reserved.
(ns salt.retry
  "Retry was copied and modified version of retry.clj from https://github.com/cognitect-labs/aws-api. This software is licensed under same license and original Copyright statements included in this file."
  (:require [clojure.core.async :as a]
            [salt.core :as s]))

(defn default-retriable?
  "Returns true if response is categorized as retriable or is exception with ex-data
  which is categorized as retriable."
  [response]
  (let [resp (or (ex-data response) response)]
    (= :retriable (::s/response-category resp))))

(defn capped-exponential-backoff
  "Returns a function of the num-retries (so far), which returns the
  lesser of max-backoff and an exponentially increasing multiple of
  base, or nil when (>= num-retries max-retries).
  See with-retry to see how it is used.
  Alpha. Subject to change."
  [base max-backoff max-retries]
  (fn [num-retries]
    (when (< num-retries max-retries)
      (min max-backoff
           (* base (bit-shift-left 1 num-retries))))))

(def default-backoff
  "Returns (capped-exponential-backoff 100 20000 3).
  Alpha. Subject to change."
  (capped-exponential-backoff 100 20000 3))

(defn with-retry
  "For internal use. Do not call directly.
  Calls req-fn, a *non-blocking* function that wraps some operation
  and returns a channel. When the response to req-fn is retriable?
  and backoff returns an int, waits backoff ms and retries, otherwise
  puts response on resp-chan.
  Resp-chan is closed when src-chan is closed."
  ([req-fn resp-chan] (with-retry req-fn resp-chan default-retriable? default-backoff))
  ([req-fn resp-chan retriable? backoff]
   (a/go-loop [retries 0
               src-chan (req-fn)]
     (if-let [resp (a/<! src-chan)]
       (if (retriable? resp)
         (if-let [bo (backoff retries)]
           (do
             (a/<! (a/timeout bo))
             (a/close! src-chan)
             (recur (inc retries) (req-fn)))
           (do
             (a/>! resp-chan resp)
             (recur retries src-chan)))
         (do
           (a/>! resp-chan resp)
           (recur retries src-chan)))
       (a/close! resp-chan)))
   resp-chan))
