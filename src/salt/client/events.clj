;; All rights reserved.
;; Copyright (c) Michal Kurťák
(ns ^:no-doc salt.client.events
  (:require [clojure.core.async :as a]))

(defn initial-op
  [correlation-id recv-chan]
  {:command :subscribe
   :correlation-id correlation-id
   :recv-chan recv-chan
   :body {:type :subscribe
          :correlation-id correlation-id
          :recv-chan recv-chan}})

(defn- handle-with-command
  [op command]
  (assoc op :command command))

(defn- unsubscribe
  [{:keys [:correlation-id :conn-chan :data-chan] :as op}]
  (assoc op
         :command :unsubscribe
         :body {:type :unsubscribe
                :correlation-id correlation-id
                :conn-chan conn-chan
                :data-chan data-chan}))

(defn- handle-connect
  [{expected-correlation-id :correlation-id :as op}
   {:keys [:type :correlation-id] :as response}]
  (if (= :connected type)
    (cond
      (or
       (= expected-correlation-id correlation-id)
       (= :all correlation-id)) (assoc op :command :receive)
      (= :none correlation-id) (assoc op :command :exit)
      :else (assoc op :command :connect))
    (if (nil? response)
      (assoc op :command :exit)
      (assoc op :command :error :body response))))

(defn- handle-receive-data
  [op response]
  (assoc op :command :send :body response))

(defn- handle-receive-connection
  [op {:keys [:correlation-id]}]
  (if (= :none correlation-id)
    (assoc op :command :exit)
    (assoc op :command :receive)))

(defn- handle-receive
  [op [channel {:keys [:type] :as msg}]]
  (case channel
    :receive (if (instance? Throwable msg)
               (assoc op :command :error :body msg)
               (if (= :connected type)
                 (handle-receive-connection op msg)
                 (if (nil? msg)
                   (assoc op :command :exit)
                   (handle-receive-data op msg))))
    :cancel (unsubscribe op)))

(defn- handle-send-or-error
  [op [channel]]
  (case channel
    :cancel (unsubscribe op)
    :send (assoc op :command :receive)))

(defn handle-response
  [{:keys [:command] :as op} response]
  (try
    (case command
      :subscribe (handle-with-command op :connect)
      :connect (handle-connect op response)
      :receive (handle-receive op response)
      :send (handle-send-or-error op response)
      :error (handle-send-or-error op response)
      :unsubscribe (handle-with-command op :exit)
      :exit nil)
    (catch Throwable e
      (assoc op :command :error :body e))))

(defn events
  "Subscribe with `sse-subs-chan` and deliver saltstack events in resp-chan.

  Channel will deliver:
  - Exception if error occurs
  - Parsed SSE events

  If you want to stop and close resp-chan, put whatever value to `cancel-chan`

  If error occurs, this events stream is not stopped (unsubscibed), but continue
  listening to events. This causes [[`salt.client/sse`]] to retry connection again,
  if it fails again, another error is delivered and so on.
  This behavior is needed to inform events consumers, there is an error on
  underlining saltstack events stream.

  events recv channel does not use buffer, because it does not block or park and
  passes all messages to resp-chan.

  See [[salt.client/client]] for more details."
  [client-atom cancel-chan resp-chan recv-buffer-size]
  (let [correlation-id (java.util.UUID/randomUUID)
        client @client-atom
        subs-chan (:sse-subs-chan client)
        recv-chan (if (< 1 recv-buffer-size) (a/chan) (a/chan recv-buffer-size))]
    (a/go
      (loop [{:keys [:command :body] :as op}
             (initial-op correlation-id recv-chan)]
        (when command
          (->>
           (case command
             :subscribe (a/>! subs-chan body)
             :connect (a/<! recv-chan)
             :receive (a/alt!
                        cancel-chan [:cancel]
                        recv-chan ([result] [:receive result])
                        :priority true)
             :send (a/alt!
                     cancel-chan [:cancel]
                     [[resp-chan body]] [:send]
                     :priority true)
             :error (a/alt!
                      cancel-chan [:cancel]
                      [[resp-chan body]] [:send]
                      :priority true)
             :unsubscribe (a/alt!
                            [[subs-chan body]] [:unsubscribe]
                            recv-chan ([msg] [:receive msg])
                            :priority true)
             :exit (do
                     ;; unblock all pending writers
                     (a/close! recv-chan)
                     (->> (repeatedly #(a/poll! recv-chan))
                          (take-while identity))
                     (a/close! resp-chan)))
           (handle-response op)
           (recur)))))
    resp-chan))
