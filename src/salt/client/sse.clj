;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns ^:no-doc salt.client.sse
  (:require [clojure.core.async :as a]
            [salt.api :as api]
            [salt.core :as s]
            [salt.client.request :as req]))

(defn- to-vec
  [x]
  (if (sequential? x) x (vector x)))

(defn- closeable?
  [{:keys [:subscription-count]
    {:keys [::s/sse-keep-alive?]} :client}]
  (and (false? sse-keep-alive?)
       (= 0 subscription-count)))

(defn- max-retries-reached
  [{:keys [:sse-retries] {:keys [::s/max-sse-retries]} :client}]
  (>= sse-retries max-sse-retries))

(defn- initial-command
  [op]
  (if (not (closeable? op)) :validate :park))

(defn initial-op
  [client-atom resp-chan]
  (let [op {:request (::s/default-sse-request @client-atom)
            :retry-timeout 500
            :subscription-count 0
            :sse-retries 0
            :sse-mult (a/mult resp-chan)
            :client @client-atom}]
    (assoc op :command (initial-command op))))

(defn- error-op
  "Send error only if maximum retries exceeded."
  [op body]
  (if (max-retries-reached op)
    (assoc op :command :error-send :body body)
    (assoc op :command :error-timeout :body nil)))

(defn- handle-login
  [{:keys [:client] :as op} login-response]
  (if (instance? Throwable login-response)
    (error-op op login-response)
    (assoc op
           :command :swap-login
           :client (assoc client ::s/login-response login-response))))

(defn- reset-retry-timeout
  [retry-timeout retries]
  (/ retry-timeout (inc retries)))

(defn- connected-message
  [correlation-id]
  {:type :connected
   :correlation-id correlation-id})

(defn- handle-request
  [{:keys [:sse-retries :retry-timeout] :as op}
   [_ {:keys [:type] :as response} channel]]
  (if (instance? Throwable response)
    (if (and (= :unauthorized (::s/response-category (ex-data response)))
             (not (max-retries-reached op))) ; prevent endless loop between request and login
      (assoc op
             :command :login            ; login if unauthorized response received
             :body (req/create-login-request op)
             :sse-retries (inc sse-retries)) ; inc sse-retries to prevent endless loop
      (error-op op response))
    (if (= :connect type)
      (assoc op
             :command :send             ; send connected-message with `:all`
             :connection (assoc response :chan channel)
             :sse-retries 0             ; reset retries and timeout after successful request
             :retry-timeout (reset-retry-timeout retry-timeout sse-retries)
             :body (connected-message :all))
      (error-op op (ex-info (str "Invalid type " type " received")
                            {:response response})))))

(defn- handle-receive-subscription
  [{:keys [:subscription-count] :as op}
   {:keys [:type :correlation-id]}]
  (case type
    :subscribe (assoc op
                      :subscription-count (inc subscription-count)
                      :command :send
                      :body (connected-message correlation-id))
    :unsubscribe (as-> op next
                   (assoc next :subscription-count (dec subscription-count))
                   (assoc next :command (if (closeable? next) :close :receive)))
    :exit (assoc op :command :exit)))

(defn- handle-receive-event
  [{:keys [:subscription-count] :as op}
   {:keys [:type :data :retry] :as event}]
  (if (instance? Throwable event)       ; SSE sent an error
    (assoc op :command :send :body event)
    (if (nil? type)                     ; SSE has been closed
      (assoc op :command :close)
      (if (= 0 subscription-count)
        (assoc op :command :receive)    ; ignore event if there is no subscriber
        (case type
          :data (assoc op :command :send :body data)
          :retry (assoc op :command :receive :retry-timeout retry)
          :close (assoc op :command :receive))))))

(defn- handle-receive
  [op [channel msg]]
  (if (= :subscription channel)
    (handle-receive-subscription op msg)
    (handle-receive-event op msg)))

(defn- handle-send-subscription
  "Subscription received when sending response.

  This could happen in deadlock prevention mechanism.
  Dispatch subscription by type
  * `:subscribe` increase subscriber and conj reply to subscriber to current body to send
  * `:unsubscribe` decrease subscriber count and if
     ** operation is [[closeable?]] `:close`
     ** no subscribers `:receive` (not closeable but no subscribers => keep-alive)
     ** otherwise continue with sending
  * `:exit` exit"
  [{:keys [:subscription-count :body] :as op}
   {:keys [:type :correlation-id]}]
  (case type
    :subscribe (assoc op
                      :subscription-count (inc subscription-count)
                      :command :send
                      :body (conj (to-vec body) (connected-message correlation-id)))
    :unsubscribe (as-> op next
                   (assoc next :subscription-count (dec subscription-count))
                   (assoc next :command (cond
                                          (closeable? next) :close
                                          (= 0 (:subscription-count next)) :receive
                                          :else :send)))
    :exit (assoc op :command :exit)))

(defn- handle-send
  "Response sent or subscription received
  If response is sent
  * `:receive` command next SSE event, if there is no other msg to sent
  * `:send` msg if there is something in msg this is the case of received
     subscription during send.
  If subscription is received handle it with [[handle-send-subscription]]"
  [op [channel msg]]
  (case channel
    :subscription (handle-send-subscription op msg)
    :send (if (seq msg)
            (assoc op :command :send :body msg)
            (assoc op :command :receive :body nil))))

(defn- handle-close
  "Execute initial command after close, remove body and reset connection"
  [op]
  (assoc op
         :command (initial-command op)
         :body nil
         :connection nil))

(defn- handle-park
  "Subscription received.

  Dispatch subscription by type:
  * `:subscribe` wake up and start again with validate command
  * `:unsubscribe` stays in park
  * `:exit` exit"
  [{:keys [:subscription-count :retry-timeout :sse-retries] :as op}
   [_ {:keys [:type]}]]
  (case type
    :subscribe (assoc op
                      :subscription-count (inc subscription-count)
                      :sse-retries 0
                      :retry-timeout (reset-retry-timeout retry-timeout sse-retries)
                      :command :validate)
    :unsubscribe (assoc op
                        :subscription-count (max 0 (dec subscription-count))
                        :command :park)
    :exit (assoc op :command :exit)))

(defn- handle-error-subscription
  "Error has not been sent and subscription has been received instead.

  Dispatch subscription by type:
  * `:subscribe` increase and error-send again, there is no need to reply
                 with correlation-id because sse is in error state.
                 So either it will try to reconnect (it has subscribers)
                 or it will send an error
  * `:unsubscribe` just decrease subscribers and error-send again
                   same reasons for staying in error state as for :subscribe
  * `:exit` exit"
  [{:keys [:subscription-count] :as op}
   {:keys [:type]}]
  (case type
    :subscribe (assoc op
                      :subscription-count (inc subscription-count)
                      :command :error-send)
    :unsubscribe (as-> op next
                   (assoc next :subscription-count (dec subscription-count))
                   (assoc next :command (if (= 0 (:subscription-count next))
                                          :error-timeout
                                          :error-send)))
    :exit (assoc op :command :exit)))

(defn- handle-error-send
  [op [channel msg]]
  (case channel
    :subscription (handle-error-subscription op msg)
    :send (assoc op
                 :command :error-timeout
                 :body nil)))

(defn- handle-error-timeout
  "Close after error timeout"
  [{:keys [sse-retries retry-timeout] :as op}]
  (let [retries (inc sse-retries)]
    (assoc op
           :command :close
           :sse-retries retries
           :retry-timeout (min 5000 (* retries retry-timeout)))))

(defn handle-response
  "Handles response of command.
  Called after command has been executed and response has been received.
  This function just dispatches handler based on operation command
  and returns new operation with new command"
  [{:keys [command] :as op} response]
  (try
    (case command
      :validate (req/handle-validate-token op)
      :login (handle-login op response)
      :swap-login (req/handle-swap-login op)
      :request (handle-request op response)
      :receive (handle-receive op response)
      :send (handle-send op response)
      :close (handle-close op)
      :park (handle-park op response)
      :error-send (handle-error-send op response)
      :error-timeout (handle-error-timeout op)
      :exit nil)
    (catch Throwable e
      (error-op op e))))

(defn- subscription!
  "Tap/Untap sse-mult on recv-chan as side-effect and return msg."
  [{:keys [:sse-mult]} {:keys [:type :recv-chan] :as msg}]
  (case type
    :subscribe (a/tap sse-mult recv-chan)
    :unsubscribe (a/untap sse-mult recv-chan)
    msg)
  msg)

(defn- unsubscribe-all!
  [{:keys [:sse-mult]}]
  (a/untap-all sse-mult))

(defn- subscription-resp
  [op msg]
  [:subscription (subscription! op msg)])

(defn graceful-shutdown
  "Graceful shutdown:
  1. Close HTTP connection to SSE `/events` endpoint
  2. Close subs-chan no new subscriptions can be accepted
  3. unblock pending subscribers - subscribers that have already sent
     a subscription message and are blocking on subs-chan
  4. send `:connected` `:none` message to subscribers but do not wait for response.
     Subscribers should gracefully shutdown
  5. Unsubscribe core.async channels from sse-mult.
  6. Close response channel,"
  [op connection subs-chan resp-chan]
  (api/close-sse connection)
  (a/close! subs-chan)
  (->> (repeatedly #(a/poll! subs-chan))
       (map #(subscription! op %))
       (take-while identity))
  (unsubscribe-all! op)
  (a/close! resp-chan))

(defn sse
  "Invoke [[salt.api/sse]] request and returns `resp-chan` which deliver SSE events, subscription responses and error.

  This function creates go-loop that connects to SSE events, listens to
  `subs-chan` and write SSE to `resp-chan`. It handles errors and authentication.

   Messages taken from `subs-chan`:
  | Key                | Description |
  | -------------------| ------------|
  | :type :subscribe   | Add subscriber with correlation id
  | :type :unsubscribe | Remove subscriber with correlation id
  | :type :exit        | Quit go-loop

  Responses delivered to `resp-chan`:
  | Response         | Description |
  | -----------------| ------------|
  | Exception        | Error occurs and SSE could not be delivered (e.g. connection error and reconnect fails). Client should return error.
  | `:type :data`    | SSE event. Body is parsed from JSON.
  | `:type :connect` | When subscription is made (with :correlation-id set) or on reconnect with :correlation-id set to :all

  See [[salt.client/client]] for configuration options."
  [client-atom subs-chan resp-chan]
  (a/go (loop [{:keys [command body retry-timeout]
                {sse-chan :chan :as connection} :connection
                {pool-opts ::s/default-sse-pool-opts
                 sse-buffer-size ::s/sse-buffer-size} :client
                :as op}
               (initial-op client-atom resp-chan)]
          (when command
            (->> (case command
                   :validate nil
                   :login (a/<! (api/login body))
                   :swap-login (req/swap-login! client-atom op)
                   :request (let [ch (api/sse body pool-opts sse-buffer-size)]
                              [:sse (a/<! ch) ch])
                   :receive (a/alt!
                              subs-chan ([msg] (subscription-resp op msg))
                              sse-chan ([msg] [:sse msg])
                              :priority true)
                   :send (let [b (to-vec body)]
                           (a/alt!
                             subs-chan ([msg] (subscription-resp op msg))
                             [[resp-chan (first b)]] [:send (next b)]
                             :priority true))
                   :close (api/close-sse connection)
                   :park [:subscription (subscription! op (a/<! subs-chan))]
                   :error-send (a/alt!
                                 subs-chan ([msg] (subscription-resp op msg))
                                 [[resp-chan body]] [:send body]
                                 :priority true)
                   :error-timeout (a/<! (a/timeout retry-timeout))
                   :exit (graceful-shutdown op connection subs-chan resp-chan))
                 (handle-response op)
                 (recur)))))
  resp-chan)
