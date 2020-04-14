;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.client.sse
  (:require [clojure.core.async :as a]
            [salt.api :as api]
            [salt.core :as s]
            [salt.client.request :as req]))

(defn- closeable?
  [{:keys [:subscription-count]
    {:keys [::s/sse-keep-alive?]} :client}]
  (and (false? sse-keep-alive?)
       (= 0 subscription-count)))

(defn- initial-command
  [op]
  (if (not (closeable? op)) :validate :park))

(defn initial-op
  ([client-atom resp-chan] (initial-op client-atom resp-chan {}))
  ([client-atom resp-chan req]
   (let [op {:request req
             :retry-timeout 500
             :subscription-count 0
             :sse-retries 0
             :sse-mult (a/mult resp-chan)
             :client @client-atom}]
     (assoc op :command (initial-command op)))))

(defn- error-body
  [{:keys [:sse-retries]
    {:keys [::s/max-sse-retries]} :client} body]
  (when (>= sse-retries max-sse-retries) body))

(defn- handle-login
  [{:keys [:client] :as op} login-response]
  (if (instance? Throwable login-response)
    (assoc op :command :error :body (error-body op login-response))
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
    (if (and
         (= :unauthorized (::s/response-category (ex-data response)))
         (not (closeable? op)))
      (assoc op
             :command :login
             :body (req/create-login-request op)
             :sse-retries (inc sse-retries)) 
      (assoc op :command :error :body (error-body op response)))
    (if (= :connect type)
      (assoc op
             :command :send
             :connection (assoc response :chan channel)
             :sse-retries 0
             :retry-timeout (reset-retry-timeout retry-timeout sse-retries)
             :body (connected-message :all))
      (assoc op
             :command :error
             :body (error-body op (ex-info (str "Invalid type " type " received")
                                           {:response response}))))))

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
   {:keys [:type] :as event}]
  (if (instance? Throwable event)
    (assoc op :command :send :body event)
    (if (nil? type)
      (assoc op :command :close)
      (if (= 0 subscription-count)
        (assoc op :command :receive)
        (case type
          :data (assoc op :command :send :body (:data event))
          :retry (assoc op :command :receive :retry-timeout (:retry event))
          :close (assoc op :command :receive))))))

(defn- handle-receive
  [op [channel msg]]
  (if (= :subscription channel)
    (handle-receive-subscription op msg)
    (handle-receive-event op msg)))

(defn- handle-send
  [{:keys [:connection] :as op}]
  (assoc op
         :command (if (some? connection)
                    (if (closeable? op) :close :receive)
                    :park)
         :body nil))

(defn- handle-close
  [op]
  (assoc op
         :command (initial-command op)
         :body nil
         :connection nil))

(defn- handle-park
  [{:keys [:subscription-count :retry-timeout :sse-retries] :as op}
   [_ {:keys [:type]}]]
  (case type 
    :subscribe (assoc op
                      :subscription-count (inc subscription-count)
                      :sse-retries 0
                      :retry-timeout (reset-retry-timeout retry-timeout sse-retries)
                      :command :validate)
    :unsubscribe (assoc op
                        :subscription-count (dec subscription-count)
                        :command :park)
    :exit (assoc op :command :exit)))

(defn- handle-error-subscription
  [{:keys [:subscription-count] :as op}
   {:keys [:type]}]
  (case type
    :subscribe (assoc op
                      :subscription-count (inc subscription-count)
                      :command :error
                      :body nil)
    :unsubscribe (assoc op
                        :subscription-count (dec subscription-count)
                        :command :error
                        :body nil)
    :exit (assoc op :command :exit)))

(defn- handle-error
  [{:keys [sse-retries retry-timeout] :as op} [channel msg]]
  (if (= :subscription channel)
    (handle-error-subscription op msg)
    (let [retries (inc sse-retries)]
      (assoc op
             :command :close
             :sse-retries retries
             :retry-timeout (* retries retry-timeout)))))

(defn handle-response
  [{:keys [command] :as op} response]
  (try
    (case command
      :validate (req/handle-validate-token op)
      :login (handle-login op response)
      :swap-login (req/handle-swap-login op)
      :request (handle-request op response)
      :receive (handle-receive op response)
      :send (handle-send op)
      :close (handle-close op)
      :park (handle-park op response)
      :error (handle-error op response)
      :exit nil)
    (catch Throwable e
      (assoc op :command :error :body (error-body op e)))))

(defn- subscription!
  [{:keys [:sse-mult]} {:keys [:type :recv-chan] :as msg}]
  (case type
    :subscribe (do
                 (a/tap sse-mult recv-chan)
                 msg)
    :unsubscribe (do
                   (a/untap sse-mult recv-chan)
                   msg)
    msg))

(defn- unsubscribe-all!
  [{:keys [:sse-mult]}]
  (a/untap-all sse-mult))

(defn sse
  "Invoke [[salt.api/sse]] request and returns `resp-chan` which deliver SSE events, subscription responses and error.
  
  This function
  * logs in user if not already logged in and handles unauthorized exception
  * creates infinite go-loop listens to `subs-chan` and write SSE to `resp-chan`

  To receive SSE events client should:
  - create a [[core.async/pub]] on `resp-chan` and receive responses
  - put {:type :subscribe :correlation-id} to subs-chan
  - take for connection response from `resp-chan` {:type :connect :correlation-id} 
  - execute [[salt.client.request/request]] with async client
  - take SSE events

  Details:
  
  Takes values from `subs-chan`
  | Key                | Description |
  | -------------------| ------------|
  | :type :subscribe   | Subscribe with correlation id
  | :type :unsubscribe | Unsubscribe
  | :type :exit        | Quit go-loop
  
  Channel will deliver:
  | Response         | Description |
  | -----------------| ------------|
  | Exception        | Error occurs and SSE could not be delivered (e.g. connection error and reconnect fails). Client should return error.
  | `:type :data`    | SSE event. Body is parsed from json.
  | `:type :connect` | When subscription is made (with :correlation-id set) or on reconnect with :correlation-id :all or subscription error occurs (:error is set)

  Behavior of this go-loop is specified with [[salt.client/client]]
  - ::salt.core/sse-keep-alive? - create sse connection even if there are no subscribers
  - ::salt.core/sse-max-retries - number errors could occur before go-loop is parked
  - ::salt.core/default-http-request - default request. will be merged with `req`"
  ([client-atom subs-chan] (sse client-atom subs-chan (a/chan) {}))
  ([client-atom subs-chan resp-chan] (sse client-atom subs-chan resp-chan {}))
  ([client-atom subs-chan resp-chan req]
   (a/go (loop [{:keys [command body retry-timeout]
                 {sse-chan :chan :as connection} :connection
                 {pool-opts ::s/default-sse-pool-opts} :client
                 :as op}
                (initial-op client-atom resp-chan req)]
           (when command
             (->> (case command
                    :validate nil
                    :login (a/<! (api/login body))
                    :swap-login (req/swap-login! client-atom op)
                    :request (let [ch (api/sse body pool-opts)]
                               [:sse (a/<! ch) ch])
                    :receive (a/alt!
                               subs-chan ([msg] [:subscription (subscription!
                                                                @client-atom msg)])
                               sse-chan ([msg] [:sse msg])
                               :priority true)
                    :send (a/>! resp-chan body)
                    :close (api/close-sse connection)
                    :park [:subscription (subscription!
                                          @client-atom (a/<! subs-chan))]
                    :error (do
                             (when body (a/>! resp-chan body))
                             (let [timeout-ch (a/timeout retry-timeout)]
                               (a/alt!
                                 subs-chan ([msg]
                                            [:subscription (subscription! op msg)])
                                 timeout-ch [:timeout])))
                    :exit (do          
                            ;; close http connection
                            (api/close-sse connection)
                            ;; do not accept any new subscriptions
                            (a/close! subs-chan)
                            ;; unblock all pending subscribers
                            (->> (repeatedly #(a/poll! subs-chan))
                                 (map subscription!)
                                 (take-while identity))
                            ;; send info to current subscribers
                            ;; if they try to unsubscribe put will return false
                            (a/>! resp-chan (connected-message :none))
                            ;; unsub all
                            (unsubscribe-all! op)
                            ;; close response channel
                            (a/close! resp-chan)))
                  (handle-response op)
                  (recur)))))
   resp-chan))
