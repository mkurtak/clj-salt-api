;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.client.request
  (:require [clojure.core.async :as a]
            [salt.api :as api]
            [salt.core :as s]))

(defn create-request
  [{:keys [:client :request]}]
  (let [resp (merge (select-keys client [::s/master-url ::s/login-response])
                    (::s/default-http-request client)
                    request)]
    resp))

(defn create-login-request
  [{:keys [:client]}]
  (merge (select-keys client [::s/master-url ::s/username ::s/password ::s/eauth])
         (::s/default-http-request client)))

(defn token-valid?
  [{{:keys [:token :expire]} ::s/login-response}]
  (and token (number? expire) (< (/ (System/currentTimeMillis) 1000) expire)))

(defn handle-validate-token
  [{:keys [:client] :as op}]
  (if (token-valid? client)
    (assoc op :command :request :body (create-request op))
    (assoc op :command :login :body (create-login-request op))))

(defn- handle-request
  [op response]
  (if (= :unauthorized (::s/response-category (ex-data response)))
    (assoc op
           :command :login
           :body (create-login-request op))
    (assoc op :command (if (instance? Throwable response) :error :response)
           :body response)))

(defn- handle-login
  [{:keys [:client] :as op} login-response]
  (if (instance? Throwable login-response)
    (assoc op :command :error :body login-response)
    (assoc op
           :command :swap-login
           :client (assoc client ::s/login-response login-response))))

(defn swap-login!
  [client-atom {:keys [:client]}]
  (swap! client-atom assoc ::s/login-response (::s/login-response client)))

(defn handle-swap-login
  [op]
  (assoc op :command :request :body (create-request op)))

(defn initial-op
  [client-atom req]
  {:command :validate :request req :client @client-atom})

(defn handle-response
  [{:keys [command] :as op} response]
  (try (case command
         :validate (handle-validate-token op)
         :login (handle-login op response)
         :swap-login (handle-swap-login op)
         :request (handle-request op response)
         :error nil
         :response nil)
       (catch Throwable e
         (assoc op :command :error :body e))))

(defn request
  "Invoke [[salt.api/request]] request and returns channel which deliver the response.

  This function logs in user if not already logged in and handles unauthorized exception.
  Channel will deliver:
  - Parsed salt-api response body
  - Exception if error occurs

  Channel is closed after response is delivered.

  Request will be merged with client default-http-request. See [[salt.client/client]] for more details."
  ([client-atom req] (request client-atom req (a/chan)))
  ([client-atom req resp-chan]
   (a/go (loop [{:keys [command body] :as op}
                (initial-op client-atom req)]
           (when command
             (->> (case command
                    :validate nil
                    :login (a/<! (api/login body))
                    :swap-login (swap-login! client-atom op)
                    :request (a/<! (api/request body))
                    :response (do (a/>! resp-chan body)
                                  (a/close! resp-chan))
                    :error (do (a/>! resp-chan body)
                               (a/close! resp-chan)))
                  (handle-response op)
                  (recur)))))
   resp-chan))
