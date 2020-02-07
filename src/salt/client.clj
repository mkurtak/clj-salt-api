;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.client
  (:require [clojure.core.async :as a]
            [salt.api :as api]
            [salt.client.async :as async]
            [salt.client.request :as req]
            [salt.client.sse :as sse]
            [salt.core :as s]))

(defn- sse-topic
  [msg]
  (if (or (instance? Throwable msg) (= :connected (:type msg))) :connection :data))

(defn client-not-started
  "Create client atom with default values. Do not start sse."
  [opts]
  (atom (merge {::s/eauth "pam"
                ::s/sse-keep-alive? true
                ::s/max-sse-retries 3} opts)))

(defn- client-start
  [client-atom]
  (let [subs-chan (a/chan)
        resp-chan (a/chan)
        pub (a/pub resp-chan sse-topic)]
    (sse/sse client-atom subs-chan resp-chan)
    (swap! client-atom assoc
           :sse-subs-chan subs-chan
           :sse-resp-chan resp-chan
           :sse-pub pub)
    client-atom))

(defn client
  "Given a config map, create a client for saltstack api. Supported keys:
  
  `::salt.core/master-url`           - required, saltstack master base url
  `::salt.core/username`             - optional, username to be used in salt auth
                                       system
  `::salt.core/password`             - optional, password to be used in salt auth
                                       system
  `::salt.core/eauth`                - optional, Eauth system to be used in salt
                                       auth system. defaults to 'pam'.
                                       Please refer saltstack documentation for
                                       all available values
  `::salt.core/default-http-request` - optional, default ring request to be
                                       merged with all requests
  `::salt.core/sse-keep-alive?`      - optional, if true /events SSE connection
                                       will be allways kept open, 
                                       if false /events SSE connection will be
                                       kept open only if there are active async
                                       requests, defaults to true
  `::salt.core/max-sse-retries`      - optional, maximum number of errors before
                                       /events SSE connection is retried. 
                                       If number of errors exceeds this value,
                                       all async request receive an error and 
                                       SSE behaves as sse-keep-alive? false, 
                                       defaults to 3"
  [opts]
  (client-start (client-not-started opts)))

(defn request
  "Executes salt request. Puts one response or error to `resp-chan`.

  `client-atom` client created with [[salt.client/client]]
  `req` ring request map (see [[aleph.http/request]] documentation)
  `resp-chan` core.async channel to deliver response. defaults to chan

  `resp-chan` will deliver:
  - Parsed salt-api response body
  - Exception if error occurs (with response in meta)

  Channel is closed after response is delivered."
  ([client-atom req] (request client-atom req (a/chan)))
  ([client-atom req resp-chan]
   (req/request client-atom req resp-chan)))

(defn request-async
  "Executes salt request on async client. Puts minion responses or error to `resp-chan`.

  `client-atom` client created with [[salt.client/client]]
  `req` ring request map (see [[aleph.http/request]] documentation)
  `resp-chan` core.async channel to deliver response. defaults to chan

  `resp-chan` will deliver:
  - For each minion a map consisting of keys `[:minion :return :success]`
  - Parsed salt-api response body
  - Exception if error occurs (with response in meta)

  This function implements best practices for working with salt-api as defined in
  [https://docs.saltstack.com/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html#best-practices
  If SSE reconnect occurs during the call, jobs.print_job is used to retrieve 
  the state of job. 

  Channel is closed after response is delivered."
  ([client-atom req] (request-async client-atom req (a/chan)))
  ([client-atom req resp-chan]
   (let [{:keys [:sse-subs-chan :sse-pub]} @client-atom]
     (async/request-async client-atom req sse-subs-chan sse-pub resp-chan))))

(defn revoke-session
  "Logs out user from saltstack."
  [client-atom]
  (let [client @client-atom]
    (swap! client-atom dissoc ::s/login-response)
    (when (req/token-valid? client)
      (api/logout client))))

(defn close
  "Closes the client. It cannot be used after close."
  [client-atom]
  (let [client @client-atom]
    (a/>!! (:sse-subs-chan client) {:type :exit})
    (a/close! (:sse-resp-chan client))
    (a/close! (:sse-subs-chan client))
    (revoke-session client-atom)))

(comment
  (def salt-client (salt.client/client {::s/master-url "http://192.168.50.10:8000"
                                        ::s/username "saltapi"
                                        ::s/password "saltapi"
                                        ::s/max-sse-retries 3
                                        ::s/sse-keep-alive? true}))

  (clojure.core.async/<!! (request salt-client
                                   {:form-params {:client "local"
                                                  :tgt "*"
                                                  :fun "test.ping"}}))

  (do
    (def resp-chan (request-async salt-client
                                  {:form-params {:client "local_async"
                                                 :tgt "*"
                                                 :fun "test.ping"}}))

    (def resp-chan2 (request-async salt-client
                                   {:form-params {:client "local_async"
                                                  :tgt "*"
                                                  :fun "test.version"}})))
  (clojure.core.async/<!! resp-chan)
  (clojure.core.async/<!! resp-chan2)

  (close salt-client)
  )
