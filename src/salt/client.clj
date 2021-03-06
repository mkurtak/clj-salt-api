;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.client
  (:require [clojure.core.async :as a]
            [salt.api :as api]
            [salt.client.async :as async]
            [salt.client.events :as events]
            [salt.client.request :as req]
            [salt.client.sse :as sse]
            [salt.core :as s]))

(defn ^:no-doc client-not-started
  "Create client atom with default values. Do not start sse."
  [opts]
  (atom (merge {::s/eauth "pam"
                ::s/sse-keep-alive? true
                ::s/max-sse-retries 3
                ::s/sse-buffer-size 100}
               opts)))

(defn- ^:no-doc client-start
  "Start `sse/sse` and create channels subs and resp channels for communication.

  Creates `subs-chan` without buffer. Buffer is not needed because subscriber
  has to wait for subscription response anyway.

  Creates `sse-resp-chan` without buffer. Buffer is not needed because mult
  is created on this channel, so buffers should be set on taps.

  Assoc `:sse-subs-chan` and `:sse-resp-chan` in `client-atom`"
  [client-atom]
  (let [subs-chan (a/chan)
        resp-chan (a/chan)]
    (sse/sse client-atom subs-chan resp-chan)
    (swap! client-atom assoc
           :sse-subs-chan subs-chan
           :sse-resp-chan resp-chan)
    client-atom))

(defn client
  "Given a config map, create a client for saltstack api. Supported keys:

  - `::salt.core/master-url`           - required, saltstack master base url
  - `::salt.core/username`             - optional, username to be used in salt auth
                                         system
  - `::salt.core/password`             - optional, password to be used in salt auth
                                         system
  - `::salt.core/eauth`                - optional, Eauth system to be used in salt
                                         auth system. defaults to 'pam'.
                                         Please refer saltstack documentation for
                                         all available values
  - `::salt.core/default-http-request` - optional, default ring request to be
                                         merged with all requests
  - `::salt.core/default-sse-pool-opts`- optional, default connection pool opts,
                                         see `aleph` documentation for more details.
  - `::salt.core/default-sse-request`  - optional, default ring request to for sse
                                         it is merged with `::salt.core/default-http-requet`
  - `::salt.core/sse-keep-alive?`      - optional, if true /events SSE connection
                                         will be always kept open,
                                         if false /events SSE connection will be
                                         kept open only if there are active async
                                         requests, defaults to true
  - `::salt.core/max-sse-retries`      - optional, maximum number of errors before
                                         /events SSE connection is retried.
                                         If number of errors exceeds this value,
                                         all async request receive an error and
                                         SSE behaves as sse-keep-alive? false,
                                         defaults to 3
  - `::salt.core/sse-buffer-size`      - optional, sse-chan core.async buffer size,
                                         defaults to 100"

  [opts]
  (client-start (client-not-started opts)))

(defn request
  "Executes salt request. Puts one response or error to `resp-chan`.

  - `client-atom` client created with [[salt.client/client]]
  - `req` ring request map (see [[aleph.http/request]] documentation).
         This is plain salt-api HTTP request data
         merged with client  `::salt.core/default-http-request`
  - `resp-chan` core.async channel to deliver response. defaults to chan

  `resp-chan` will deliver:
  - Parsed salt-api response body
  - Exception if error occurs (with response in meta)

  Channel is closed after response is delivered."
  ([client-atom req] (request client-atom req (a/chan 100)))
  ([client-atom req resp-chan]
   (req/request client-atom req resp-chan)))

(defn request-async
  "Executes salt request on async client. Puts master/minion responses or error to `resp-chan`.

  - `client-atom` client created with [[salt.client/client]]
  - `req` ring request map (see [[aleph.http/request]] documentation).
        This is plain salt-api HTTP request data
        merged with client  `::salt.core/default-http-request`.
        Async request reuses salt client `:timeout` form parameter in the same
        manner as https://docs.saltstack.com/en/latest/ref/clients/index.html#salt.client.LocalClient
  - `resp-chan` core.async channel to deliver response. defaults to chan
  - `recv-buffer-size` buffer size for core.async recv channel from [[salt.client.sse]]

  `resp-chan` core.async channel to deliver response. defaults to chan

  `resp-chan` will deliver:
  - For each minion a map consisting of keys `[:minion :return :success]`
  - For master response (wheel,runner) a map consisting off keys `[:return :success]`
  - Exception if error occurs (with response in meta)

  This function implements best practices for working with salt-api as defined in
  [https://docs.saltstack.com/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html#best-practices
  If SSE reconnect occurs during the call, jobs.print_job is used to retrieve
  the state of job.

  Channel is closed after response is delivered."
  ([client-atom req] (request-async client-atom req (a/chan 100) 100))
  ([client-atom req resp-chan recv-buffer-size]
   (async/request-async client-atom req resp-chan recv-buffer-size)))

(defn events
  "Listen to saltstack data events. Puts all events and sse errors to `resp-chan`.

  - `client-atom` client created with [[salt.client/client]]
  - `cancel-chan` core.async channel to receive cancel message. defaults to chan
  - `resp-chan` core.async channel to deliver response. defaults to chan
  - `recv-buffer-size` buffer size for core.async recv channel from [[salt.client.sse]]

  `resp-chan` will deliver:
  - data events (retry events will not be delivered)
  - exceptions if saltstack sse encounters an error

  To cancel and close `resp-chan`, send some message to `cancel-chan`."
  ([client-atom] (events client-atom (a/chan) (a/chan 100) 100))
  ([client-atom cancel-chan] (events client-atom cancel-chan (a/chan 100) 100))
  ([client-atom cancel-chan resp-chan recv-buffer-size]
   (events/events client-atom cancel-chan resp-chan recv-buffer-size)))

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
