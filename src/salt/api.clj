;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.api
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [salt.core :as s]
   [salt.http :as http]
   [salt.retry :as retry]))

(def ending-slash #"/$")
(def starting-slash #"^/")

(defn- throw-err
  "Simple wrapper that throws exception if e is Throwable, otherwise just return e."
  [e]
  (if (instance? Throwable e) (throw e) e))

(defn- add-url-path
  [url path]
  (if (str/blank? path)
    url
    (str/join "/" [(str/replace url ending-slash "")
                   (str/replace path starting-slash "")])))

(defn create-login-request
  [{:keys [::s/master-url ::s/username ::s/password ::s/eauth]
    :or {eauth "pam"}
    :as opts}]
  (merge opts
         {:url (add-url-path master-url "/login")
          :method :post
          :headers {"Accept" "application/json"}
          :form-params {:eauth eauth
                        :username username
                        :password password}}))

(defn create-request
  [{:keys [::s/master-url :url :method :headers :content-type]
    {:keys [:token]} ::s/login-response
    :or {method :post content-type :json}
    :as opts}]  
  (merge opts
         {:url (add-url-path master-url url)
          :method method
          :content-type content-type
          :headers (merge headers {"Accept" "application/json"
                                   "X-Auth-token" token})}))

(defn create-sse-request
  [{:keys [::s/master-url :headers]
    {:keys [:token]} ::s/login-response
    :as opts}]
  (merge opts
         {:url (add-url-path master-url "/events")
          :method :get
          :headers (merge headers {"Accept" "application/json"
                                   "X-Auth-token" token})}))

(defn- parse-return-vector
  [{[result] :return :as body}]
  (if result
    result
    (throw (ex-info "Could not parse token" {::s/response-body body}))))

(defn http-request
  [req resp-chan]
  (retry/with-retry #(http/request req) resp-chan))

(defn login
  "Executes login [[salt.http/request]], handles exception, parses body and return new channel with response.

  Prepends master-url and appends '/login' to ring request url.    
  Request is retried with default backoff and retriable?. See [[salt.retry/with-retry]] for more details.
  Exception and parsed body is sent with channel and channel is closed afterwards.
  Request is aleph ring request with additonal keys:
  
  | Key                      | Description |
  | -------------------------| ------------|
  | `::salt.core/master-url` | Base url of salt-master. Its prepended to ring :url
  | `::salt.core/username`   | Username to be used in salt authetincation if any
  | `::salt.core/password`   | Password to be used in salt authetincation if any
  | `::salt.core/eauth`      | Eauth system to be used in salt authentication. Please refer saltstack documentation for available values"
  [req]
  (http-request
   (create-login-request req)
   (a/chan 1 (comp
              (map throw-err)
              (map http/parse-body)
              (map parse-return-vector))
           identity)))

(defn request
  "Executes [[salt.http/request]], handles exception, parses body and return new channel with response.

  Prepends master-url to ring request url.    
  Request is retried with default backoff and retriable?. See [[salt.retry/with-retry]] for more details.
  Exception and parsed body is sent with channel and channel is closed afterwards.
  Request is aleph ring request with additonal keys:
  
  | Key                           | Description |
  | ------------------------------| ------------|
  | `::salt.core/master-url`      | Base url of salt-master. Its prepended to ring :url
  | `::salt.core/login-response`  | Response from [[salt.api/login]]|"
  [req]
  (http-request
   (create-request req)
   (a/chan 1 (comp
              (map throw-err)
              (map http/parse-body))
           identity)))

(defn logout
  "Executes logout request with [[request]] function."
  [req]
  (request (assoc req :url "/logout")))

(defn sse
  "Executes [[salt.http/request]], handles exception, parses individual SSEs  and return new channel emiting SSEs.

  Prepends master-url and appends '/events' to ring request url.
  Request is retried with default backoff and retriable?. See [[salt.retry/with-retry]] for more details.
  If exception occurs, it is written to the channel and channel is closed.
  If SSE connection is closed, channel is closed.
  Request is aleph ring request with additonal keys
  
  | Key                           | Description |
  | ------------------------------| ------------|
  | `::salt.core/master-url`      | Base url of salt-master. It is preepended :url
  | `::salt.core/login-response`  | Response from [[salt.api/login]]"
  ([req] (sse req {}))
  ([req pool-opts]
   (retry/with-retry
     #(http/sse (create-sse-request req) pool-opts)
     (a/chan 1 (comp                    ;todo channel size?
                (map throw-err)
                (map http/parse-sse))
             identity))))

(defn close-sse
  [connection]
  (http/close-sse connection))
