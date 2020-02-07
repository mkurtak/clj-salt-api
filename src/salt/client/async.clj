;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.client.async
  (:require [clojure.core.async :as a]
            [clojure.set :as st]
            [clojure.string :as str]
            [salt.client.request :as req]))

(defn initial-op
  [req correlation-id]
  {:command :subscribe
   :request req
   :correlation-id correlation-id
   :body {:type :subscribe :correlation-id correlation-id}})

(defn- handle-connect
  [{expected-correlation-id :correlation-id :as op}
   {:keys [:type :correlation-id :error] :as response}]
  (if (= :connected type)
    (if (or (= expected-correlation-id correlation-id) (= :all correlation-id))
      (if (some? error)
        (assoc op :command :connect-error :body error)
        (assoc op :command :request :body (req/create-request op)))
      (assoc op :command :connect))
    (assoc op :command :error :body response)))

(defn- handle-request
  [op {[{:keys [:jid :minions]}] :return :as response}]
  (if (instance? Throwable response)
    (assoc op :command :error :body response)
    (assoc op
           :jid jid
           :minions (set minions)
           :command :receive)))

(defn- unsubscribe
  [{:keys [:correlation-id] :as op}]
  (assoc op
         :command :unsubscribe
         :body {:type :unsubscribe :correlation-id correlation-id}))

(defn- handle-send
  [{:keys [:minions] :as op}]
  (if (empty? minions)
    (unsubscribe op)
    (assoc op :command :receive :body nil)))

(defn- create-reconnect-request
  [{:keys [:jid]}]
  {:form-params {:client "runner"
                 :fun "jobs.print_job"
                 :jid jid}})

(defn- handle-receive-connection
  [op
   {:keys [:correlation-id] :as response}]
  (if (instance? Throwable response)
    (assoc op :command :error :body response)
    (if (= :all correlation-id)
      (assoc op :command :reconnect :body (create-reconnect-request op))
      (assoc op :command :receive))))

(defn- handle-receive-data
  [{:keys [:jid :minions] :as op}
   {:keys [:tag :data]}]
  (if (and tag data (str/starts-with? tag (str "salt/job/" jid "/ret")))
    (assoc op
           :command :send
           :body {:minion (:id data)
                  :return (:return data)
                  :success (:success data)}
           :minions (disj minions (:id data)))
    (assoc op :command :receive :body nil)))

(defn- parse-print-job-return
  [return jid minions]
  (->> (:Result (get return (keyword (str jid))))
       (map #(vector (name (first %)) (second %)))
       (filter #(contains? minions (first %)))
       (map #(assoc {}
                    :minion (first %)
                    :return (:return (second %))
                    :success (:success (second %))))))

(defn- handle-reconnect
  [{:keys [:jid :minions] :as op}
   {[return] :return :as response}]
  (if (instance? Throwable response)
    (assoc op :command :error :body response)
    (let [returns (parse-print-job-return return jid minions)
          result-minions (set (map :minion returns))
          remaining-minions (st/difference minions result-minions)]
      (if (seq result-minions)
        (assoc op :command :send :body returns :minions remaining-minions)
        (assoc op :command :receive :body nil)))))

(defn- handle-receive
  [op [channel msg]]
  (if (= :connection channel)
    (handle-receive-connection op msg)
    (handle-receive-data op msg)))

(defn- handle-with-command
  [op command]
  (assoc op :command command))

(defn handle-response
  ([op] (handle-response op nil))
  ([{:keys [:command] :as op} response]
   (try
     (case command
       :subscribe (handle-with-command op :connect)
       :connect (handle-connect op response)
       :request (handle-request op response)
       :receive (handle-receive op response)
       :send (handle-send op)
       :reconnect (handle-reconnect op response)
       :connect-error (handle-with-command op :exit)
       :error (unsubscribe op)
       :unsubscribe (handle-with-command op :exit)
       :exit nil)
     (catch Throwable e
       (assoc op :command :error :body e)))))

(defn- to-vec
  [x]
  (if (sequential? x) x (vector x)))

(defn request-async
  "Subscribe to `sse-pub`, invoke async client request and deliver responses in resp-chan.

  This function uses [[salt.client.request/request]] to call async client.
  Channel will deliver:
  - Exception if error occurs
  - Parsed SSE events

  This function implements best practices for working with salt-api as defined in
  https://docs.saltstack.com/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html#best-practices
  If SSE reconnect occurs during the call, jobs.print_job is used to retrieve 
  the state of job. 

  Channel is closed after all minions return.
  Request will be merged with client default-http-request. 
  See [[salt.client/client]] for more details."
  [client-atom req subs-chan sse-pub resp-chan]
  (let [correlation-id (java.util.UUID/randomUUID)
        conn-chan (a/chan)
        data-chan (a/chan 100)]
    (a/go
      (loop [{:keys [:command :body] :as op}
             (initial-op req correlation-id)]
        (when command
          (->> (case command
                 :subscribe (do (a/sub sse-pub :connection conn-chan)
                                (a/sub sse-pub :data data-chan)
                                (a/>! subs-chan body)
                                nil)
                 :connect (a/<! conn-chan)
                 :request (a/<! (req/request client-atom body))
                 :receive (let [[result ch] (a/alts! [conn-chan data-chan]
                                                     :priority true)]
                            (if (= ch conn-chan)
                              [:connection result]
                              [:data result]))
                 :send (doseq [b (to-vec body)]
                         (a/>! resp-chan b))
                 :reconnect (a/<! (req/request client-atom body))
                 :connect-error (a/>! resp-chan body)
                 :error (a/>! resp-chan body)
                 :unsubscribe (a/>! subs-chan body)
                 :exit (do
                         (a/unsub sse-pub :data data-chan)
                         (a/unsub sse-pub :connection conn-chan)
                         (a/close! conn-chan)
                         (a/close! data-chan)
                         (a/close! resp-chan)))
               (handle-response op)
               (recur)))))
    resp-chan))
