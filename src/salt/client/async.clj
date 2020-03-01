;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.client.async
  (:require [clojure.core.async :as a]
            [clojure.set :as st]
            [clojure.string :as str]
            [salt.client.request :as req]))

(def master-clients
  #{"runner_async" "wheel_async"})

(defn initial-op
  "Create initial operation. Minion timeout is same computed from saltstack client timeout setting. See https://docs.saltstack.com/en/latest/ref/clients/index.html#salt.client.LocalClient."
  [req correlation-id]
  {:command :subscribe
   :request req
   :correlation-id correlation-id
   :master-client? (contains? master-clients (get-in req [:form-params :client]))
   :minion-timeout (* 1000 (get-in req [:form-params :timeout] 5))
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

(defn- parse-request
  [{:keys [:master-client?] :as op} {:keys [:jid :minions :tag]}]
  (if master-client?
    (assoc op :jid jid :job-tag (str tag "/ret") :command :receive)
    (assoc op :jid jid :job-tag (str "salt/job/" jid "/ret")
           :minions (set minions)
           :command :receive
           :last-receive-time (System/currentTimeMillis))))

(defn- handle-request
  [op {[{:keys [:jid] :as return}] :return :as response}]
  (if (instance? Throwable response)
    (assoc op :command :error :body response)
    (if jid
      (parse-request op return)
      (assoc op :command :error :body
             (ex-info "No job returned from salt" {:response response})))))

(defn- unsubscribe
  [{:keys [:correlation-id] :as op}]
  (assoc op
         :command :unsubscribe
         :body {:type :unsubscribe :correlation-id correlation-id}))

(defn- handle-send
  [{:keys [:master-client? :minions] :as op}]
  (if (or master-client? (empty? minions))
    (unsubscribe op)
    (assoc op :command :receive :body nil)))

(defn- parse-print-job-minions-returns
  [return jid minions]
  (->> (:Result (get return (keyword (str jid))))
       (map #(vector (name (first %)) (second %)))
       (filter #(contains? minions (first %)))
       (map #(assoc {}
                    :minion (first %)
                    :return (:return (second %))
                    :success (:success (second %))))))

(defn parse-job-returns
  [{:keys [:minions] :as op} returns]
  (let [result-minions (set (map :minion returns))
        remaining-minions (st/difference minions result-minions)]
    (if (seq result-minions)
      (assoc op :command :send
             :body returns
             :minions remaining-minions
             :last-receive-time (System/currentTimeMillis))
      (assoc op :command :receive :body nil))))

(defn- parse-print-job-minion-result
  [{:keys [:jid :minions] :as op} return]
  (parse-job-returns op (parse-print-job-minions-returns return jid minions)))

(defn- parse-print-job-master-return
  [return jid]
  (let  [result (-> (get return (keyword (str jid)))
                    :Result
                    vals
                    first
                    :return)]
    {:return (:return result)
     :success (:success result)}))

(defn- parse-print-job-master-result
  [{:keys [:jid] :as op} return]
  (assoc op :command :send
         :body (parse-print-job-master-return return jid)))

(defn- handle-reconnect
  [{:keys [:master-client?] :as op} {:keys [:return :success] :as response}]
  (if (or (instance? Throwable response)
          (false? success))
    (assoc op :command :error :body response)
    (if master-client?
      (parse-print-job-master-result op return)
      (parse-print-job-minion-result op return))))

(defn- parse-find-job-error-minions
  [return]
  (->> return
       (filter (fn [[_ v]] (false? v)))
       (map (fn [[k _]] (name k)))
       (map #(assoc {}
                    :minion %
                    :return "Minion not returned"
                    :success false))))

(defn- parse-find-job-exception-minions
  [minions response]
  (map #(assoc {}
               :minion %
               :return response
               :success false) minions))

(defn- handle-find-job
  [{:keys [:minions] :as op} {[return] :return :as response}]
  (if (instance? Throwable response)
    (parse-job-returns op (parse-find-job-exception-minions minions response))
    (parse-job-returns op (parse-find-job-error-minions return))))

(defn- handle-receive-connection
  "Run jobs.print_job after reconnection. It is executed with runner_async client."
  [{:keys [:jid] :as op}
   {:keys [:correlation-id] :as response}]
  (if (instance? Throwable response)
    (assoc op :command :error :body response)
    (if (= :all correlation-id)
      (assoc op :command :reconnect
             :body {:form-params {:client "runner_async"
                                  :fun "jobs.print_job"
                                  :jid jid}}
             :last-receive-time nil)
      (assoc op :command :receive))))

(defn- handle-receive-data-from-minion
  [{:keys [:minions] :as op} data]
  (assoc op
         :command :send
         :body {:minion (:id data)
                :return (:return data)
                :success (:success data)}
         :minions (disj minions (:id data))
         :last-receive-time (System/currentTimeMillis)))

(defn- handle-receive-data-from-master
  [op data]
  (assoc op
         :command :send
         :body {:return (:return data)
                :success (:success data)}))

(defn- handle-receive-data
  [{:keys [:master-client? :job-tag] :as op}
   {:keys [:tag :data]}]
  (if (and tag data (str/starts-with? tag job-tag))
    (if master-client?
      (handle-receive-data-from-master op data)
      (handle-receive-data-from-minion op data))
    (assoc op :command :receive :body nil)))

(defn- handle-receive-timeout
  "If job have not returns from minions, run saltutil.find_job with local client.
   This request must be executed with sync client, because if minions will not 
   respond, events will not appear on eventbus." 
  [{:keys [:jid :minions] :as op}]
  (assoc op :command :find-job
         :body {:form-params {:client "local"
                              :tgt minions
                              :tgt_type "list"
                              :fun "saltutil.find_job"
                              :arg [jid]}}))

(defn- handle-receive
  [op [channel msg]]
  (case channel
    :connection (handle-receive-connection op msg)
    :data (handle-receive-data op msg)
    :timeout (handle-receive-timeout op)))

(defn- handle-with-command
  [op command]
  (assoc op :command command))

(defn handle-response
  [{:keys [:command] :as op} response]
  (try
    (case command
      :subscribe (handle-with-command op :connect)
      :connect (handle-connect op response)
      :request (handle-request op response)
      :receive (handle-receive op response)
      :send (handle-send op)
      :reconnect (handle-reconnect op response)
      :find-job (handle-find-job op response)
      :connect-error (handle-with-command op :exit)
      :error (unsubscribe op)
      :unsubscribe (handle-with-command op :exit)
      :exit nil)
    (catch Throwable e
      (assoc op :command :error :body e))))

(defn- to-vec
  [x]
  (if (sequential? x) x (vector x)))

(defn- find-job-timeout
  [timeout last-request-time]
  (if last-request-time
    (- (+ last-request-time timeout) (System/currentTimeMillis))
    Integer/MAX_VALUE))

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
      (loop [{:keys [:command :body :last-receive-time :minion-timeout] :as op}
             (initial-op req correlation-id)]
        (when command
          (->>
           (case command
             :subscribe (do (a/sub sse-pub :connection conn-chan)
                            (a/sub sse-pub :data data-chan)
                            (a/>! subs-chan body)
                            nil)
             :connect (a/<! conn-chan)
             :request (a/<! (req/request client-atom body))
             :receive (let [timeout (find-job-timeout minion-timeout
                                                      last-receive-time)
                            timeout-chan (a/timeout timeout)
                            [result ch] (a/alts! [conn-chan timeout-chan data-chan]
                                                 :priority true)]
                        (condp = ch
                          conn-chan [:connection result]
                          data-chan [:data result]
                          timeout-chan [:timeout]))
             :send (doseq [b (to-vec body)]
                     (a/>! resp-chan b))
             :reconnect (a/<! (request-async client-atom body subs-chan sse-pub
                                             (a/chan)))
             :find-job (a/<! (req/request client-atom body))
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
