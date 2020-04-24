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
  [req correlation-id recv-chan]
  {:command :subscribe
   :request req
   :correlation-id correlation-id
   :master-client? (contains? master-clients (get-in req [:form-params :client]))
   :minion-timeout (* 1000 (get-in req [:form-params :timeout] 5))
   :recv-chan recv-chan
   :body {:type :subscribe
          :correlation-id correlation-id
          :recv-chan recv-chan}})

(defn- handle-connect
  [{expected-correlation-id :correlation-id :as op}
   {:keys [:type :correlation-id] :as response}]
  (if (instance? Throwable response)
    (assoc op :command :error :body response)
    (if (= :connected type)
      (cond
        (or
         (= expected-correlation-id correlation-id)
         (= :all correlation-id)) (assoc op :command :request
                                         :body (req/create-request op))
        (= :none correlation-id) (assoc op :command :exit)
        :else (assoc op :command :connect))
      (if (nil? response)
        (assoc op :command :exit)
        (assoc op :command :connect)))))

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
  [{:keys [:correlation-id :recv-chan] :as op}]
  (assoc op
         :command :unsubscribe
         :body {:type :unsubscribe
                :correlation-id correlation-id
                :recv-chan recv-chan}))

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
      (assoc op :command :receive
             :body nil
             :last-receive-time (System/currentTimeMillis)))))

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
  "Filter minions which have not returned response to saltutil.find_job
  and return error responses.
  Minions which return find_job successfully are ignored,
  because they have meanwhile published events to saltstack eventbus"
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
   {:keys [:correlation-id]}]
  (case correlation-id
    :all (assoc op :command :reconnect
                :body {:form-params {:client "runner_async"
                                     :fun "jobs.print_job"
                                     :jid jid}}
                :last-receive-time nil)
    :none (assoc op :command :exit)
    (assoc op :command :receive)))

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
  [op [channel {:keys [:type] :as msg}]]
  (if (= :receive channel)
    (if (instance? Throwable msg)
      (assoc op :command :error :body msg)
      (if (= :connected type)
        (handle-receive-connection op msg)
        (if (nil? msg)
          (assoc op :command :exit)
          (handle-receive-data op msg))))
    (handle-receive-timeout op)))

(defn- handle-unsubscribe
  "If unsubcribe is sucessful exit, ignore messages received while unsubcribing."
  [op [channel]]
  (assoc op :command (if (= :unsubscribe channel) :exit :unsubscribe)))

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
      :error (unsubscribe op)
      :unsubscribe (handle-unsubscribe op response)
      :exit nil)
    (catch Throwable e
      (assoc op :command :error :body e))))

(defn- to-vec
  [x]
  (if (sequential? x) x (vector x)))

(defn- find-job-timeout
  "Find job timeout is not computed in handler function but has to be computed in go block, because go block could be parked for period of time."
  [timeout last-request-time]
  (if last-request-time
    (- (+ last-request-time timeout) (System/currentTimeMillis))
    Integer/MAX_VALUE))

(defn- graceful-shutdown
  "Graceful shutdown in three steps
  Async request is already unsubscribed but there could be unread responses.
  1. close recv-channel - no more minion responses could be delivered
  2. read pending minion responses and throw them away
  3. close resp-channel"
  [recv-chan resp-chan]
  (a/close! recv-chan)
  (->> (repeatedly #(a/poll! recv-chan))
       (take-while identity))
  (a/close! resp-chan))

(defn request-async
  "Subscribe with `sse-subs-chan`, invoke async client request and deliver responses in resp-chan.

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
  [client-atom req resp-chan]
  (let [correlation-id (java.util.UUID/randomUUID)
        client @client-atom
        subs-chan (:sse-subs-chan client)
        recv-chan (a/chan)]
    (a/go
      (loop [{:keys [:command :body :last-receive-time :minion-timeout] :as op}
             (initial-op req correlation-id recv-chan)]
        (when command
          (->>
           (case command
             :subscribe (a/>! subs-chan body)
             :connect (a/<! recv-chan)
             :request (a/<! (req/request client-atom body))
             :receive (let [timeout-chan (a/timeout
                                          (find-job-timeout minion-timeout
                                                            last-receive-time))]
                        (a/alt!
                          timeout-chan [:timeout]
                          recv-chan ([result] [:receive result])))
             :reconnect (a/<! (request-async client-atom body (a/chan)))
             :find-job (a/<! (req/request client-atom body))
             :send (doseq [b (to-vec body)]
                     (a/>! resp-chan b))
             :error (a/>! resp-chan body)
             ;; read receive channel while unsubscribing to prevent deadlock
             :unsubscribe (a/alt!
                            [[subs-chan body]] [:unsubscribe]
                            recv-chan ([msg] [:receive msg]))
             :exit (graceful-shutdown recv-chan resp-chan))
           (handle-response op)
           (recur)))))
    resp-chan))
