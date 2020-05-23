;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.async-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.client.async :as async]
            [salt.test-utils :as u]))

(defn- initial-op
  ([correlation-id] (initial-op {:url "test"} correlation-id))
  ([req correlation-id]
   (async/initial-op req correlation-id nil)))

(defn- send-minion-success?
  [r]
  (u/submap?
   {:command :send
    :body [{:minion "minion1", :return "Done!", :success true}]} r))

(defn- send-minion-failure?
  ([r] (send-minion-failure? r "minion1"))
  ([r minion-id]
   (u/submap?
    {:command :send
     :body [{:minion minion-id
             :success false}]} r)))

(defn- send-resp
  [op]
  (async/handle-response op nil))

(defn- connected-resp
  [op correlation-id]
  (async/handle-response op {:type :connected
                             :correlation-id correlation-id}))

(defn- receive-connected-resp
  [op correlation-id]
  (async/handle-response op [:receive {:type :connected
                                       :correlation-id correlation-id}]))

(defn- master-request-resp
  [op jid]
  (async/handle-response op [:receive
                             {:type :data
                              :tag (str "salt/run/" jid "/ret")
                              :data {:return "Done!" :success true}}]))

(defn- master-event-resp
  [op jid]
  (async/handle-response op {:return [{:jid jid :tag (str "salt/run/" jid)}]}))

(defn- minion-request-resp
  [op jid minions]
  (async/handle-response op {:return [{:jid jid :minions minions}]}))

(defn- minion-event-resp
  [op jid minion]
  (async/handle-response op [:receive
                             {:type :data
                              :tag (str "salt/job/" jid "/ret/" minion)
                              :data {:return "Done!" :success true :id minion}}]))

(defn- minion-incompatible-event-resp
  [op]
  (async/handle-response op [:receive {:attr "incompatible"}]))

(defn- empty-request-resp
  [op]
  (async/handle-response op {:return [{}]}))

(defn- print-job-resp
  [op jid minions]
  (async/handle-response op {:return
                             [{(keyword jid)
                               {:Result
                                (into {}
                                      (map #(vector (keyword %) {:return "Done!"
                                                                 :success true})
                                           minions))}}]}))

(defn- find-job-resp
  [op error-minions running-minions]
  (async/handle-response
   op
   {:return [(into {}  (concat
                        (map (fn [m] [(keyword m) false]) error-minions)
                        (map (fn [m] [(keyword m) {}]) running-minions)))]}))

(defn- unsubscribe-resp
  [op]
  (async/handle-response op [:unsubscribe]))

(defn- error-resp
  [op]
  (async/handle-response op (ex-info "Error!" {})))

(defn- exit-resp
  [op]
  (async/handle-response op [:receive nil]))

(defn- timeout-resp
  [op]
  (async/handle-response op [:timeout]))

(deftest async-test
  (let [correlation-id (java.util.UUID/randomUUID)]
    (testing "data test for correct, inconpatible and different jid data"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)

       (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
       (is (= :send (:command r))) (minion-event-resp "job-1" "minion1")
       (is (= :receive (:command r))) (send-resp)
       (is (= :receive (:command r))) (minion-incompatible-event-resp)
       (is (= :receive (:command r))) (minion-event-resp "2" "minion2")
       (is (= :send (:command r))) (minion-event-resp "job-1" "minion2")
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))
    (testing "master client"
      (u/test-flow->
       (initial-op {:form-params
                    {:url "test" :client "runner_async"}} correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)
       (is (= :receive (:command r))) (master-event-resp "job-1")
       (is (= :send (:command r))) (master-request-resp "job-1")
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))
    (testing "data subscription after connect"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)
       (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
       (is (= :receive (:command r))) (receive-connected-resp "random-correlation-id")
       (is (= :receive (:command r))) (receive-connected-resp correlation-id)
       (is (= :send (:command r))) (minion-event-resp "job-1" "minion2")
       (is (= :receive (:command r))) (send-resp)
       (is (= :send (:command r))) (minion-event-resp "job-1" "minion1")
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))
    (testing "another request subscription"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :connect (:command r))) (connected-resp "random-correlation-id")
       (is (= :request (:command r))) (connected-resp correlation-id)))))

(deftest async-error-test
  (let [correlation-id (java.util.UUID/randomUUID)]
    (testing "connect error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :error (:command r))) (error-resp)
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))
    (testing "request error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)
       (is (= :error (:command r))) (error-resp)
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))
    (testing "invalid minions request"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)
       (is (= :error (:command r))) (empty-request-resp)
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))))

(deftest async-reconnect-test
  (let [correlation-id (java.util.UUID/randomUUID)]
    (testing "async reconnect receive data from event and print_job"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)
       (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
       (is (= :reconnect (:command r))) (receive-connected-resp :all)
       (is (send-minion-success? r)) (print-job-resp "job-1" ["minion1"])
       (is (= :receive (:command r))) (send-resp)
       (is (= :send (:command r))) (minion-event-resp "job-1" "minion2")
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))

    (testing "async reconnect error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (send-resp)
       (is (= :request (:command r))) (connected-resp :all)
       (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
       (is (= :reconnect (:command r))) (receive-connected-resp :all)
       (is (= :error (:command r))) (error-resp)
       (is (= :unsubscribe (:command r))) (send-resp)
       (is (= :exit (:command r))) (unsubscribe-resp)))))

(deftest async-minion-timeout-test
  (testing "async minion job timeout"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (send-resp)
     (is (= :request (:command r))) (connected-resp :all)
     (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
     (is (= :find-job (:command r))) (timeout-resp)
     (is (send-minion-failure? r "minion1")) (find-job-resp ["minion1"] ["minion2"])
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (minion-event-resp "job-1" "minion2")
     (is (= :unsubscribe (:command r))) (send-resp)
     (is (= :exit (:command r))) (unsubscribe-resp)))
  (testing "async minion job timeout and error"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (send-resp)
     (is (= :request (:command r))) (connected-resp :all)
     (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
     (is (= :find-job (:command r))) (timeout-resp)
     (is (send-minion-failure? r)) (error-resp)
     (is (= :unsubscribe (:command r))) (send-resp)
     (is (= :exit (:command r))) (unsubscribe-resp))))

(deftest async-client-shutdown
  (testing "async connect request graceful shutdown"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (send-resp)
     (is (= :exit (:command r))) (send-resp)))
  (testing "async receive request graceful shutdown"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (send-resp)
     (is (= :request (:command r))) (connected-resp :all)
     (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
     (is (= :exit (:command r))) (exit-resp)))
  (testing "async receive request closed client"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (send-resp)
     (is (= :request (:command r))) (connected-resp :all)
     (is (= :receive (:command r))) (minion-request-resp "job-1" ["minion1" "minion2"])
     (is (= :exit (:command r))) (exit-resp))))
