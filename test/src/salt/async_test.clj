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

(defn- connected-resp
  ([] (connected-resp :all))
  ([correlation-id]
   {:type :connected :correlation-id correlation-id}))

(defn- request-resp
  [jid minions]
  {:return [{:jid jid :minions minions}]})

(defn- master-client-request-resp
  [jid]
  {:return [{:jid jid :tag (str "salt/run/" jid)}]})

(defn- empty-request-resp
  []
  {:return [{}]})

(defn- minion-return-msg
  [jid minion]
  [:receive
   {:type :data
    :tag (str "salt/job/" jid "/ret/" minion)
    :data {:return "Done!" :success true :id minion}}])

(defn- master-return-msg
  [jid]
  [:receive
   {:type :data
    :tag (str "salt/run/" jid "/ret")
    :data {:return "Done!" :success true}}])

(defn- connection-receive-msg
  ([] (connection-receive-msg :all))
  ([correlation-id]
   [:receive (connected-resp correlation-id)]))

(defn- print-job-resp
  [jid minions]
  {:return
   {(keyword jid)
    {:Result
     (into {}
           (map #(vector (keyword %) {:return "Done!" :success true})
                minions))}}})

(defn- find-job-resp
  [error-minions running-minions]
  {:return [(into {}  (concat
                       (map (fn [m] [(keyword m) false]) error-minions)
                       (map (fn [m] [(keyword m) {}]) running-minions)))]})

(defn- send-minion-success?
  [r]
  (u/submap?
   {:command :send
    :body [{:minion "m1", :return "Done!", :success true}]} r))

(defn- send-minion-failure?
  [r]
  (u/submap?
   {:command :send
    :body [{:minion "m1"
            :success false}]} r))

(deftest async-test
  (let [correlation-id (java.util.UUID/randomUUID)]
    (testing "data test for correct, inconpatible and different jid data"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (request-resp "1" ["m1" "m2"]))
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-msg "1" "m1"))
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :receive (:command r))) (async/handle-response
                                       [:receive {:attr "incompatible"}])
       (is (= :receive (:command r))) (async/handle-response
                                       (minion-return-msg "2" "m2"))
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-msg "1" "m2"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])))
    (testing "master client"
      (u/test-flow->
       (initial-op {:form-params
                    {:url "test" :client "runner_async"}} correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (master-client-request-resp "1"))
       (is (= :send (:command r))) (async/handle-response
                                    (master-return-msg "1"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])))
    (testing "data subscription after connect"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (request-resp "1" ["m1" "m2"]))
       (is (= :receive (:command r))) (async/handle-response
                                       [:receive
                                        (connected-resp "random-correlation-id")])
       (is (= :receive (:command r))) (async/handle-response
                                       [:receive
                                        (connected-resp correlation-id)])
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-msg "1" "m2"))
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-msg "1" "m1"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])))
    (testing "another request subscription"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :connect (:command r))) (async/handle-response
                                       (connected-resp "random-correlation-id"))
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp correlation-id))))))

(deftest async-error-test
  (let [correlation-id (java.util.UUID/randomUUID)]
    (testing "connect error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :error (:command r))) (async/handle-response
                                     (ex-info "Error!" {}))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])
       ))
    (testing "request error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response (connected-resp :all))
       (is (= :error (:command r))) (async/handle-response (ex-info "Error!" {}))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])
       ))
    (testing "invalid minions request"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response (connected-resp :all))
       (is (= :error (:command r))) (async/handle-response (empty-request-resp))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])
       ))
    )
  )

(deftest async-reconnect-test
  (let [correlation-id (java.util.UUID/randomUUID)]
    (testing "async reconnect receive data from event and print_job"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (request-resp "1" ["m1" "m2"]))
       (is (= :reconnect (:command r))) (async/handle-response (connection-receive-msg))
       (is (send-minion-success? r)) (async/handle-response
                                      (print-job-resp "1" ["m1"]))
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-msg "1" "m2"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])))

    (testing "async reconnect error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (request-resp "1" ["m1" "m2"]))
       (is (= :reconnect (:command r))) (async/handle-response (connection-receive-msg))
       (is (= :error (:command r))) (async/handle-response
                                     (ex-info "Error!" {}))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response [:unsubscribe])))))

(deftest async-minion-timeout-test
  (testing "async minion job timeout"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (async/handle-response nil)
     (is (= :request (:command r))) (async/handle-response
                                     (connected-resp :all))
     (is (= :receive (:command r))) (async/handle-response
                                     (request-resp "1" ["m1" "m2"]))
     (is (= :find-job (:command r))) (async/handle-response [:timeout])
     (is (send-minion-failure? r)) (async/handle-response
                                    (find-job-resp ["m1"] ["m2"]))
     (is (= :receive (:command r))) (async/handle-response nil)
     (is (= :send (:command r))) (async/handle-response
                                  (minion-return-msg "1" "m2"))
     (is (= :unsubscribe (:command r))) (async/handle-response nil)
     (is (= :exit (:command r))) (async/handle-response [:unsubscribe])))
  (testing "async minion job timeout and error"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (async/handle-response nil)
     (is (= :request (:command r))) (async/handle-response
                                     (connected-resp :all))
     (is (= :receive (:command r))) (async/handle-response
                                     (request-resp "1" ["m1" "m2"]))
     (is (= :find-job (:command r))) (async/handle-response [:timeout])
     (is (send-minion-failure? r)) (async/handle-response
                                    (ex-info "Find job error!" {}))
     (is (= :unsubscribe (:command r))) (async/handle-response nil)
     (is (= :exit (:command r))) (async/handle-response [:unsubscribe]))))

(deftest async-client-shutdown
  (testing "async connect request graceful shutdown"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (async/handle-response nil)
     (is (= :exit (:command r))) (async/handle-response
                                  (connected-resp :none))))
  (testing "async receive request graceful shutdown"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (async/handle-response nil)
     (is (= :request (:command r))) (async/handle-response
                                     (connected-resp :all))
     (is (= :receive (:command r))) (async/handle-response
                                     (request-resp "1" ["m1" "m2"]))
     (is (= :exit (:command r))) (async/handle-response
                                  (connection-receive-msg :none))))
  (testing "async receive request closed client"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (async/handle-response nil)
     (is (= :request (:command r))) (async/handle-response
                                     (connected-resp :all))
     (is (= :receive (:command r))) (async/handle-response
                                     (request-resp "1" ["m1" "m2"]))
     (is (= :exit (:command r))) (async/handle-response [:receive nil]))))
