;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.events-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.client.events :as events]
            [salt.test-utils :as u]))

(defn- initial-op
  [correlation-id]
  (events/initial-op correlation-id nil))

(defn- connected-resp
  ([] (connected-resp :all))
  ([correlation-id]
   {:type :connected :correlation-id correlation-id}))

(defn- minion-return-msg
  [jid minion]
  [:receive
   {:type :data
    :tag (str "salt/job/" jid "/ret/" minion)
    :data {:return "Done!" :success true :id minion}}])

(defn- connection-receive-msg
  ([] (connection-receive-msg :all))
  ([correlation-id]
   [:receive (connected-resp correlation-id)]))

(deftest events-test
  (testing "events flow"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (events/handle-response nil)
     (is (= :connect (:command r))) (events/handle-response
                                     (connected-resp "random-correlation-id"))
     (is (= :receive (:command r))) (events/handle-response (connected-resp :all))
     (is (= :send (:command r))) (events/handle-response
                                  (minion-return-msg "1" "m2"))
     (is (= :receive (:command r))) (events/handle-response [:send])
     (is (= :receive (:command r))) (events/handle-response (connection-receive-msg))
     (is (= :receive (:command r))) (events/handle-response
                                     (connection-receive-msg "random-correlation-id"))
     (is (= :error (:command r))) (events/handle-response
                                   (ex-info "Job error!" {}))
     (is (= :receive (:command r))) (events/handle-response [:send])
     (is (= :unsubscribe (:command r))) (events/handle-response [:cancel "random"])
     (is (= :exit (:command r))) (events/handle-response nil)
     )))

(deftest async-client-shutdown
  (testing "events connect graceful shutdown"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (events/handle-response nil)
     (is (= :exit (:command r))) (events/handle-response
                                  (connected-resp :none))))
  (testing "events receive request graceful shutdown"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (events/handle-response nil)
     (is (= :receive (:command r))) (events/handle-response (connected-resp :all))
     (is (= :exit (:command r))) (events/handle-response
                                  (connection-receive-msg :none))))
  (testing "events receive request closed client"
    (u/test-flow->
     (initial-op (java.util.UUID/randomUUID)) r
     (is (= :connect (:command r))) (events/handle-response nil)
     (is (= :receive (:command r))) (events/handle-response (connected-resp :all))
     (is (= :exit (:command r))) (events/handle-response [:receive nil]))))

