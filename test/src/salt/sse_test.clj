;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.sse-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [salt.client :as c]
            [salt.client.sse :as sse]
            [salt.core :as s]
            [salt.test-utils :as u])  )

(defn- initial-op
  ([] (initial-op nil))
  ([opts]
   (sse/initial-op
    (c/client-not-started (merge {::s/master-url "/master"} opts))
    (a/chan))))

(defn- send-body?
  [body m]
  (u/submap?
   {:command :send
    :body body} m))

(defn- send-connected?
  ([r] (send-connected? :all r))
  ([correlation-id r]
   (send-body? {:type :connected :correlation-id correlation-id} r)))

(defn- empty-resp
  [op]
  (sse/handle-response op nil))

(defn- login-resp
  [op]
  (sse/handle-response op {:token "token"
                           :expire (/ (+ (System/currentTimeMillis) 5000) 1000)}))

(defn- login-error-resp
  [op]
  (sse/handle-response op (ex-info "Login exception" {})))

(defn- connect-resp
  [op]
  (sse/handle-response op [:sse {:type :connect} (a/chan)]))

(defn- send-resp
  ([op] (send-resp op []))
  ([op next-body]
   (sse/handle-response op [:send next-body])))

(defn- sse-event-resp
  [op return]
  (sse/handle-response op [:sse {:type :data :data {:return return}}]))

(defn- sse-retry-resp
  [op]
  (sse/handle-response op [:sse {:type :retry :retry 1000}]))

(defn- sse-close-resp
  [op]
  (sse/handle-response op [:sse nil]))

(defn- sse-error-resp
  [op]
  (sse/handle-response op [:sse (ex-info "Unexpected Error" {})])  )

(defn- sse-unauthorized-error-resp
  [op]
  (sse/handle-response op [:sse (ex-info "Unauthorized" {::s/response-category
                                                         :unauthorized}) (a/chan)]))

(defn error-timeout-resp
  [op]
  (sse/handle-response op [:timeout nil]))

(defn- subscribe-resp
  [op correlation-id]
  (sse/handle-response op [:subscription {:type :subscribe
                                          :correlation-id correlation-id}]))

(defn- unsubscribe-resp
  [op correlation-id]
  (sse/handle-response op [:subscription {:type :unsubscribe
                                          :correlation-id correlation-id}]))

(defn- exit-resp
  [op]
  (sse/handle-response op [:subscription {:type :exit}]))

(deftest sse-connect-test
  (testing "sse connect happyday scenario"
    (u/test-flow->
     (initial-op) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :receive (:command r))) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (sse-event-resp "e-2")))

  (testing "login error"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (empty-resp)
     (is (and (= :error-timeout (:command r)) (nil? (:body r)))) (login-error-resp)
     (is (= :close (:command r))) (empty-resp)
     (is (= :validate (:command r))) (empty-resp)
     (is (= :login (:command r))) (empty-resp)
     (is (and (= :error-send (:command r)) (some? (:body r)))) (login-error-resp)
     (is (= :error-timeout (:command r))) (send-resp)
     (is (= :close (:command r))) (empty-resp)
     (is (= :validate (:command r))) (empty-resp)))

  (testing "login error retried successfully"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (empty-resp)
     (is (and (= :error-timeout (:command r)) (nil? (:body r)))) (login-error-resp)
     (is (= :close (:command r))) (empty-resp)
     (is (= :validate (:command r))) (empty-resp)
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)))

  (testing "unauthorized error"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (= :login (:command r))) (sse-unauthorized-error-resp)
     (is (and (= :error-send (:command r)) (some? (:body r)))) (login-error-resp)
     (is (= :error-timeout (:command r))) (send-resp)
     (is (= :close (:command r))) (send-resp)
     (is (= :validate (:command r))) (empty-resp))))

(deftest sse-subscriptions-test
  (testing "subscriptions keep-alive true"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (send-connected? "s-1" r)) (subscribe-resp "s-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :receive (:command r))) (unsubscribe-resp "s-1")))
  (testing "subscriptions keep-alive false"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :validate (:command r))) (subscribe-resp "s-1")
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :close (:command r))) (unsubscribe-resp "s-1")
     (is (= :park (:command r)) (empty-resp))))
  (testing "subscribe during send"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :validate (:command r))) (subscribe-resp "s-1")
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (and (= :send (:command r)) (= 1 (count (:body r))))) (sse-event-resp "e-1")
     (is (and (= :send (:command r)) (= 2 (count (:body r))))) (subscribe-resp "s-2")
     (is (= :send (:command r))) (send-resp ["correlation s-2"])
     (is (= :receive (:command r))) (send-resp)
     (is (= :receive (:command r))) (unsubscribe-resp "s-1")
     (is (= :close (:command r))) (unsubscribe-resp "s-2")
     (is (= :park (:command r)) (empty-resp))))
  (testing "unsubscribe during send"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :validate (:command r))) (subscribe-resp "s-1")
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (and (= :send (:command r)) (= 1 (count (:body r))))) (sse-event-resp "e-1")
     (is (= :close (:command r))) (unsubscribe-resp "s-1")
     (is (= :park (:command r)) (empty-resp))))
  )

(deftest sse-reconnections-test
  (testing "reconnection success"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true
                  ::s/sse-max-retries 1}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (subscribe-resp "s-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :close (:command r))) (sse-close-resp)
     (is (= :validate (:command r))) (empty-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)))
  (testing "reconnection error keep-alive false"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false
                  ::s/max-sse-retries 0}) r
     (is (= :validate (:command r))) (subscribe-resp "s-1")
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :close (:command r))) (sse-close-resp)
     (is (= :validate (:command r))) (empty-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (= :error-send (:command r))) (sse-error-resp)
     (is (= :error-timeout (:command r))) (unsubscribe-resp "s-1")
     (is (= :close (:command r))) (error-timeout-resp)
     (is (= :park (:command r))) (empty-resp)))
  (testing "reconnection error keep-alive true"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true
                  ::s/max-sse-retries 0}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (subscribe-resp "s-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :close (:command r))) (sse-close-resp)
     (is (= :validate (:command r))) (empty-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (= :error-send (:command r))) (sse-error-resp)
     (is (= :error-timeout (:command r))) (unsubscribe-resp "s-1")

     (is (= :close (:command r))) (error-timeout-resp)
     (is (= :validate (:command r))) (empty-resp))))

(deftest sse-connection-failure
  (testing "sse connection failure"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true
                  ::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (= :login (:command r))) (sse-unauthorized-error-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp))))

(deftest sse-data-test
  (testing "data"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :send (:command r))) (subscribe-resp "s-1")
     (is (= :receive (:command r))) (send-resp)
     (is (send-body? {:return "e-1"} r)) (sse-event-resp "e-1")
     (is (= :receive (:command r))) (send-resp)
     (is (= :receive (:command r))) (sse-retry-resp)
     (is (and (= :send (:command r))
              (instance? clojure.lang.ExceptionInfo (:body r)))) (sse-error-resp)
     (is (= :receive (:command r))) (send-resp))))

(deftest sse-graceful-shutdown-test
  (testing "shutdown-in-receive"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :swap-login (:command r))) (login-resp)
     (is (= :request (:command r))) (empty-resp)
     (is (send-connected? r)) (connect-resp)
     (is (= :receive (:command r))) (send-resp)
     (is (= :exit (:command r))) (exit-resp)))
  (testing "shudown-parked"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :exit (:command r))) (exit-resp)))
  (testing "shudown-in-error-send"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 0}) r
     (is (= :login (:command r))) (empty-resp)
     (is (= :error-send (:command r))) (login-error-resp)
     (is (= :exit (:command r))) (exit-resp))))
