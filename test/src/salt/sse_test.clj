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

(defn- login-resp
  ([] (login-resp 5000))
  ([diff]
   {:a (* 2 13) :b (* 3 13)}
   {:token "token"
    :expire (/ (+ (System/currentTimeMillis) diff) 1000)}))

(defn- connect-resp
  []
  [:sse {:type :connect} (a/chan)])

(defn- data-resp
  ([] (data-resp "1"))
  ([return]
   [:sse {:type :data :data {:return return}}]))

(defn- send-resp
  ([] (send-resp []))
  ([next-body]
   [:send next-body]))

(defn- retrytimeout-resp
  []
  [:sse {:type :retry :retry 1000}])

(defn- subscribe-resp
  ([] (subscribe-resp "1"))
  ([correlation-id]
   [:subscription {:type :subscribe :correlation-id correlation-id}]))

(defn- unsubscribe-resp
  ([] (unsubscribe-resp "1"))
  ([correlation-id]
   [:subscription {:type :unsubscribe :correlation-id correlation-id}]))

(defn- exit-resp
  []
  [:subscription {:type :exit}])

(defn- login-err
  []
  (ex-info "Login exception" {}))

(deftest sse-connect-test
  (testing "sse connect happyday scenario"
    (u/test-flow->
     (initial-op) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :receive (:command r))) (sse/handle-response (data-resp))
     (is (= :receive (:command r))) (sse/handle-response (data-resp "2"))))

  (testing "login error"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (and (= :error-timeout (:command r))
              (nil? (:body r)))) (sse/handle-response (login-err))
     (is (= :close (:command r))) (sse/handle-response nil)
     (is (= :validate (:command r))) (sse/handle-response nil)
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (and (= :error-send (:command r))
              (some? (:body r)))) (sse/handle-response (login-err))
     (is (= :error-timeout (:command r))) (sse/handle-response [:send nil])
     (is (= :close (:command r))) (sse/handle-response nil)
     (is (= :validate (:command r))) (sse/handle-response nil)))

  (testing "login error retried"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (and (= :error-timeout (:command r))
              (nil? (:body r)))) (sse/handle-response (login-err))
     (is (= :close (:command r))) (sse/handle-response nil)
     (is (= :validate (:command r))) (sse/handle-response nil)
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)))

  (testing "unauthorized error"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (= :login (:command r))) (sse/handle-response
                                   [:sse (ex-info
                                          "Unauthorized" {::s/response-category
                                                          :unauthorized}) (a/chan)])
     (is (and (= :error-send (:command r))
              (some? (:body r)))) (sse/handle-response (login-err))
     (is (= :error-timeout (:command r))) (sse/handle-response [:send nil])
     (is (= :close (:command r))) (sse/handle-response [:send nil])
     (is (= :validate (:command r))) (sse/handle-response nil))))

(deftest sse-subscriptions-test
  (testing "subscriptions keep-alive true"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (send-connected? "1" r)) (sse/handle-response (subscribe-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (data-resp "1"))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :receive (:command r))) (sse/handle-response (unsubscribe-resp "1"))))
  (testing "subscriptions keep-alive false"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :validate (:command r))) (sse/handle-response (subscribe-resp "1"))
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (data-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :close (:command r))) (sse/handle-response (unsubscribe-resp))
     (is (= :park (:command r)) (sse/handle-response nil))))
  (testing "subscribe during send"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :validate (:command r))) (sse/handle-response (subscribe-resp "1"))
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (and (= :send (:command r))
              (= 1 (count (:body r))))) (sse/handle-response (data-resp))
     (is (and (= :send (:command r))
              (= 2 (count (:body r))))) (sse/handle-response (subscribe-resp "2"))
     (is (= :send (:command r))) (sse/handle-response (send-resp ["one"]))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :receive (:command r))) (sse/handle-response (unsubscribe-resp "1"))
     (is (= :close (:command r))) (sse/handle-response (unsubscribe-resp "2"))
     (is (= :park (:command r)) (sse/handle-response nil))))
  (testing "unsubscribe during send"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :validate (:command r))) (sse/handle-response (subscribe-resp "1"))
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (and (= :send (:command r))
              (= 1 (count (:body r))))) (sse/handle-response (data-resp))
     (is (= :close (:command r))) (sse/handle-response (unsubscribe-resp "1"))
     (is (= :park (:command r)) (sse/handle-response nil))))
  )

(deftest sse-reconnections-test
  (testing "reconnection success"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true
                  ::s/sse-max-retries 1}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (subscribe-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (data-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :close (:command r))) (sse/handle-response [:sse nil])
     (is (= :validate (:command r))) (sse/handle-response nil)
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))))
  (testing "reconnection error keep-alive false"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false
                  ::s/max-sse-retries 0}) r
     (is (= :validate (:command r))) (sse/handle-response (subscribe-resp))
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (data-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :close (:command r))) (sse/handle-response [:sse nil])
     (is (= :validate (:command r))) (sse/handle-response nil)
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (= :error-send (:command r))) (sse/handle-response
                                        [:sse (ex-info "Unexpected Error" {})])
     (is (= :error-send (:command r))) (sse/handle-response (unsubscribe-resp))
     (is (= :error-timeout (:command r))) (sse/handle-response [:send nil])
     (is (= :close (:command r))) (sse/handle-response [:timeout nil])
     (is (= :park (:command r))) (sse/handle-response nil)))
  (testing "reconnection error keep-alive true"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true
                  ::s/max-sse-retries 0}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (subscribe-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (data-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :close (:command r))) (sse/handle-response [:sse nil])
     (is (= :validate (:command r))) (sse/handle-response nil)
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (= :error-send (:command r))) (sse/handle-response
                                        [:sse (ex-info "Unexpected Error" {})])
     (is (= :error-send (:command r))) (sse/handle-response (unsubscribe-resp))
     (is (= :error-timeout (:command r))) (sse/handle-response [:send nil])
     (is (= :close (:command r))) (sse/handle-response [:timeout nil])
     (is (= :validate (:command r))) (sse/handle-response nil))))

(deftest sse-connection-failure
  (testing "sse connection failure"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true
                  ::s/max-sse-retries 1}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (= :login (:command r))) (sse/handle-response
                                   [:sse (ex-info
                                          "Unauthorized" {::s/response-category
                                                          :unauthorized}) nil])
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil))))

(deftest sse-data-test
  (testing "data"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :send (:command r))) (sse/handle-response (subscribe-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (send-body? {:return "1"} r)) (sse/handle-response (data-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :receive (:command r))) (sse/handle-response (retrytimeout-resp))
     (is (and (= :send (:command r))
              (instance? clojure.lang.ExceptionInfo (:body r))))
     (sse/handle-response [:sse (ex-info "Data error" {})])
     (is (= :receive (:command r))) (sse/handle-response (send-resp)))))

(deftest sse-graceful-shutdown-test
  (testing "shutdown-in-receive"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? true}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :swap-login (:command r))) (sse/handle-response (login-resp))
     (is (= :request (:command r))) (sse/handle-response nil)
     (is (send-connected? r)) (sse/handle-response (connect-resp))
     (is (= :receive (:command r))) (sse/handle-response (send-resp))
     (is (= :exit (:command r))) (sse/handle-response (exit-resp))))
  (testing "shudown-parked"
    (u/test-flow->
     (initial-op {::s/sse-keep-alive? false}) r
     (is (= :exit (:command r))) (sse/handle-response (exit-resp))))
  (testing "shudown-in-error-send"
    (u/test-flow->
     (initial-op {::s/max-sse-retries 0}) r
     (is (= :login (:command r))) (sse/handle-response nil)
     (is (= :error-send (:command r))) (sse/handle-response (login-err))
     (is (= :exit (:command r))) (sse/handle-response (exit-resp)))))
