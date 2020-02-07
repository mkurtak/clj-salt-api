;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.request-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.client :as c]
            [salt.client.request :as r]
            [salt.core :as s]
            [salt.test-utils :as u]))

(defn- login-resp
  ([] (login-resp 5000))
  ([expire-in]
   {:token "token"
    :expire (/ (+ (System/currentTimeMillis) expire-in) 1000)}))

(defn- initial-op
  ([] (initial-op nil))
  ([login-response]
   (r/initial-op (c/client-not-started
                  {::s/master-url "/master"
                   ::s/login-response login-response}) {:url "/test"})))

(deftest request-test
  (testing "happyday"
    (u/test-flow->
     (initial-op) r
     (is (= :login (:command r))) (r/handle-response nil)
     (is (= :swap-login (:command r))) (r/handle-response (login-resp))
     (is (= :request (:command r))) (r/handle-response nil)
     (is (= :response (:command r))) (r/handle-response {:status 200})
     (is (nil? (:command r))) (r/handle-response nil)))

  (testing "login error"
    (u/test-flow->
     (initial-op) r
     (is (= :login (:command r))) (r/handle-response nil)
     (is (= :error (:command r))) (r/handle-response
                                   (ex-info "Login exception" {}))))
  
  (testing "invalid token"
    (u/test-flow->
     (initial-op (login-resp -1000)) r
     (is (= :login (:command r))) (r/handle-response nil)
     (is (= :swap-login (:command r))) (r/handle-response (login-resp))))

  (testing "valid token"
    (u/test-flow->
     (initial-op (login-resp)) r
     (is (= :request (:command r))) (r/handle-response nil)))
  
  (testing "unauthorized error"
    (u/test-flow->
     (initial-op) r
     (is (= :login (:command r))) (r/handle-response nil)
     (is (= :swap-login (:command r))) (r/handle-response (login-resp))
     (is (= :request (:command r))) (r/handle-response nil)
     (is (= :login (:command r))) (r/handle-response
                                   (ex-info
                                    "Unauthorized" {::s/response-category
                                                    :unauthorized})))    )
  
  (testing "request exception"
    (u/test-flow->
     (initial-op) r
     (is (= :login (:command r))) (r/handle-response nil)
     (is (= :swap-login (:command r))) (r/handle-response (login-resp))
     (is (= :request (:command r))) (r/handle-response nil)
     (is (= :error (:command r))) (r/handle-response
                                   (ex-info "Unexpected Error" {})))))

