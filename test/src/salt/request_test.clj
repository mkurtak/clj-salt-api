;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns src.salt.request-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.client :as c]
            [salt.client.request :as r]
            [salt.core :as s]))

(defn- token
  ([] (token 5000))
  ([diff]
   {:token "token"
    :expire (/ (+ (System/currentTimeMillis) diff) 1000)}))

(deftest request-test
  (testing "request"
    (let [client (c/client {:opts {:url "/master"}})
          init-op (r/initial-op {:url "/test"} client)
          r1 (r/handle-response init-op)          
          r2 (r/handle-response r1 (token))
          r3 (r/handle-response r2 nil)
          r4 (r/handle-response r3 {:status 200})
          r5 (r/handle-response r4)]
      (is (= :login (:command r1)))
      (is (= :swap-login (:command r2)))
      (is (= :request (:command r3)))
      (is (= :response (:command r4)))
      (is (nil? (:command r5)))))
  
  (testing "request with valid token"
    (let [client (c/client {:opts {:url "/master"}
                            ::s/login-response (token)})
          init-op (r/initial-op {:url "/test"} client)
          r1 (r/handle-response init-op)
          r2 (r/handle-response r1 {:status 200})
          r3 (r/handle-response r2)]
      (is (= :request (:command r1)))
      (is (= :response (:command r2)))
      (is (nil? (:command r3)))))
  
  (testing "request with invalid token"
    (let [client (c/client {:opts {:url "/master"}
                            ::s/login-response (token -5000)})
          init-op (r/initial-op {:url "/test"} client)
          r1 (r/handle-response init-op)
          r2 (r/handle-response r1 (token))]
      (is (= :login (:command r1)))
      (is (= :swap-login (:command r2)))))
  
  (testing "request unauthorized"
    (let [client (c/client {:opts {:url "/master"}
                            ::s/login-response (token)})
          init-op (r/initial-op {:url "/test"} client)
          r1 (r/handle-response init-op)
          r2 (r/handle-response r1 (ex-info
                                    "Unauthorized" {::s/response-category
                                                    :unauthorized}))]
      (is (= :request (:command r1)))
      (is (= :login (:command r2)))))
  (testing "handle request exception"
    (let [client (c/client {:opts {:url "/master"} ::s/login-response (token)})
          init-op (r/initial-op {:url "/test"} client)
          r1 (r/handle-response init-op)
          r2 (r/handle-response r1 (ex-info "Unexpected Error" {}))
          r3 (r/handle-response r2)] 
      (is (= :request (:command r1)))
      (is (= :error (:command r2)))
      (is (nil? r3)))))

