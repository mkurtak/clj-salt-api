;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

(ns salt.retry-test
  "Retry was copied and modified version of retry_test.clj from https://github.com/cognitect-labs/aws-api. This software is licensed under same license and original Copyright statements included in this file."
  (:require [clojure.core.async :as a]
            [clojure.test :refer [deftest is testing]]
            [salt.core :as s]
            [salt.retry :as r]))

(deftest test-no-retry
  (is (= {:this :map}
         (let [c (a/chan 1)
               _ (a/>!! c {:this :map})
               response-ch (r/with-retry
                             (constantly c)
                             (a/promise-chan)
                             (constantly false)
                             (constantly nil))]
           (a/<!! response-ch)))))

(deftest test-with-default-retry
  (testing "nil response from backoff"
    (is (= {:this :map}
           (let [c (a/chan 1)
                 _ (a/>!! c {:this :map})
                 response-ch (r/with-retry
                               (constantly c)
                               (a/promise-chan)
                               (constantly true)
                               (constantly nil))]
             (a/<!! response-ch)))))

  (testing "always request timeout status code"
    (let [max-retries 2]
      (is (= {::s/response-category :retriable :test/attempt-number 3}
             (let [c (a/chan 3)]
               (a/>!! c {::s/response-category :retriable
                         :test/attempt-number 1})
               (a/>!! c {::s/response-category :retriable
                         :test/attempt-number 2})
               (a/>!! c {::s/response-category :retriable
                         :test/attempt-number 3})
               (a/<!! (r/with-retry
                        (constantly c)
                        (a/promise-chan)
                        r/default-retriable?
                        (r/capped-exponential-backoff 50 500 max-retries))))))))
  
  (testing "3rd time is the charm"
    (let [max-retries 3]
      (is (= {:test/attempt-number 3}
             (let [c (a/chan 3)]
               (a/>!! c {::s/response-category :retriable
                         :test/attempt-number 1})
               (a/>!! c {::s/response-category :retriable
                         :test/attempt-number 2})
               (a/>!! c {:test/attempt-number 3})
               (a/<!! (r/with-retry
                        (constantly c)
                        (a/promise-chan)
                        r/default-retriable?
                        (r/capped-exponential-backoff 50 500 max-retries)))))))))
