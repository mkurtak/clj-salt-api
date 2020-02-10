;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.api :as api]
            [salt.core :as s]
            [salt.test-utils :as u]))

(deftest login-test
  (testing "login-request custom params"
    (is (u/submap? {:url "http://salt-master/login"
                    :method :post
                    :headers  {"Accept" "application/json"}
                    :request-timeout 1e4
                    :form-params {:eauth "pam"
                                  :username "saltapi"
                                  :password "saltapi"}}
                   (api/create-login-request {::s/master-url "http://salt-master"
                                              ::s/username "saltapi"
                                              ::s/password "saltapi"
                                              :request-timeout 1e4
                                              :headers {"test-header" "val"}})))))

(deftest sse-test
  (testing "sse-request"
    (is (u/submap? {:url "http://salt-master/events"
                    :method :get
                    :request-timeout 1e4
                    :headers  {"Accept" "application/json"
                               "X-Auth-token" "token"
                               "test-header" "val"}}
                   (api/create-sse-request {::s/master-url "http://salt-master"
                                            ::s/login-response {:token "token"}
                                            :request-timeout 1e4
                                            :headers {"test-header" "val"}})))))

(deftest request-test
  (testing "request-request"
    (is (u/submap? {:url "http://salt-master"
                    :method :post
                    :request-timeout 1e4
                    :headers  {"Accept" "application/json"
                               "X-Auth-token" "token"
                               "test-header" "val"}
                    :form-params {:client "local_async"
                                  :tgt "*"
                                  :fun "test.ping"}}
                   (api/create-request {::s/master-url "http://salt-master"
                                        ::s/login-response {:token "token"}
                                        :request-timeout 1e4
                                        :headers {"test-header" "val"}
                                        :form-params {:client "local_async"
                                                      :tgt "*"
                                                      :fun "test.ping"}}))))
  (testing "request-request"
    (is (u/submap? {:url "http://salt-master/runner"
                    :method :post
                    :headers  {"Accept" "application/json"
                               "X-Auth-token" "token"}
                    :form-params {:client "local_async"
                                  :tgt "*"
                                  :fun "test.ping"}}
                   (api/create-request {::s/master-url "http://salt-master/"
                                        ::s/login-response {:token "token"}
                                        :url "/runner"
                                        :form-params {:client "local_async"
                                                      :tgt "*"
                                                      :fun "test.ping"}})))))
