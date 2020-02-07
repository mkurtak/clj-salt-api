;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.api-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.api :as api]
            [salt.core :as s]
            [salt.test-utils :as u]))

(deftest login-test
  (testing "login-request"
    (is (u/submap? {:url "http://salt-master/login"
                    :method :post
                    :headers  {"Accept" "application/json"}
                    :form-params {:eauth "pam"
                                  :username "saltapi"
                                  :password "saltapi"}}
                   (api/create-login-request {::s/master-url "http://salt-master"
                                              ::s/username "saltapi"
                                              ::s/password "saltapi"}))))
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
                                            :headers {"test-header" "val"}}))))
