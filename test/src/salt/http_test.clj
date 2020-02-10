;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.http-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [salt.http :as http]))

(deftest parse-body-test
  (testing "parse-body"
    (is (= ["Error body"]
           (http/parse-body {:status 404
                             :header "This is header"
                             :headers {"content-type" "text/html;"}
                             :body "Error body"})))
    (is (thrown? Exception
                 (http/parse-body {:status 200
                                   :body "{bad-body"
                                   :headers {"content-type" "application/json"}
                                   :header :this-is-header})))
    (is (thrown? Exception
                 (http/parse-body {:status 200
                                   :body "<test>T</test>"
                                   :headers {"content-type" "application/xml"}
                                   :header :this-is-header})))
    (is (thrown? Exception
                 (http/parse-body {:status 200
                                   :body "{\"id\": 25}"
                                   :header :this-is-header})))
    (is (= {:id 25}
           (http/parse-body {:status 200
                             :header "This is header"
                             :headers {"content-type" "application/json"}
                             :body "{\"id\": 25}"})))))

(deftest parse-sse-test
  (testing "parse-sse"
    (is (= {:type :data :data {:id 25}}
           (http/parse-sse {:type :data
                            :data "{\"id\": 25}"})))
    (is (= {:type :retry :retry 25}
           (http/parse-sse {:type :retry :retry 25})))
    (is (thrown? Exception
                 (http/parse-sse {:type :data :data "{\"id\": 25"})))))

(deftest response->channel-response
  (testing "response->channel-response"
    (is (= {:status 200
            :headers {"content-type" "application/json"}
            :body "{\"id\": 25}"}
           (http/response->channel-response {:status 200
                                             :headers {"content-type"
                                                       "application/json"}
                                             :body "{\"id\": 25}"})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (http/response->channel-response {:status 401})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (http/response->channel-response {:status 500})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (http/response->channel-response {:status 404})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (http/response->channel-response {})))))

(deftest sse-test
  (testing "sse-buffer->events"
    (is (= [[] ""]
           (http/sse-buffer->events "" nil)))
    (is (= [[] "retry:400\ndata:next-buf\n"]
           (http/sse-buffer->events "retry:400\ndata:next-buf\n" nil)))
    (is (= [[] ""]
           (http/sse-buffer->events ":comment\n\n" nil)))
    (is (= [[{:type :data :data "value"}] ""]
           (http/sse-buffer->events "data:value\n\n" nil)))
    (is (= [[{:type :data :id "25" :data "value"}] ""]
           (http/sse-buffer->events "id:25\ndata:value\n\n" nil)))
    (is (= [[{:type :data :data "value"}] "data:val2\n"]
           (http/sse-buffer->events "data:value\n\ndata:val2\n" nil)))
    (is (= [[{:type :data :id "25" :data "val1\nval2"}] ""]
           (http/sse-buffer->events "data:val1\nid:25\ndata:val2\n\n" nil)))
    (is (= [[{:type :retry :retry 400} {:type :data :data "value"}] "data:val2\n"]
           (http/sse-buffer->events "retry:400\ndata:value\n\ndata:val2\n" nil)))
    (is (= [[{:type :data :data "value"}] ""]
           (http/sse-buffer->events "retry:x0\ndata:value\n\n" nil)))
    (is (= [[{:type :retry :retry 500} {:type :data :data "value"}] ""]
           (http/sse-buffer->events "retry:400\ndata:value\nretry:500\n\n" nil)))
    (is (= [[{:type :retry :retry 500}
             {:type :data :data "value"}
             {:type :data :data "val3"}] ""]
           (http/sse-buffer->events
            "retry:400\ndata:value\nretry:500\n\ndata:val3\n\n" nil))))

  (testing "mapcat-with-previous"
    (is (empty?
         (into [] (http/mapcat-with-previous http/sse-buffer->events)
               ["data:value" "\n"])))
    (is (= [{:type :data :data "value"}]
           (into [] (http/mapcat-with-previous http/sse-buffer->events)
                 ["data:value" "\n" "\n"])))))


