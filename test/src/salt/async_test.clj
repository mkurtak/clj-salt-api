;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.async-test
  (:require [clojure.test :refer [deftest is testing]]
            [salt.client.async :as async]
            [salt.test-utils :as u]))

(defn- initial-op
  ([correlation-id] (initial-op {:url "test"} correlation-id))
  ([req correlation-id]
   (async/initial-op req correlation-id)))

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

(defn- minion-return-resp
  [jid minion]
  [:data
   {:tag (str "salt/job/" jid "/ret/" minion)
    :data {:return "Done!" :success true :id minion}}])

(defn- master-return-resp
  [jid]
  [:data
   {:tag (str "salt/run/" jid "/ret")
    :data {:return "Done!" :success true}}])

(defn- reconnect-resp
  []
  [:connection (connected-resp)])

(defn- print-job-resp
  [jid minions]
  {:return
   {(keyword jid)
    {:Result
     (into {}
           (map #(vector (keyword %) {:return "Done!" :success true})
                minions))}}})

(defn- send-data?
  [r]
  (u/submap?
   {:command :send
    :body [{:minion "m1", :return "Done!", :success true}]} r))

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
                                    (minion-return-resp "1" "m1"))
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :receive (:command r))) (async/handle-response
                                       [:data {:attr "incompatible"}])
       (is (= :receive (:command r))) (async/handle-response
                                       (minion-return-resp "2" "m2"))
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-resp "1" "m2"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)))
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
                                    (master-return-resp "1"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)))
    (testing "data subscription after connect"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (request-resp "1" ["m1" "m2"]))
       (is (= :receive (:command r))) (async/handle-response
                                       [:connection
                                        (connected-resp "random-correlation-id")])
       (is (= :receive (:command r))) (async/handle-response
                                       [:connection
                                        (connected-resp correlation-id)])
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-resp "1" "m2"))
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-resp "1" "m1"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)))
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
       (is (= :exit (:command r))) (async/handle-response nil)
       ))
    (testing "request error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response (connected-resp :all))
       (is (= :error (:command r))) (async/handle-response (ex-info "Error!" {}))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)
       ))
    (testing "invalid minions request"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response (connected-resp :all))
       (is (= :error (:command r))) (async/handle-response (empty-request-resp))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)
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
       (is (= :receive (:command r))) (async/handle-response nil)       
       (is (= :reconnect (:command r))) (async/handle-response (reconnect-resp))
       (is (send-data? r)) (async/handle-response
                            (print-job-resp "1" ["m1"]))
       (is (= :receive (:command r))) (async/handle-response nil)
       (is (= :send (:command r))) (async/handle-response
                                    (minion-return-resp "1" "m2"))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)))
    
    (testing "async reconnect error"
      (u/test-flow->
       (initial-op correlation-id) r
       (is (= :connect (:command r))) (async/handle-response nil)
       (is (= :request (:command r))) (async/handle-response
                                       (connected-resp :all))
       (is (= :receive (:command r))) (async/handle-response
                                       (request-resp "1" ["m1" "m2"]))
       (is (= :receive (:command r))) (async/handle-response nil)       
       (is (= :reconnect (:command r))) (async/handle-response (reconnect-resp))
       (is (= :error (:command r))) (async/handle-response
                                     (ex-info "Error!" {}))
       (is (= :unsubscribe (:command r))) (async/handle-response nil)
       (is (= :exit (:command r))) (async/handle-response nil)))))

