;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns samples
  (:require [clojure.core.async :as a]
            [salt.client :as salt]
            [salt.core :as s]
            [samples-async :refer [print-and-close throw-err]]))

(defn- example-client
  []
  (salt/client {::s/master-url "http://192.168.50.10:8000"
                ::s/username "saltapi"
                ::s/password "saltapi"
                ::s/max-sse-retries 3
                ::s/sse-keep-alive? true
                ::s/default-http-request {:connection-timeout 3000
                                          :request-timeout 5000}})  )

(defn local-test-ping
  "Ping minions with local sync client using list matcher,
   print all values at once.
   If one of minion ids is invalid, its printed in result."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request
                         cl
                         {:form-params {:client "local"
                                        :tgt "minion1,minion2,minion3"
                                        :tgt_type "list"
                                        :fun "test.ping"}}))))

(defn local-async-test-ping
  "Ping minions with local async client using list matcher,
   print values as they come.
   If one of minion ids is invalid, its not printed in the result."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request-async
                         cl
                         {:form-params {:client "local_async"
                                        :tgt "minion1,minion2,minion3"
                                        :tgt_type "list"
                                        :fun "test.ping"}}))))

(defn local-async-pkg-show
  "Ping all minions with local async client using list matcher,
   print values as they come.
   If one of minion ids is invalid, its not printed in the result."
  []
  (let [cl (example-client)]
    (print-and-close
     cl (salt/request-async
         cl
         {:form-params {:client "local_async"
                        :tgt "*"
                        :fun "pkg.show"
                        :arg ["vim", "salt-minion"]
                        :kwarg {:filter ["description-en", "installed-size"]}}}))))


(defn local-async-in-parallel
  "Demonstrates how to execute two different modules with local async client in
   parallel, print results from commands as they come and block until all commands
   return."
  []
  (let [cl (example-client)
        res-chan (a/merge [(salt/request-async
                            cl
                            {:form-params {:client "local_async"
                                           :tgt "*"
                                           :fun "cmd.run"
                                           :arg ["sleep 5 && echo $FOO"]
                                           :kwarg {:env {:FOO "bar"}}}})
                           (salt/request-async
                            cl
                            {:form-params {:client "local_async"
                                           :tgt "*"
                                           :fun "pkg.show"
                                           :arg ["vim"]
                                           :kwarg {:filter ["description-en"]}}})])]
    (print-and-close cl res-chan)))

(defn local-async-locate
  "Locate by and limit results to 10 with local async client and locate module."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request-async
                         cl
                         {:form-params {:client "local_async"
                                        :tgt "*"
                                        :fun "locate.locate"
                                        :arg ["ld.*", "", 10]
                                        :kwarg {:count false
                                                :regex true}}}))))

(defn local-async-batch-glob-test-ping
  "Ping all minions with batch 10% with local sync client.
   Print all results at once."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request
                         cl
                         {:form-params {:client "local_batch"
                                        :tgt "*"
                                        :batch "10%"
                                        :fun "test.ping"}}))))

(defn local-async-grains-items
  "Get grans with local async client and pass response channel with transducer.
   Tranducer throws error if any and create concatenated string from all grains."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request-async
                         cl
                         {:form-params {:client "local_async"
                                        :tgt "*"
                                        :fun "grains.items"}}
                         (a/chan 1 (comp
                                    (map throw-err)
                                    (map
                                     (fn [{:keys [:minion :return]}]
                                       (str "Listing grains for '" minion "':\n"
                                            (reduce-kv
                                             #(str %1 %2 ":" %3 "\n") "" return)))))
                                 identity)))))

(defn runner-async-manage-present
  "Demonstrates runner async client."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request-async
                         cl
                         {:form-params {:client "runner_async"
                                        :fun "manage.present"}}))))

(defn wheel-async-key-list-all
  "Demonstrates wheel async client."
  []
  (let [cl (example-client)]
    (print-and-close cl (salt/request-async cl
                                            {:form-params {:client "wheel_async"
                                                           :fun "key.list_all"}}))))
(comment
  (local-test-ping)
  (local-async-test-ping)
  (local-async-pkg-show)
  (local-async-in-parallel)
  (local-async-locate)
  (local-async-batch-glob-test-ping)
  (local-async-grains-items)
  (runner-async-manage-present)
  (wheel-async-key-list-all)
  )
