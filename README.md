[![CircleCI](https://circleci.com/gh/mkurtak/clj-salt-api.svg?style=shield)](https://circleci.com/gh/mkurtak/clj-salt-api)
[![codecov](https://codecov.io/gh/mkurtak/clj-salt-api/branch/master/graph/badge.svg)](https://codecov.io/gh/mkurtak/clj-salt-api)
# clj-salt-api

[salt-api](http://docs.saltstack.com/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html#module-salt.netapi.rest_cherrypy.app) client library for Clojure.

## Rationale

clj-salt-api is data oriented Clojure library for invoking salt-api. You only need three functions at runtime: `client`, which creates clj-salt-api client, `request`, which invokes sync operation on the client and `request-async` which invokes async operation on the client. Both request methods take a ring request as a parameter and return a core.async channel that delivers the response/responses.

clj-salt-api handles authentication and implements [best practices for interacting with salt-api](https://docs.saltstack.com/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html#best-practices) as defined in salt.netapi modules specification:

> Given the performance overhead and HTTP timeouts for long-running operations described above, the most effective and most scalable way to use both Salt and salt-api is to run commands asynchronously using the `local_async `, `runner_async`, and `wheel_async` clients.

> Running asynchronous jobs results in being able to process 3x more commands per second for LocalClient and 17x more commands per second for RunnerClient, in addition to much less network traffic and memory requirements. Job returns can be fetched from Salt's job cache via the /jobs/<jid> endpoint, or they can be collected into a data store using Salt's Returner system.

> The /events endpoint is specifically designed to handle long-running HTTP connections and it exposes Salt's event bus which includes job returns. Watching this endpoint first, then executing asynchronous Salt commands second, is the most lightweight and scalable way to use rest_cherrypy while still receiving job returns in real-time. But this requires clients that can properly handle the inherent asynchronicity of that workflow.

clj-salt-api manages single HTTP connection to /events endpoint and sends salt events to respective async requests. Response from each minion is delivered as separate value in core.async channel. clj-salt-api does all error handling and reconnections under the hood.

## Usage

```clojure
(require '[salt.client :as salt]
         '[salt.core :as s]
         '[clojure.core.async :as a])

;; Create a client
(def client (salt/client {::s/master-url "http://localhost:8000"
                          ::s/username "saltapi"
                          ::s/password "saltapi"
                          ::s/max-sse-retries 3
                          ::s/sse-keep-alive? true}))

;; Execute async request and collect responses from all minions in one vector
(a/<!! (a/into [] (salt/request-async client {:form-params {:client "local_async"
                                                            :tgt "*"
                                                            :fun "pkg.version"
                                                            :arg "vim"}})))

;; Execute sync request
(a/<!! (salt/request client {:form-params {:client "local"
                                           :tgt "*"
                                           :fun "test.ping"}}))

;; Close client
(salt/close client)
```

## Copyright and License

Copyright © 2020 Michal Kurťák

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
