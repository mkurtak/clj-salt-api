;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.http
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as a]
            [clojure.string :as str]
            [manifold.deferred :as d]
            [manifold.stream :as ms]
            [salt.core :as s]))

(def retriable-status-codes
  "Set pof retriable status codes based on 
  https://en.wikipedia.org/wiki/List_of_HTTP_status_codes"  
  #{408 500 502 503 504 509 520 521 522 523 524 598 599})

(defn- status-code->category
  [{:keys [:status]}]
  (if status
    (cond
      (= 200 status) :ok
      (= 401 status) :unauthorized
      (contains? retriable-status-codes status) :retriable
      :else :error)
    :error))

(defn- parse-json
  [body]
  (json/parse-string (bs/to-string body) true))

(defn response->channel-response
  [resp]
  (let [category (status-code->category resp)]
    (if (= category :ok)
      resp
      (throw (ex-info "Request error."
                      {::s/response resp ::s/response-category category}
                      (when (instance? Throwable resp)
                        resp))))))

(defn- connect
  "Connects deferred created in `create-deferred-fn`  with core.async `resp-chan`"
  [create-deferred-fn resp-chan]
  (try (d/on-realized (d/catch
                          (create-deferred-fn)
                          identity)
                      (fn [r]
                        (a/>!! resp-chan (if (nil? r) {} r))
                        (a/close! resp-chan))
                      (fn [r]
                        (a/>!! resp-chan (if (nil? r) {} r))
                        (a/close! resp-chan)))
       resp-chan
       ;; Exception is only thrown from create-deferred-fn (from calling thread).
       ;; It must be put on promise/chan asynchronously to prevent deadlock.
       (catch Exception e
         (a/put! resp-chan e)
         (a/close! resp-chan)
         resp-chan)))

(defn request
  "Invoke `aleph.http/request` and transform manifold deferred to core.async channel.
  Return new channel delivering response:
  
  * Ring response if ok
  * Exception if error occurs"
  [req]
  (let [resp-chan (a/promise-chan)]
    (connect #(-> (http/request (merge req {:throw-exceptions? false}))
                  (d/chain response->channel-response))
             resp-chan)))

(def empty-line-pattern
  "New line followed by whitespace chars ending with new line."
  #"\r?\n[ \t\\x0B\f\r]?\n")

(def last-empty-line-pattern
  "Empty line not followed by another empty line.
  This regex uses negative lookahead and DOTALL mode."
  (re-pattern (str "(?s)" empty-line-pattern "(?!.*" empty-line-pattern ")")))

(def sse-supported-attrs #{:event :data :id})

(defn- line->field
  [line]
  (let [splits (str/split line #":" 2)
        attr   (first splits)
        value  (second splits)]
    [(keyword attr)
     (if (nil? value) "" value)]))

(defn- sse-supported-field?
  [[attr val]]
  (or
   (contains? sse-supported-attrs attr)
   (and (= :retry attr)
        (every? #(Character/isDigit %) (str/trim val)))))

(defn- reduce-sse-fields
  "Join values of field with same attributes with newline."
  [fields]
  (assoc (apply merge-with
                #(str/join "\n" [%1 %2])
                (map #(into {} [%]) fields))
         :type :data))

(defn- reduce-retry-fields
  "Leave only last retry field."
  [fields]
  {:type :retry
   :retry (Short/parseShort (str/trim (second (last fields))))})

(defn- not-sse-comment?
  [field]
  (not= ':' (first field)))

(defn- retry-field?
  [field]
  (= :retry (first field)))

(defn sse-buffer->events
  [buf prev-buf]
  (let [buf-splits (str/split (str prev-buf buf) last-empty-line-pattern 2)
        no-splits (= 1 (count buf-splits))
        chunks (if no-splits nil (str/split (first buf-splits) empty-line-pattern)) 
        next-prev (if no-splits (first buf-splits) (second buf-splits))]
    [(->> chunks
          (mapcat (fn [chunk]
                    (->> chunk
                         (str/split-lines)
                         (filter not-sse-comment?)
                         (map line->field)
                         (filter sse-supported-field?)
                         (group-by retry-field?)
                         (map (fn [group]
                                (let [[retry? fields] group]
                                  (if retry?
                                    (reduce-retry-fields fields)
                                    (reduce-sse-fields fields)))))))))
     next-prev]))

(defn mapcat-with-previous
  [f]
  (comp
   (fn [rf]
     (let [pv (volatile! nil)]
       (fn
         ([] (rf))
         ([result] (rf result))         ; todo what to do with remaining pv?
         ([result input]
          (let [[items pval] (f input @pv)]
            (vreset! pv pval)
            (rf result items))))))
   cat))

(defn- sse-pool
  [{:keys [:connection-options] :as opts}]
  (http/connection-pool (merge opts
                               {:total-connections 2
                                :max-queue-size 1
                                :target-utilization 1
                                :connection-options (merge
                                                     connection-options
                                                     {:raw-stream? true})})))

(defn sse
  "Invoke [[aleph.http/request]] on SSE endpoint and stream response to core.async channel.

  Uses pool defined in `sse-pool` -> with 1 connection and raw-stream? option.
  Pool could be customized with `pool-opts` (see [[aleph.http/request]] documentation).

  Server-sent events is a stream of text lines separated by empty lines.
  
  Returns new channel delivering server-sent events with following types
  | Type       | Attributes | Description
  | -----------| -----------|
  | `:connect` | :stream    | Connection is established. Use :stream to close SSE. 
  | `:data`    | :id :data  | One SSE event. When event contains multiple data attributes, they are concatenated with newline character.
  | `:retry`   | :retry     | SSE indicates to set retry-timeout.
  | `:close`   |            | Sent before stream and respective core.async channel is closed.
  
  If SSE request could not be made, exception is written to the channel and channel is closed."
  ([req] (sse req {}))
  ([req pool-opts]
   (let [resp-chan (a/chan 10)]         ; todo how to set buffer size?
     (connect (fn [] (-> (http/request (merge req {:throw-exceptions? false
                                                   :pool (sse-pool pool-opts)}))
                         (d/chain response->channel-response
                                  :body
                                  #(do (a/>!! resp-chan {:type :connect
                                                         :stream %})
                                       %)
                                  #(ms/map bs/to-string %)
                                  #(ms/filter some? %)
                                  #(ms/transform (mapcat-with-previous
                                                  sse-buffer->events)
                                                 %)
                                  #(ms/map (fn [x] (a/>!! resp-chan x) x) %)
                                  #(ms/reduce (fn [r _] r) {:type :close} %))))
              resp-chan))))

(defn close-sse
  "Close manifold stream if any."
  [{:keys [:stream]}]
  (when stream
    (ms/close! stream)))

(defn parse-body
  "Parse body from `response` using content-type header to determine format.
  
   If content-type is missing or unsupported, or parsing error occurs, throw ex with response in data.
   If body is not present, return nil.
   If body is successfully parsed, return map and full response in meta."
  [response]
  (if-let [body (:body response)]
    (if-let [content-type (get-in response [:headers "content-type"])]
      (cond
        (= "application/json" content-type) (with-meta
                                              (parse-json body)
                                              {::s/presponse response})
        (str/starts-with? content-type "text/") (with-meta
                                                  [(bs/to-string body)]
                                                  {::s/response response})
        :else (throw
               (ex-info "Unsupported content-type" {::s/response response
                                                    :content-type content-type})))
      (throw (ex-info "No content-type." {::s/response response})))))

(defn parse-sse
  "Parse JSON from `event` if event is type of :data"
  [event]
  (if (= :data (:type event))
    {:type :data
     :data (json/parse-string (:data event) true)}
    event))
