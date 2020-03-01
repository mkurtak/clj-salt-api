;; Copyright (c) Michal Kurťák
;; All rights reserved.
(ns salt.test-utils)

(defn- expand-let-flow
  [name forms]
  (let [prev (gensym)]
    (loop [x '()
           frms (reverse (partition 2 forms))]
      (if frms
        (let [form (first frms)
              expr (second form)
              test-expr (first form)
              threaded `(let [~prev ~name
                              ~name (~(first expr) ~prev ~@(next expr))]
                          ~test-expr
                          ~@x)]
          (recur (list threaded)
                 (next frms)))
        x))))

(defmacro test-flow->
  [expr name & forms]
  `(let [~name ~expr]
     ~@(expand-let-flow name forms)))

(defn submap?
  "Is m1 a subset of m2?"
  [m1 m2]
  (if (and (map? m1) (map? m2))
    (every? (fn [[k v]] (and (contains? m2 k)
                             (submap? v (get m2 k))))
            m1)
    (if (and (coll? m1) (coll? m2))
      (every? (fn [v] (some #(submap? v %) m2))
              m1)
      (= m1 m2))
    ))

(comment
  (submap? {:command :send
            :conf {:val1 "val1"}
            :body [{:minion "m1"
                    :success false}]}
           {:command :send
            :test "test1"
            :conf {:val1 "val1"
                   :val2 "val2"}
            :body [{:minion "m1"
                    :more "Test"
                    :success false}]
            })
  )
