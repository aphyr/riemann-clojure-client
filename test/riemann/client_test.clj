(ns riemann.client-test
  (:import java.net.InetAddress
           io.riemann.riemann.client.OverloadedException)
  (:use riemann.client
        clojure.test))

(deftest async-test
  (with-open [c (tcp-client :host "localhost")]
    (let [e {:service "async-test" :foo "hi there"}]
      (let [res (send-event c e)]
        (is (= true (:ok @res)))
        ; Confirm receipt
        (is (-> c
                (query "service = \"async-test\"")
                deref
                first
                :foo
                (= "hi there")))))))

(deftest send-query
  (with-open [c (tcp-client :host "localhost")]
    (let [e {:service "test" :state "ok" :ttl 10}]
      (send-event c e)
      (let [e1 (first @(query c "service = \"test\" and state = \"ok\""))]
        (is (= (:service e) (:service e1)))
        (is (= (:state e) (:state e1)))
        (is (float? (:ttl e1)))
        (is (float? (:time e1)))))))

(deftest load-shedding-test
  (with-open [c (tcp-client :auto-connect false)]
    (.. c transport (setWriteBufferLimit 2))
    (connect! c)

    (let [e {:service "overload-test"
             :ttl 10
             :description (apply str (repeat 100 "x"))}
          results (->> (repeat 10000 e)
                       (pmap (partial send-event c))
                       doall)
          outcomes (->> results
                        (mapv (fn [p]
                                (try (deref p 0 :not-ready)
                                     (catch OverloadedException e
                                       :overloaded)))))]
      ; Some overloaded
      (is (not-empty (filter #{:overloaded} outcomes)))
      ; Some OK
      (is (not-empty (remove #{:overloaded :not-ready} outcomes))))))

(deftest default-time
  (with-open [c (tcp-client :host "localhost")]
    (testing "undefined time"
      (let [t1 (/ (System/currentTimeMillis) 1000)
            _ @(send-event c {:service "test-no-time"})
            t2 (/ (System/currentTimeMillis) 1000)
            t  (-> c
                   (query "service = \"test-no-time\"")
                   deref
                   first
                   :time)]
        (is (<= t1 t t2))))

    (testing "with an explicitly nil time"
      @(send-event c {:service "test-nil-time" :time nil})
      ; Server should fill it in
      (is (number? (-> c (query "service = \"test-nil-time\"")
                       deref
                       first
                       :time))))

    (testing "with a given time"
      @(send-event c {:service "test-given-time" :time 1234})
      (is (= 1234.0
             (-> c (query "service = \"test-given-time\"")
                 deref
                 first
                 :time))))))

(deftest default-host
  (with-open [c (tcp-client :host "localhost")]
    (testing "undefined host"
      @(send-event c {:service "test-no-host" :state "ok"})
      (is (= (.. InetAddress getLocalHost getHostName)
             (-> c (query "service = \"test-no-host\"")
                 deref
                 first
                 :host))))

    (testing "with an explicitly nil host"
      @(send-event c {:service "test-nil-host" :host nil})
      (is (nil? (-> c (query "service = \"test-nil-host\"")
                    deref
                    first
                    :host))))

    (testing "with a given host"
      @(send-event c {:service "test-given-host" :host "foo"})
      (is (= "foo"
             (-> c (query "service = \"test-given-host\"")
                 deref
                 first
                 :host))))))
