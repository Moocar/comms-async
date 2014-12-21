(ns me.moocar.comms-async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!!]]
            [me.moocar.comms-async :as comms])
  (:import (java.util Arrays)))

(defn send-request
  [send-ch request-bytes]
  (let [response-ch (async/chan 1)]
    (async/put! send-ch [request-bytes response-ch])
    response-ch))

(defn echo-handler []
  (keep (fn [{:keys [request-id body-bytes] :as request}]
          (when request-id
            (assoc request :response-bytes body-bytes)))))

(defn start-server [config handler-xf]
  (comms/start-inline-server
   (comms/new-inline-server (assoc config
                                   :handler-xf handler-xf))))

(defn start-client [config server]
  (comms/start-inline-client
   (comms/new-inline-client (assoc config :server server))))

(defn to-bytes [[bytes offset len]]
  (Arrays/copyOfRange bytes offset (+ offset len)))

(deftest start-stop-test
  (let [handler-xf (echo-handler)
        server (start-server {} handler-xf)]
    (try
      (let [client (start-client {} server)
            send-ch (:send-ch (:conn client))
            request (byte-array (map byte [1 2 3 4]))]
        (try
          (let [response (<!! (send-request send-ch request))]
            (is (= (seq request) (seq (to-bytes (:body-bytes response))))))
          (async/put! send-ch [request]))))))
