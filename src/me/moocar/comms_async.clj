(ns me.moocar.comms-async
  (:require [clojure.core.async :as async :refer [go <! go-loop]])
  (:import (java.nio ByteBuffer)))

(def ^:const request-flag
  "Byte flag placed at the beginning of a packet to indicate the next
  8 bytes are the request-id and that the sender of the packet expects
  to receive a response (with the response flag)"
  (byte 1))

(def ^:const response-flag
  "Byte flag placed at the begninning of a packet to indicate that
  this is a response packet for the request-id in the next 8 bytes"
  (byte 0))

(def ^:const no-request-flag
  "Byte flag placed at the beginning of a packet to indicate that this
  is a request that does not expect a response and therefore the
  request-id is not present (data begins at position 1)"
  (byte -1))

(def ^:const packet-type-bytes-length
  "Number of bytes taken up by the packet-type flag"
  1)

(def ^:const request-id-bytes-length
  "Number of bytes taken up by the request ID"
  8)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## Custom transforms

(defn custom-request
  "Returns a function that takes a request and assoc's the result
  of (from-bytes (:body-bytes request)) back onto the request
  as :body"
  [from-bytes]
  (fn [request]
    (let [[bytes offset len] (:body-bytes request)]
      (assoc request
             :body (from-bytes bytes offset len)))))

(defn custom-response
  "Returns a function that takes a request, and returns it with the
  result of (to-bytes (:response request)) assoced onto the request
  as :response-bytes"
  [to-bytes]
  (fn [request]
    (if-let [response (:response request)]
      (let [response-bytes (to-bytes response)]
        (assoc request
               :response-bytes [response-bytes 0 (alength response-bytes)]))
      request)))

(defn custom-send
  "Returns a function that takes send-ch args [[request response-ch]]
  and returns a new set of args where request is converted to bytes
  using from-bytes, and response-ch is a transduced channel that uses
  to-bytes to convert eventual responses back to bytes"
  [from-bytes to-bytes]
  (fn [[request response-ch]]
    [(to-bytes request)
     (when response-ch
       (let [bytes->ch (async/chan 1 (map (custom-request from-bytes)))]
         (async/pipe bytes->ch response-ch)
         bytes->ch))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ## Sending/Receiving

(defn- add-response-ch
  "Adds a response-ch for the request id. After 10 seconds, closes the
  ch and removes it from the set"
  [response-chans-atom request-id response-ch]
  (swap! response-chans-atom assoc request-id response-ch)
  (async/take! (async/timeout 10000)
               (fn [_]
                 (async/close! response-ch)
                 (swap! response-chans-atom dissoc request-id))))

(defn- request-buf
  "Returns a function that takes a vector of bytes and response-ch,
  and returns a byte buffer that contains the packet-type, request-id
  and body bytes"
  [request-id-seq-atom response-chans-atom]
  (fn [[^bytes bytes response-ch]]
    (if response-ch
      (let [request-id (swap! request-id-seq-atom inc)
            body-size (alength bytes)
            buffer-size (+ packet-type-bytes-length
                           request-id-bytes-length
                           body-size)
            buf (.. (ByteBuffer/allocate buffer-size)
                    (put request-flag)
                    (putLong request-id)
                    (put bytes)
                    (rewind))]
        (add-response-ch response-chans-atom request-id response-ch)
        buf)
      (let [body-size (alength bytes)
            buffer-size (+ packet-type-bytes-length body-size)
            buf (.. (ByteBuffer/allocate buffer-size)
                    (put no-request-flag)
                    (put bytes)
                    (rewind))]
        buf))))

(defn- response-buf
  "Extracts response-bytes out of request and converts into a
  ByteBuffer in the form of

  [response-flag request-id & bytes]
        ^             ^         ^
        |             |         |
     1 byte      8 byte long   rest

  If for some reason there is no `:response-bytes` in the request,
  returns nil"
  [{:keys [response-bytes request-id] :as request}]
  (when response-bytes
    (let [[bytes offset len] response-bytes
          buf-size (+ packet-type-bytes-length
                      request-id-bytes-length
                      len)
          buf (.. (ByteBuffer/allocate buf-size)
                  (put response-flag)
                  (putLong request-id)
                  (put bytes offset len)
                  (rewind))]
      buf)))

(defn- handle-read
  "Handles new bytes coming in off connection. An incoming packet can
  be one of 3 types (denoted by first byte):

  - request: [request-id(8 byte long) body-bytes(rest of bytes)]. A
  new request coming into this connection. Therefore the next 8 bytes
  are the request-id. New requests are put onto request-ch

  - response: [request-id(8 byte long) body-bytes(rest of bytes)]. A
  response to a previous request that was sent. The next 8 bytes are
  the request-id for the original outgoing request. Responses are put
  onto the response-ch for the request-id

  - no-response: [body-bytes(rest of bytes)]. A request coming into
  this connection that does NOT expect a response. Therefore there is
  no request-id, and the body takes up the rest of the bytes. New
  requests are put onto request-ch

  In all the above scenarios, the item put onto the channel is a map
  of :conn :body-bytes ([bytes offset len]) and a request-id for
  request or response packets"
  [request-ch conn buf]
  (let [{:keys [response-chans-atom]} conn
        packet-type (.get buf)
        request-id (when-not (= no-request-flag packet-type)
                     (.getLong buf))
        body-bytes [(.array buf) 
                    (+ (.arrayOffset buf) (.position buf))
                    (.remaining buf)]
        to-ch (if (= response-flag packet-type)
                (get @response-chans-atom request-id)
                request-ch)
        request (cond-> {:conn conn
                         :body-bytes body-bytes}
                        (= packet-type request-flag)
                        (assoc :request-id request-id))]
    (async/put! to-ch request)))

(defn read-handler [request-ch conn]
  (fn [buf] (handle-read request-ch conn buf)))

(defn basic-connection-map
  ([] (basic-connection-map (map identity)))
  ([send-xf]
   (let [request-id-seq-atom (atom 0)
         response-chans-atom (atom {})]
     {:send-ch (async/chan 100 (comp send-xf
                                   (map (request-buf request-id-seq-atom response-chans-atom))))
      :read-ch (async/chan 100)
      :write-ch (async/chan 100)
      :error-ch (async/chan 100 (map (fn [t] (.printStackTrace t))))
      :response-chans-atom response-chans-atom
      :request-id-seq-atom request-id-seq-atom})))

(defn connect [server client]
  (let [server-conn (basic-connection-map)]
    (async/pipe (:write-ch (:conn client)) (:read-ch server-conn))
    (async/pipe (:write-ch server-conn) (:read-ch (:conn client)))
    (go-loop []
      (when-let [buf (<! (:read-ch server-conn))]
        (handle-read (:request-ch server) server-conn buf)
        (recur)))))

(defn- send-to-write-ch
  "When request contains a :response-bytes, converts them into a
  ByteBuffer and puts onto the requets's connection's write-ch"
  [request]
  (let [{:keys [conn]} request
        {:keys [write-ch]} conn]
    (when-let [buf (response-buf request)]
      (async/put! write-ch buf))
    nil))

(defn listen-for-requests
  "Starts a pipeilne that listens for requests on request-ch and uses
  handler-xf to handle the request. handler-xf should be a transducer
  that returns performs any required operations and then returns the
  request object, possible with :response-bytes if a response should
  be sent back to the other side of the connection"
  [request-ch handler-xf]
  ;; to-ch is a /dev/null
  (let [to-ch (async/chan 1 (keep (constantly nil)))]
    (async/pipeline-blocking 1
                             to-ch
                             (comp handler-xf
                                   (keep send-to-write-ch))
                             request-ch)
    to-ch))

(defn read-loop [request-ch conn]
  (go-loop []
    (when-let [buf (<! (:read-ch conn))]
      (try
        (handle-read request-ch conn buf)
        (catch Throwable t
          (async/put! (:error-ch conn) t)))
      (recur))))

(defn start-inline-server
  [{:keys [new-conn-f handler-xf] :as server}]
  (let [request-ch (async/chan 1024)
        new-conn-f (or new-conn-f (constantly #(basic-connection-map)))
        request-listener (listen-for-requests request-ch handler-xf)]
    (assoc server
           :request-ch request-ch
           :request-listener request-listener)))

(defn new-inline-server
  [{:keys [new-conn-f handler-xf] :as server}]
  {:pre [handler-xf]}
  server)

(defn new-inline-client
  [config]
  {:pre [(:server config)]}
  config)

(defn start-inline-client
  [{:keys [request-ch server] :as this}]
  (let [conn (basic-connection-map)
        this (assoc this
                    :conn conn)]
    (read-loop request-ch conn)
    (async/pipe (:send-ch conn) (:write-ch conn))
    (connect server this)
    this))
