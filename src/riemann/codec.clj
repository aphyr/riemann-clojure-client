(ns riemann.codec
  "Encodes and decodes Riemann messages and events, between byte arrays,
  buffers, and in-memory types."
  (:require clojure.set)
  (:import [io.riemann.riemann Proto$Query Proto$Attribute Proto$Event Proto$Msg]
           [java.net InetAddress]
           [com.google.protobuf ByteString]))

(defrecord Query [string])
(defrecord Event [host service state description metric tags time ttl])
(defrecord Msg [ok error events query decode-time])

(def event-keys (set (map keyword (Event/getBasis))))

(defn decode-pb-query
  "Transforms a java protobuf Query to a Query."
  [^Proto$Query q]
  (Query. (.getString q)))

(defn encode-pb-query
  "Transforms a Query to a protobuf Query."
  (^Proto$Query [query]
     (-> (Proto$Query/newBuilder)
       (.setString (:string query))
       (.build))))

(defn decode-pb-event-record
  "Transforms a java protobuf to an Event."
  [^Proto$Event e]
  (Event.
    (when (.hasHost e)        (.getHost e))
    (when (.hasService e)     (.getService e))
    (when (.hasState e)       (.getState e))
    (when (.hasDescription e) (.getDescription e))
    (cond
      (.hasMetricSint64 e)    (.getMetricSint64 e)
      (.hasMetricD e)         (.getMetricD e)
      (.hasMetricF e)         (.getMetricF e))
    (when (< 0 (.getTagsCount e)) (vec (.getTagsList e)))
    (cond
      (.hasTimeMicros e) (/ (double (.getTimeMicros e)) 1000000) ;; time in us => s
      (.hasTime e)       (.getTime e) ;; time in s
      true               (/ (double (System/currentTimeMillis))
                            1000)) ;; default
    (when (.hasTtl e)         (.getTtl e))))

(def blacklisted-attributes #{ "tags" "time" "ttl" "metric"})

(defn blacklisted?
  [^Proto$Attribute a]
  (blacklisted-attributes (.getKey a)))

(defn decode-pb-event
  [^Proto$Event e]
  (let [event (decode-pb-event-record e)]
    (if (< 0 (.getAttributesCount e))
      (into event (map (fn [^Proto$Attribute a]
                         [(keyword (.getKey a)) (.getValue a)])
                       (remove blacklisted? (.getAttributesList e))))
      event)))

(defn ^Proto$Event encode-pb-event
  "Transforms a map to a java protobuf Event."
  [e]
  (let [event (Proto$Event/newBuilder)]
    (when (:host e)         (.setHost         event (:host e)))
    (when (:service e)      (.setService      event (:service e)))
    (when (:state e)        (.setState        event (:state e)))
    (when (:description e)  (.setDescription  event (:description e)))
    (when-let [m (:metric e)]
      ; For backwards compatibility, always construct floats.
      (try (.setMetricF event (float m)) (catch Throwable _))
      (if (and (integer? m) (<= Long/MIN_VALUE m Long/MAX_VALUE))
        (.setMetricSint64 event (long m))
        (.setMetricD event (double m))))
    (when (:tags e)         (.addAllTags      event (:tags e)))
    (when (:time e) (do (.setTimeMicros event (long (* 1000000 (:time e))))
                        (.setTime       event (long (:time e)))))
    (when (:ttl e)          (.setTtl          event (:ttl e)))
    (doseq [k (clojure.set/difference (set (keys e)) event-keys)]
      (when-some [v (get e k)]
        (let [a (Proto$Attribute/newBuilder)]
          (.setKey a (str (when (namespace k) (str (namespace k) \/)) (name k)))
          (.setValue a (str v))
          (.addAttributes event a))))
    (.build event)))

(defn encode-client-pb-event
  "Clients usually fill in an event's local host and time automatically, if
  not explicitly specified."
  [e]
  (let [absent? (-> e keys set complement)]
    (encode-pb-event
     (cond-> e
       (absent? :host) (assoc :host (.. InetAddress getLocalHost getHostName))
       (absent? :time) (assoc :time (/ (double (System/currentTimeMillis)) 1000))))))

(defn decode-pb-msg
  "Transforms a java protobuf Msg to a defrecord Msg."
  [^Proto$Msg m]
  (let [t (System/nanoTime)]
    (Msg. (when (.hasOk m) (.getOk m))
          (when (.hasError m) (.getError m))
          (mapv decode-pb-event (.getEventsList m))
          (when (.hasQuery m) (decode-pb-query (.getQuery m)))
          t)))

(defn ^Proto$Msg encode-pb-msg
  "Transform a Protobuf Msg or a Clojure map to a java Protobuf Msg."
  [m]
  (if (instance? Proto$Msg m)
    m
    (let [msg (Proto$Msg/newBuilder)]
      (when-let [events (:events m)]
        (doseq [e events]
          (.addEvents msg (encode-pb-event e))))
      (when-not (nil? (:ok m)) (.setOk msg (:ok m)))
      (when (:error m) (.setError msg (:error m)))
      (when (:query m) (.setQuery msg (encode-pb-query (:query m))))
      (.build msg))))
