(ns ^{:doc "File queue using Tape (by Square)"}
  arctype.service.tape
  (:refer-clojure :exclude [key])
  (:import
    [com.squareup.tape2 QueueFile QueueFile$Builder])
  (:require
    [clojure.core.async :as async]
    [clojure.tools.logging :as log]
    [clojure.java.io :as io]
    [schema.core :as S]
    [sundbry.resource :as resource]
    [arctype.service.util :refer [<?? map-vals thread-try]]
    [arctype.service.protocol :refer :all]))

(def Config
  {:path S/Str ; directory name for queue files
   (S/optional-key :write-buffer-size) S/Int ; Size of write buffer (in objects, not bytes)
   })

(def default-config
  {:write-buffer-size 32})

(defn- undefined-queue
  [topic queues]
  (ex-info "Undefined topic"
           {:topic topic
            :topics (:keys queues)}))

(defn put!
  [{:keys [queues encoder]} topic data]
  (if-let [queue-file (get queues topic)]
    (let [buf (encoder data)]
      (locking queue-file
        (.add queue-file buf)
        (.notify queue-file)))
    (throw (undefined-queue topic queues))))

(defn- open-queue
  [this queue-name]
  (let [file (io/file (str (:path (:config this)) "/" queue-name ".tape"))]
    (.build (QueueFile$Builder. file))))

(defn- open-queues
  [this]
  (->> (:topics this)
       (map (fn [topic]
              [topic (open-queue this (name topic))]))
       (into {})))

(defn- close-queues
  [queues]
  (doseq [[queue-id queue-file] queues]
    (.close queue-file)))

(defn- mkdir!
  [path]
  (let [dir (io/file path)]
    (if (.exists dir)
      (when-not (.isDirectory dir)
        (throw (ex-info "Tape path is not a directory"
                        {:path path})))
      (when-not (.mkdirs dir)
        (throw (ex-info "Failed to make tape directory"
                        {:path path}))))))

(defrecord TapeQ [config queues decoder]
  PLifecycle

  (start [this]
    (log/info "Starting Tape service")
    (mkdir! (:path config))
    (-> this
        (assoc :queues (open-queues this))))

  (stop [this]
    (log/info "Stopping Tape service")
    (-> this
        (update :queues close-queues)))

  PEventProducer
  (put-event! [this topic data]
    (put! this topic data))

  PEventConsumer
  (start-event-consumer [this topic options handler]
    (if-let [queue (get queues topic)]
      (let [continue? (atom true)
            stopped? (atom false)
            thread (Thread.
                     (fn []
                       (while @continue?
                         (try
                           (if-let [next-bytes (locking queue (.peek queue))]
                             (let [data (decoder next-bytes)]
                               (try
                                 (handler data)
                                 (catch Exception e
                                   (log/error e {:message "Event handler failed"})))
                               (locking queue (.remove queue)))
                             (locking queue (.wait queue)))
                           (catch Exception e
                             (log/fatal e {:message "QueueFile io/error"})
                             (throw e))))
                       (reset! stopped? true)))]
        (.start thread)
        {:thread thread
         :continue? continue?
         :stopped? stopped?
         :queue queue
         :options options})
      (throw (undefined-queue topic queues))))

  (stop-event-consumer [this {:keys [thread continue? stopped? queue options]}]
    (reset! continue? false)
    (locking queue (.notify queue))
    (.join thread (:stop-timeout-ms options))
    (when-not @stopped?
      (log/warn {:message "Event consumer stop timeout"
                 :stop-timeout-ms (:stop-timeout-ms options)})))

  )

(S/defn create
  [resource-name
   config :- Config
   encoder :- S/Any ; (fn [obj] -> bytes) Data encoder)
   decoder :- S/Any ; (fn [bytes] -> obj)
   topics :- [S/Any]] ; list of keywords or string
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->TapeQ
        {:config config
         :topics topics
         :encoder encoder
         :decoder decoder})
      resource-name)))
