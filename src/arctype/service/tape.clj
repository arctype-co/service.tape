(ns ^{:doc "Message I/O using Kafka"}
  arctype.service.kafka
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
  {:queues-path S/Str ; directory name for queue files
   (S/optional-key :write-buffer-size) S/Int ; Size of write buffer (in objects, not bytes)
   :topics [S/Str]})

(defn put!
  [this topic data]
  
  )

(defn- open-queue
  [this queue-name]
  (let [file (io/file (str (:queues-path (:config this)) "/" queue-name ".tape"))]
    (.build (QueueFile$Builder. file))))

(defn- open-queues
  [this]
  (->> (:queue-names (:config this))
       (map (fn [queue-name]
              [(keyword queue-name) (open-queue this queue-name)]))
       (into {})))

(defn- close-queues
  [queues]
  (doseq [[queue-id queue-file] queues]
    (.close queue-file)))

(defrecord TapeQ [config]
  PLifecycle

  (start [this]
    (log/info "Starting Tape service")
    (-> this
        (assoc :queues (open-queues this))))

  (stop [this]
    (log/info "Stopping Tape service")
    (-> this
        (update :queues close-queues)))

  PEventProducer
  (put-event! [this topic data]
    (put! this topic data))

  )

(S/defn create
  [resource-name
   config :- Config
   encoder :- S/Any] ; (fn [dict] -> bytes) Data encoder)
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->TapeQ
        {:config config
         :encoder encoder})
      resource-name)))
