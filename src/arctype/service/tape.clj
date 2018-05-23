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

(defn put!
  [{:keys [queues encoder]} topic data]
  (if-let [queue-file (get queues topic)]
    (let [buf (encoder data)]
      (locking queue-file
        (.add queue-file buf)))
    (throw (ex-info "Can not put to undefined topic"
                    {:topic topic
                     :topics (:keys queues)}))))

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

(defrecord TapeQ [config]
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

  )

(S/defn create
  [resource-name
   config :- Config
   encoder :- S/Any ; (fn [dict] -> bytes) Data encoder)
   topics :- [S/Any]] ; list of keywords or string
  (let [config (merge default-config config)]
    (resource/make-resource
      (map->TapeQ
        {:config config
         :topics topics
         :encoder encoder})
      resource-name)))
