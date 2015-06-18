(ns stoic.config.curator
  (:require [curator.framework :refer (curator-framework)]
            [curator.discovery :refer (service-discovery service-instance service-provider instance instances services note-error)]
            [stoic.protocols.config-supplier]
            [stoic.config.data :as data]
            [stoic.config.env :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [stoic.config.exhibitor :refer :all])
  (:import [org.apache.curator.framework.api
            CuratorWatcher CuratorEventType]))

(defn connect
  ([] (connect (or (and (exhibitor-host) (exhibitor-framework))
                   (curator-framework (zk-ips)))))
  ([client]
     (.start client)
     client))

(defn close [client]
  (.close client))

(defn add-to-zk [client path m]
  (when-not (.. client checkExists (forPath path))
    (.. client create (forPath path nil)))
  (.. client setData (forPath path (data/serialize-form m))))

(defn read-from-zk [client path]
  (data/deserialize-form (.. client getData (forPath path))))

(defn- watch-path [client path watcher]
  (.. client checkExists watched (usingWatcher watcher)
      (forPath path)))

(defrecord CuratorConfigSupplier [root path-for]
  stoic.protocols.config-supplier/ConfigSupplier
  component/Lifecycle

  (start [{:keys [client] :as this}]
    (if client this (do
                      (log/info "Connecting to ZK")
                      (assoc this :client (connect)))))

  (stop [{:keys [client] :as this}]
    (when client
      (log/info "Disconnecting from ZK")
      (close client))
    (dissoc this :client))

  (fetch [{:keys [client]} k]
    (let [path (path-for root k)
          fetch-for-path (fn [p]
                           (when-not (.. client checkExists (forPath p))
                             (.. client create (forPath p nil)))
                           (read-from-zk client p))]
      (if (sequential? path)
        (apply merge (map fetch-for-path path))
        (fetch-for-path path))))

  (watch! [{:keys [client]} k watcher-fn]
    (let [path (path-for root k)
          watch-one-path (fn [p]
                           (watch-path client p
                                       (reify CuratorWatcher
                                         (process [this event]
                                           (when (= :NodeDataChanged (keyword (.. event getType name)))
                                             (log/info "Data changed, firing watcher" event)
                                             (watcher-fn)
                                             (watch-path client p this))))))]
      (if (sequential? path)
        (apply merge (map watch-one-path path))
        (watch-one-path path)))))

(defn config-supplier
  ([path-for]
   (CuratorConfigSupplier. (zk-root) path-for))
  ([]
   (config-supplier data/path-for))
  )
