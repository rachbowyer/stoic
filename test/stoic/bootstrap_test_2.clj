(ns stoic.bootstrap-test-2
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [stoic.bootstrap :as bs]
            [stoic.protocols.config-supplier :refer :all]))


(defrecord MockConfigSupplier []
  component/Lifecycle
  (start [this]
    (println "Config supplier request start")
    (if-not (:is-started this)
      (let [watch-fn-atom (atom nil)
            version (atom 1)]
        (println "Not started so starting")
        (assoc this :watch-fn watch-fn-atom :version version :is-started true))
      (do
        (println "Config supplier already started")
        this)))

  (stop [this] this)

  ConfigSupplier
  (fetch [{version :version} k]
    (println "ConfigSupplier - fetch " k)
    (when (= k :mock)
      {:a @version}))

    (watch! [this k watcher-function]
      (println "Watching " k watcher-function)
      (reset! (:watch-fn this) watcher-function)))

(defn bump-config [stoic-config new-version]
  (reset! (:version stoic-config) new-version)
  (@(:watch-fn stoic-config)))


(defrecord MockComponent []
  component/Lifecycle

  (start [this]
    (assert (not (:is-started this)))
    (println "Component started")

    (if-not (:internal-version this)
      (do
        (println "starting with version 1")
        (assoc this :is-started true :internal-version (atom 1) :version 1))
      (let [ref-version (:internal-version this)]
        (reset! ref-version (+ @ref-version 1))
        (println "restarting with version " @ref-version)
        (assoc this :is-started true :version @ref-version))))

  (stop [this]
    (assert (:is-started this))
    (dissoc this :is-started)))


(deftest bounce-components-if-config-changes
  (let [sys (atom (component/system-map
                    :mock (MockComponent.)))
        get-sys-map (fn [] @sys)
        update-sys-map (fn [new-sys-map] (reset! sys new-sys-map))
        old-config-supplier (->MockConfigSupplier)
        {unstarted-sys :system config :config-supplier}  (bs/bootstrap get-sys-map update-sys-map old-config-supplier)
        started-sys (bs/start-safely unstarted-sys)]

    (update-sys-map started-sys)

    (testing "Component injected and started"
      (assert (-> @sys :mock :is-started))
      (assert (= 1 (-> @sys :mock :settings deref :a))))

    (testing "Component picks up config changes (and stop is called on started component)"
      (bump-config config 2)
      (assert (= 2 (-> @sys :mock :settings deref :a))))

    (testing "Restarted component is put back in the map"
      (bump-config config 11)
      (assert (= 3 (-> @sys :mock :version))))

    (component/stop @sys)))




