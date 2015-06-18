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
      (let [watch-fn-atom (atom '())
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
    (condp = k
      :mock {:a @version}
      :b {}
      :c {}))

    (watch! [this k watcher-function]
      (println "Watching " k watcher-function)
      (swap! (:watch-fn this) (fn [e] (cons watcher-function e)))))

(defn bump-config [stoic-config new-version]
  (reset! (:version stoic-config) new-version)
  (doseq [e @(:watch-fn stoic-config)] (e)))



(defrecord MockComponent [key log-start log-end]
  component/Lifecycle

  (start [this]
    (if log-start (swap! log-start (fn [e] (cons key e))))

    (is (not (:is-started this)))
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
    (is (:is-started this))
    (if log-end (swap! log-end (fn [e] (cons key e))))
    (dissoc this :is-started)))


(deftest bounce-components-if-config-changes
  (let [sys (atom (component/system-map
                    :mock (MockComponent. :mock nil nil)
                    :b (component/using (MockComponent. :b nil nil) [:mock])
                    :c (MockComponent. :c nil nil)))
        get-sys-map (fn [] @sys)
        update-sys-map (fn [new-sys-map] (reset! sys new-sys-map))
        old-config-supplier (->MockConfigSupplier)
        {unstarted-sys :system config :config-supplier}  (bs/bootstrap get-sys-map update-sys-map old-config-supplier)
        started-sys (bs/start-safely unstarted-sys)]

    (update-sys-map started-sys)

    (testing "Component injected and started"
      (is (-> @sys :mock :is-started))
      (is (= 1 (-> @sys :mock :settings deref :a))))

    (testing "Component picks up config changes (and stop is called on started component)"
      (bump-config config 2)
      (is (= 2 (-> @sys :mock :settings deref :a)))
      (is (= 2 (-> @sys :b :internal-version deref)))
      (is (= 1 (-> @sys :c :internal-version deref))))

    (testing "Restarted component is put back in the map"
      (bump-config config 11)
      (is (= 3 (-> @sys :mock :version))))

    (component/stop @sys)))


(deftest test-transitive-dependents
  (let [log-start (atom '())
        log-stop (atom '())
        sys-map (component/system-map
                  :a (component/using (MockComponent. :a log-start log-stop) [:b :c])
                  :b (MockComponent. :b log-start log-stop)
                  :c (component/using (MockComponent. :c log-start log-stop)  [:d])
                  :d (MockComponent. :d log-start log-stop)
                  :e (component/using (MockComponent. :e log-start log-stop)  [:d :f])
                  :f (MockComponent. :f log-start log-stop))

        system-started (stoic.bootstrap/start-safely sys-map)]

    (testing "Dependencies of roots only consist of the roots themselves"
        (is (bs/transitive-dependents sys-map :a) #{:a})
        (is (bs/transitive-dependents sys-map :e) #{:e}))

    (testing "Dependencies of leaf notes includes parents and grand parents"
      (is (bs/transitive-dependents sys-map :d) #{:a :b :c :d :e}))


    (testing "Stop transitive dependencies in the correct order"
      (reset! log-stop '())
      (let [system-stopped (bs/transitive-stop-component system-started :d)]
        (is (= @log-stop (reverse '(:e :a :c :d))))
        (is (not (-> system-stopped :e :is-started)))
        (is (not (-> system-stopped :a :is-started)))
        (is (not (-> system-stopped :c :is-started)))
        (is (not (-> system-stopped :d :is-started)))
        (is (-> system-stopped :b :is-started))
        (is (-> system-stopped :f :is-started))
        ))

    (testing "Starts transitive dependencies in the correct order"
      (reset! log-start '())
      (let [system-started (bs/transitive-start-component sys-map :d)]
        (is (= @log-start (reverse '(:d :c :a :e))))
        (is (-> system-started :e :is-started))
        (is (-> system-started :a :is-started))
        (is (-> system-started :c :is-started))
        (is (-> system-started :d :is-started))
        (is (not (-> system-started :b :is-started)))
        (is (not (-> system-started :f :is-started)))))))





