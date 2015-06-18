(ns stoic.bootstrap
  "Bootstrap a component system with components and settings."
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [stoic.config.file :as file]
            [stoic.protocols.config-supplier :as cs]))

(defn choose-supplier []
  (if (file/enabled?)
    (file/config-supplier)
    (do
      (require 'stoic.config.curator)
      ((resolve 'stoic.config.curator/config-supplier)))))

(defn- inject-components
  "Inject components associating in the respective settings as an atom.
   Returns a new SystemMap."
  [component-settings system]
  (apply component/system-map
         (reduce into []
                 (for [[k c] system]
                   [k (or (and (:settings c) c)
                          (assoc c :settings (get component-settings k)))]))))

(defn- fetch-settings
  "Fetch settings from the config supplier and wrap in atoms."
  [config-supplier system]
  (into {} (for [[k c] system
                 :when (not (:settings c))]
             [k (atom (cs/fetch config-supplier k))])))

(defn start-safely
  "Will start a system.
   If an error occurs in any of the components when the system starts,
   the error will be caught and an attempted system shutdown performed."
  [system]
  (try
    (component/start system)
    (catch Throwable t
      (try
        (log/error t "Could not start up system, attempting to shutdown")
        (println "Error occuring starting system, check logs.")
        (component/stop system)
        (catch Throwable t
          (log/error t "Could not shutdown system")
          system)))))

(defn transitive-dependents [system component]
 "Identifies all components dependent on a specific component and returns
  this along with the component itself"
  (let [graph (component/dependency-graph system (keys system))
        dependents (.transitive-dependents graph component)]
    (conj dependents component)))

(defn transitive-stop-component [system component]
  "Stops component and all components that are dependant on it in reverse
  dependency order."
  (let [dependents (transitive-dependents system component)]
    (log/info (str "Stopping components " dependents " in reverse dependency order"))
    (component/stop-system system dependents)))

(defn transitive-start-component [system component]
  "Starts component and all components that are dependant on it in
  dependency order."
  (let [dependents (transitive-dependents system component)]
    (log/info (str "Starting components " dependents " in dependency order"))
    (component/start-system system dependents)))

(defn- bounce-component! [get-system update-system config-supplier k settings-atom]
  (let [settings (cs/fetch config-supplier k)
        system (get-system)]
    (when (not= @settings-atom settings)
      (let [stopped-system (transitive-stop-component system k)]
        (reset! settings-atom settings)
        (update-system (transitive-start-component stopped-system k))))))

(defn- bounce-components-if-config-changes!
  "Add watchers to config to bounce relevant component if config changes."
  [get-system update-system config-supplier components component-settings]
  (doseq [[k _] components
          :let [settings-atom (get component-settings k)]
          :when settings-atom]
    (cs/watch! config-supplier k
               (partial bounce-component! get-system update-system config-supplier k settings-atom))))

(defn bootstrap
  "Inject system with settings fetched from a config-supplier.
   Components will be bounced when their respective settings change.
   Returns a SystemMap with Stoic config attached."
  ([get-system update-system]
     (bootstrap get-system update-system (choose-supplier)))
  ([get-system update-system config-supplier]
     (let [system (get-system)
           config-supplier-component (component/start config-supplier)
           component-settings (fetch-settings config-supplier-component system)
           system (inject-components component-settings system)]
       (bounce-components-if-config-changes!
         get-system update-system config-supplier-component system component-settings)
       ;(assoc system :stoic-config config-supplier-component)
       {:system system :config-supplier config-supplier-component})))




