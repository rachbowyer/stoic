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

(defn- bounce-component! [get-sys-map update-sys-map config-supplier k c settings-atom]
  (let [settings (cs/fetch config-supplier k)
        sys-map (get-sys-map)]
    (when (not= @settings-atom settings)
      (let [stopped-sys-map (component/stop sys-map)]
        (reset! settings-atom settings)
        (-> stopped-sys-map start-safely update-sys-map)))))

(defn- bounce-components-if-config-changes!
  "Add watchers to config to bounce relevant component if config changes."
  [get-sys-map update-sys-map config-supplier components component-settings]
  (doseq [[k c] components
          :let [settings-atom (get component-settings k)]
          :when settings-atom]
    (cs/watch! config-supplier k
               (partial bounce-component! get-sys-map update-sys-map config-supplier k c settings-atom))))

(defn bootstrap
  "Inject system with settings fetched from a config-supplier.
   Components will be bounced when their respective settings change.
   Returns a SystemMap with Stoic config attached."
  ([get-sys-map update-sys-map]
     (bootstrap get-sys-map update-sys-map (choose-supplier)))
  ([get-sys-map update-sys-map config-supplier]
     (let [system (get-sys-map)
           config-supplier-component (component/start config-supplier)
           component-settings (fetch-settings config-supplier-component system)
           system (inject-components component-settings system)]
       (bounce-components-if-config-changes!
         get-sys-map update-sys-map config-supplier-component system component-settings)
       (assoc system :stoic-config config-supplier-component))))


