(ns card-get
  (:require [camel-snake-kebab.core :as csk]))

(defn handler
  [_event _ctx]
  (let [foo (csk/->snake_case :bar-baz)]
    (println foo)))


;; NOTE: This handler export must be the last expression in the file

;; exports
#js {:handler handler}
