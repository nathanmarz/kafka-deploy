(ns kafka.deploy.deploy-util
  (:require [clojure.string :as s]
            [pallet.execute :as execute]))

(def env-keys-to-resolve [:username :public-key-path :private-key-path])

(defn resolve-path [path]
  (s/trim (:out (execute/local-script (echo ~path)))))

(defn resolve-keypaths
  [user-map]
  (reduce #(%2 %1)
          user-map
          (for [kwd env-keys-to-resolve]
            #(if (kwd %)
               (update-in % [kwd] resolve-path)
               %))))

(defn set-var-root* [avar val]
  (alter-var-root avar (fn [avar] val)))

(defmacro set-var-root [var-sym val]
  `(set-var-root* (var ~var-sym) ~val))

(defmacro with-var-roots [bindings & body]
  (let [settings (partition 2 bindings)
        tmpvars (repeatedly (count settings) (partial gensym "old"))
        vars (map first settings)
        savevals (vec (mapcat (fn [t v] [t v]) tmpvars vars))
        setters (for [[v s] settings] `(set-var-root ~v ~s))
        restorers (map (fn [v s] `(set-var-root ~v ~s)) vars tmpvars)
        ]
    `(let ~savevals
      ~@setters
      (try
        ~@body
      (finally
        ~@restorers))
      )))