(ns backtype.storm.provision
  (:import [java.io File])
  (:use 
        [backtype.storm security]
        [org.jclouds.compute :only [nodes-with-tag]]
        [backtype.storm.util :only [with-var-roots]]
        )
  (:require [backtype.storm.node :as node])
  (:require [backtype.storm.crate.storm :as storm])
  (:require [kafka.deploy.deploy-util :as util])
  )

(ns kafka.deploy.provision
  (:use [pallet compute configure core]
        [clojure.contrib command-line])
  (:require [kafka.deploy.crate [zookeeper :as zookeeper]]
            [pallet.crate.java :as java]
            [pallet.crate.automated-admin-user :as automated-admin-user]
            [kafka.deploy.deploy-util :as util]
            [org.jclouds.compute :only [nodes-with-tag]]
            ))
  
(def *USER* nil)

(defn base-server-spec []
  (server-spec
   :phases {:bootstrap (fn [req] (automated-admin-user/automated-admin-user
                                  req
                                  (:username *USER*)
                                  (:public-key-path *USER*)))
            :configure (phase
                        (java/java :sun :jdk))}))

(defn zookeeper-server-spec []
  (server-spec
   :extends (base-server-spec)
   :phases {:configure (phase
                        (zookeeper/install :version "3.3.4")
                        (zookeeper/configure
                         :clientPort 2181
                         :maxClientCnxns 0)
                        (zookeeper/init))
                        }))

(defn kafka-server-spec []
  (server-spec
   :extends (base-server-spec)
   :phases {:configure (phase
                         
                         )
            :post-configure (phase
                              )
            :exec (phase
                    )}))

(defn zookeeper [name]
  (group-spec
    (str "kafka-zookeeper-" name)
    :node-spec (node-spec
                  :image {:inbound-ports [2181 22]
                          :image-id "us-east-1/ami-08f40561"
                          :hardware-id "m1.large"
                          })
    :extends (zookeeper-server-spec)))

(defn kafka [name]
  (group-spec
    (str "kafka-zookeeper-" name)
    :node-spec (node-spec
                  :image {:inbound-ports [2181 22]
                          :image-id "us-east-1/ami-08f40561"
                          :hardware-id "m1.large"
                          })
    :extends (kafka-server-spec)))

(defn converge! [name aws kn zn]
  (converge {(kafka name) kn
             (zookeeper name) zn
             }
            :compute aws))

(defn kafka-config
  ([] (pallet-config "default"))
  ([conf-name] (compute-service-properties (pallet-config) [conf-name])))

(defn- print-ips-for-tag! [aws tag-str]
  (let [running-node (filter running? (nodes-with-tag tag-str aws))]
    (println "TAG:     " tag-str)
    (println "PUBLIC:  " (map primary-ip running-node))
    (println "PRIVATE: " (map private-ip running-node))))

(defn print-all-ips! [aws name]
  (let [all-tags [(str "kafka-zookeeper-" name) (str "kafka-" name)]]
       (doseq [tag all-tags]
         (print-ips-for-tag! aws tag))))

(defn start! [aws name kn zn]
  (println "Starting cluster with release" release)
    (println (format "Provisioning nodes [zn=%d, kn=%d]" zn kn))
    (converge! name aws kn zn)
    (authorize-group aws (my-region) (jclouds-group "kafka-" name) (jclouds-group "kafka-zookeeper-" name))
    (authorize-group aws (my-region) (jclouds-group "kafka-zookeeper-" name) (jclouds-group "kafka-" name))

    (lift (kafka name) :compute aws :phase [:post-configure :exec])
    (println "Provisioning Complete.")
    (print-all-ips! aws name)))

(defn stop! [aws name]
  (println "Shutting Down nodes...")
  (converge! name aws 0 0)
  (println "Shutdown Finished."))

(defn mk-aws []
  (let [conf (-> (kafka-config "default")
                 (update-in [:environment :user] util/resolve-keypaths))]
    (compute-service-from-map conf)))

(defn -main [& args]
  (let [aws (mk-aws)
        user (-> (kafka-config "default")
                 :environment
                 :user
                 util/resolve-keypaths)
        ]
    (util/with-var-roots [*USER* user]
      (with-command-line args
        "Provisioning tool for Kafka Clusters"
        [[start? "Start Cluster?"]
         [stop? "Shutdown Cluster?"]
         [ips? "Print Cluster IP Addresses?"]
         [name "Cluster name" "dev"]]

        (cond 
         stop? (stop! aws name)
         start? (start! aws name)
         ips? (print-all-ips! aws name)
         :else (println "Must pass --start or --stop or --ips"))))))