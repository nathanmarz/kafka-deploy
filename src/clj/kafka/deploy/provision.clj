(ns kafka.deploy.provision
  (:use [pallet compute configure core resource]
        [clojure.contrib command-line]
        [kafka.deploy security]
        [org.jclouds.compute :only [nodes-with-tag]])
  (:require [kafka.deploy.crate [zookeeper :as zookeeper]]
            [pallet.crate.java :as java]
            [pallet.crate.automated-admin-user :as automated-admin-user]
            [kafka.deploy.deploy-util :as util]
            [org.jclouds.compute :only [nodes-with-tag]]
            [pallet.resource.remote-file :as remote-file]
            [pallet.resource.exec-script :as exec-script]
            [pallet.resource.package :as package]
            [clojure.contrib [str-utils2 :as str]]
            [pallet [session :as session]]
            ))
  
(defn target-node-index
  "Returns the target node's index within its group. For a group of size 3, returns either 0, 1 or 2."
  [request]
  (let [nodes (sort-by private-ip (session/nodes-in-group request))
        node->idx (zipmap nodes (iterate inc 0))]
    (node->idx (session/target-node request))))  
  
(defn my-region []
  (-> (pallet-config) :services :default :jclouds.regions)
  )

(defn jclouds-group [& group-pieces]
  (str "jclouds#"
       (apply str group-pieces)
       "#"
       (my-region)
       ))  
  
(defn zookeeper-ips [compute name]
  (let [running-nodes (filter running?
                              (nodes-with-tag (str "kafka-zookeeper-" name) compute))]
    (map private-ip running-nodes)))  
  
(def *USER* nil)

(defn base-server-spec []
  (server-spec
   :phases {:bootstrap (fn [req] (automated-admin-user/automated-admin-user
                                  req
                                  (:username *USER*)
                                  (:public-key-path *USER*)))
            :configure (phase
                        (java/java :sun :jdk))}))

(def ZK-VERSION "3.3.4")

(defn zookeeper-server-spec []
  (server-spec
   :extends (base-server-spec)
   :phases {:configure (phase
                        (zookeeper/install :version ZK-VERSION)
                        (zookeeper/configure
                         :clientPort 2181
                         :maxClientCnxns 0)
                        (zookeeper/init))
            :post-configure (phase
                          (exec-script/exec-script
                            (cd ~(str zookeeper/install-path "-" ZK-VERSION))
                            (sh "bin/zkCli.sh create /kafka 1")
                            ))
                        }))

(def RELEASE-URL "http://people.apache.org/~nehanarkhede/kafka-0.7.0-incubating/kafka-0.7.0-incubating-src.tar.gz")

(defn download-release [request]
  (-> request
    (remote-file/remote-file
       "$HOME/kafka.tar.gz"
       :url RELEASE-URL
       :no-versioning true)
    ))

(defn mk-zk-str [compute name]
  (->> (zookeeper-ips compute name)
       (map #(str % ":2181"))
       (str/join ",")
       (#(str % "/kafka"))    
       ))

(defn kafka-server-spec [name]
  (server-spec
   :extends (base-server-spec)
   :phases {:configure (phase
                         (package/package "daemontools")
                         (download-release)
                         (exec-script/exec-checked-script
                           "build kafka"
                           (cd "$HOME")
                           (tar "-xzf kafka.tar.gz")
                           (mv "kafka-0.7.0-incubating-src" "kafka")
                           (cd "kafka")
                           (sh "sbt update")
                           (sh "sbt package")
                           (mkdir "logs")))
            :post-configure
            (fn [request]
              (-> request
                (remote-file/remote-file "$HOME/kafka/config/server.properties"
                             :template "server.properties"
                             :values {'zk-str (mk-zk-str (:compute request) name)
                                      'id (target-node-index request)}
                             :owner (:username *USER*))
                (remote-file/remote-file "$HOME/kafka/config/log4j.properties"
                             :template "log4j.properties"
                             :owner (:username *USER*))
                (remote-file/remote-file
                   "$HOME/kafka/run"
                   :content (str
                             "#!/bin/bash\n\n
                          sh bin/kafka-server-start.sh config/server.properties")
                   :overwrite-changes true
                   :literal true
                   :mode 755)
                ))
            :exec (phase
                    (exec-script/exec-script
                           (cd "$HOME/kafka")
                           "nohup supervise . &")
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
    (str "kafka-" name)
    :node-spec (node-spec
                  :image {:inbound-ports [2181 22]
                          :image-id "us-east-1/ami-08f40561"
                          :hardware-id "m1.large"
                          })
    :extends (kafka-server-spec name)))

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
  (println "Starting cluster")
  (println (format "Provisioning nodes [zn=%d, kn=%d]" zn kn))
  (converge! name aws kn zn)
  (authorize-group aws (my-region) (jclouds-group "kafka-" name) (jclouds-group "kafka-zookeeper-" name))
  (authorize-group aws (my-region) (jclouds-group "kafka-zookeeper-" name) (jclouds-group "kafka-" name))

  (lift (zookeeper name) :compute aws :phase [:post-configure])  
  (lift (kafka name) :compute aws :phase [:post-configure :exec])
  (println "Provisioning Complete.")
  (print-all-ips! aws name))

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
         [name "Cluster name" "dev"]
         [kn "Number of Kafka nodes" "1"]
         [zn "Number of Zookeeper nodes" "1"]]

        (cond 
         stop? (stop! aws name)
         start? (start! aws name (Integer/parseInt kn) (Integer/parseInt zn))
         ips? (print-all-ips! aws name)
         :else (println "Must pass --start or --stop or --ips"))))))