Automated deploy for a Kafka cluster on AWS. Can be used with [Storm](https://github.com/nathanmarz/storm) or standalone. 

The deploy configures both Zookeeper and Kafka.

## Usage

Install [leiningen](https://github.com/technomancy/leiningen).

Set up your `~/.pallet/config.clj` as described [here](https://github.com/nathanmarz/storm-deploy/wiki)

Start a cluster:

```
lein deps
lein run :deploy --start --name mykafkacluster --kn 8 --zn 2
```

This creates a cluster called "mykafkacluster" with 8 kafka nodes and 2 zookeeper nodes.


Stop a cluster:

```
lein run :deploy --stop --name mykafkacluster
```
