#!/bin/bash
# local kafka install https://kafka.apache.org/quickstart
file="kafka_2.13-3.8.0.tgz"
dir="kafka_2.13-3.8.0"

if [ ! -s $file ]; then
    curl -O https://dlcdn.apache.org/kafka/3.8.0/$file
fi

if [ ! -d $dir ]; then
    tar -xzf $file
fi

cd $dir

KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
./bin/kafka-server-start.sh config/kraft/server.properties

# if ends in bad state, do:
# rm -rf /tmp/kraft-combined-logs
# if gets nio exception, set java home and path to java 11