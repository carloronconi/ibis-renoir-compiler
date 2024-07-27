#!/bin/bash
file="flink-1.19.0-bin-scala_2.12.tgz"
dir="flink-1.19.0"

if [ ! -s $file ]; then
    curl -O https://dlcdn.apache.org/flink/flink-1.19.0/$file
fi

if [ ! -d $dir ]; then
    tar -xzf $file
fi

cp benchmark/flink_config.yaml flink-1.19.0/conf/config.yaml

cd flink-1.19.0
./bin/start-cluster.sh