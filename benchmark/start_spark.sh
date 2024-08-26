#!/bin/bash
file="spark-3.1.2-bin-hadoop3.2.tgz"
dir="spark-3.1.2-bin-hadoop3.2"

#https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
if [ ! -s $file ]; then
    curl -O https://archive.apache.org/dist/spark/spark-3.1.2/$file
fi

if [ ! -d $dir ]; then
    tar -xzf $file
fi

# only for sola
if [[ $(hostname) == "sola1" ]]; then
    echo "sola1 detected, overriding default java to use java 11"
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
    export PATH=$JAVA_HOME/bin:$PATH
fi

rm -rf spark-warehouse

cd $dir
./sbin/start-master.sh -h 127.0.0.1

# https://issues.apache.org/jira/browse/SPARK-30978
# Based on our experience, there is no scenario that necessarily requires 
# deploying multiple Workers on the same node with Standalone backend. 
# A worker should book all the resources reserved to Spark on the host 
# it is launched, then it can allocate those resources to one or more 
# executors launched by this worker.
./sbin/start-worker.sh spark://127.0.0.1:7077 
