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

cd $dir
./sbin/start-master.sh -h local

# https://issues.apache.org/jira/browse/SPARK-30978
# Based on our experience, there is no scenario that necessarily requires 
# deploying multiple Workers on the same node with Standalone backend. 
# A worker should book all the resources reserved to Spark on the host 
# it is launched, then it can allocate those resources to one or more 
# executors launched by this worker.
./sbin/start-worker.sh spark://local:7077 