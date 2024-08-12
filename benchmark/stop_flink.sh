#!/bin/bash
cd flink-1.19.0

for ((i=0; i<$1; i++)); do
    ./bin/stop-cluster.sh
done