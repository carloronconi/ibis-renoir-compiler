#!/bin/bash
# from official guide https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/
# to test if it's running: http://localhost:8081/
FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager
taskmanager.numberOfTaskSlots: 1"
docker network rm flink-network
docker stop jobmanager
docker stop taskmanager

docker network create flink-network

# share the ./data dir with the container with read-write permissions
docker run -d \
    --rm \
    --name=jobmanager \
    --network flink-network \
    --publish 8081:8081 \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    -v ./data:/opt/flink/data:rw \
    flink:1.19.1 jobmanager 

docker run -d \
    --rm \
    --name=taskmanager \
    --network flink-network \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    flink:1.19.1 taskmanager
