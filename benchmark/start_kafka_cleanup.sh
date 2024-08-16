#!/bin/bash
# launch after starting kafka to delete all topics aside from __consumer_offsets
# list and delete pre-existing topics
cd kafka_2.13-3.8.0
topics=$(./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)
filtered_topics=$(echo "$topics" | grep -v "__consumer_offsets")
for topic in $filtered_topics; do
    ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $topic
done

# Print the current topic list after deletion
echo "Deleted all pre-existing topics. Topics after deletion:"
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list