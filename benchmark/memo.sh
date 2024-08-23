#!/bin/bash
# rss is expressed in KiloBytes (from man ps)
# usage: ./memo.sh flink 10M_s1-s4_sola1
proc_pids=$(pgrep -d "," -f $1)
echo "The following PIDs were found for keyword $1: $proc_pids"

filename="log/scenario/$2/memo_$1.csv"
echo "The sum of their memory every second will be stored in file $filename"
mkdir -p "log/scenario/$2"
touch "$filename"
echo "timestamp,memory_KB" > "$filename"

while true;
do
    timestamp=$(date +"%Y%m%d%H%M%S")
    sum=$(ps -p $proc_pids -o rss --noheader | awk '{ sum += $1 } END { print sum }')
    echo "$timestamp,$sum" >> "$filename"
    sleep 1
done