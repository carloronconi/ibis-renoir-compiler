#!/bin/bash
# run from project root to have the script automatically discover tests and measure their memory consumption to memo_log.csv

# tests by default would log their time consumption to codegen_log.csv and would perform assertions comparing ibis to noir
# env variables are set to prevent the tests themselves from logging time consumption (redundant slowdown) and 
# from performing assertions which compare correctness of ibis vs noir (memory consumption would also account for ibis's memory usage)
export PERFORM_ASSERTIONS="false"
export PERFORM_BENCHMARK="false"
export RUN_AFTER_GEN="true"
export RENDER_QUERY_GRAPH="false"

if [ ! -s log/memo_log.csv ]; then
    echo "test_name,elapsed_s,resident_k,user,system,status" > log/memo_log.csv
fi

source .venv/bin/activate
python -m benchmark.discover_tests | grep nexmark | while IFS= read -r name; do
    trim=${name##*.}
    echo "Running $trim"
    command time -a -o "log/memo_log.csv" -f "$trim,%e,%M,%U,%S,%x" python -m unittest $name > /dev/null 2>&1
done
 