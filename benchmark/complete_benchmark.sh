#!/usr/bin/bash
# run script from project root to have it automatically discover tests and run them
# while logging memory consumption to memo_log.csv
# the tests themselves also log time consumption divided by Renoir compile and run vs Ibis

if [ ! -s log/memo_log.csv ]; then
    echo "test_name,elapsed_s,resident_k,user,system,status" > log/memo_log.csv
fi

source .venv/bin/activate
python -m benchmark.discover_tests | while IFS= read -r name; do
    trim=${name##*.}
    echo "Running $trim"
    command time -a -o "log/memo_log.csv" -f "$trim,%e,%M,%U,%S,%x" python -m unittest $name > /dev/null 2>&1
done
 