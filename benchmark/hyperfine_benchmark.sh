#!/usr/bin/bash

# add the backends you want to compare renoir against
backends_compare_against=("duckdb")
# backends_compare_against=("duckdb" "polars")

source .venv/bin/activate
# change grep to filter the tests you want to run
python -m benchmark.discover_tests | grep "nullable" | while IFS= read -r name; do
    trim=${name##*.}
    for backend in "${backends_compare_against[@]}"; do
        # TODO: change the paths to the correct ones for each test, considering that they will use differently sized data files (with different names)
        hyperfine --warmup 5 \
        "python ../ibis-quickstart $name --paths /home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv /home/carlo/Projects/ibis-quickstart/data/int-3.csv --backend renoir" \
        "python ../ibis-quickstart $name --paths /home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv /home/carlo/Projects/ibis-quickstart/data/int-3.csv --backend $backend" \
        --export-json log/hyperfine_${backend}_${trim}.json
    done
done