#!/bin/bash

# add the backends you want to compare renoir against
backends_compare_against=("duckdb")
# backends_compare_against=("duckdb" "polars")

# if you want to run the benchmark on a different dataset size, change the size_suffix, otherwise leave ""
# the git repository only contains the base non-suffixed files, so in case you use this variable you
# need to generate the extended files first.
# For nexmark, `cd data/nexmark_data_gen && cargo run -- 100000000`
size_suffix="_10000000"

source .venv/bin/activate
# change grep to filter the tests you want to run
mkdir -p log/$1
python3 -m benchmark.discover_tests | grep "test_operators" | while IFS= read -r name; do
    trim=${name##*.}
    for backend in "${backends_compare_against[@]}"; do
        hyperfine --warmup 1 \
        "python3 ../ibis-renoir-compiler $name --backend renoir --path_suffix $size_suffix" \
        "python3 ../ibis-renoir-compiler $name --backend $backend --path_suffix $size_suffix" \
        --export-json log/$1/hyperfine_${backend}_${trim}${size_suffix}.json
        printf "\n"
    done
done
cp log/codegen_log.csv log/$1/codegen_log.csv
