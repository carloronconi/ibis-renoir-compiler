#!/bin/bash

# add the backends you want to compare renoir against
backends=("renoir" "duckdb" "polars" "flink")
# backends=("duckdb" "polars")

# if you want to run the benchmark on a different dataset size, change the size_suffix, otherwise leave ""
# the git repository only contains the base non-suffixed files, so in case you use this variable you
# need to generate the extended files first.
# For nexmark, `cd data/nexmark_data_gen && cargo run -- 100000000`
size_suffix="_100000000"

source .venv/bin/activate
# change grep to filter the tests you want to run
mkdir -p log/$1
i=0
python3 -m benchmark.discover_tests | \
grep -v -e "test_nullable_group_reduce_group_reduce_join" -e "test_nullable_group_reduce_join_mutate" -e "test_non_nullable_filter_filter_select_select" | \
while IFS= read -r name; do
    i=$((i+1))
    if [ $i -lt 7 ]; then
        continue
    fi
    trim=${name##*.}
    for backend in "${backends[@]}"; do
        hyperfine --warmup 1 \
        "python3 ../ibis-renoir-compiler $name --backend $backend --path_suffix $size_suffix" \
        --export-json log/$1/hyperfine_${trim}${size_suffix}_${backend}.json
        printf "\n"
        # copy every time we run a test so if we quit before finish all tests we still have both
        cp log/codegen_log.csv log/$1/codegen_log.csv
        cp benchmark/hyperfine_benchmark.sh log/$1/hyperfine_benchmark.sh
    done
done