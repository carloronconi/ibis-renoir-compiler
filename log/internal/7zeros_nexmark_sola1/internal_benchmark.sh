#!/bin/bash

# if you want to run the benchmark on a different dataset size, change the path_suffix, otherwise leave ""
# the git repository only contains the base non-suffixed files, so in case you use this variable you
# need to generate the extended files first.
# for nexmark, `cd data/nexmark_data_gen && cargo run -- 10000000`
# for operators, `cd data/operators_data_gen && cargo run -- 10000000` 

source .venv3.11/bin/activate

python -m benchmark.internal.internal_benchmark \
    --test_patterns TestNexmark \
    --runs 10 \
    --warmup 1 \
    --table_origin csv cached \
    --dir internal/$1 \
    --backends duckdb polars flink renoir \
    --path_suffix _10000000 \
&& cp benchmark/internal/internal_benchmark.sh log/internal/$1/internal_benchmark.sh

