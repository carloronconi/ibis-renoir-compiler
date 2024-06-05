#!/usr/bin/bash

# add the backends you want to compare renoir against
backends_compare_against=("duckdb")
# backends_compare_against=("duckdb" "polars")
file_size="100000000"

source .venv/bin/activate
# change grep to filter the tests you want to run
python -m benchmark.discover_tests | grep "nexmark_query_2" | while IFS= read -r name; do
    trim=${name##*.}
    for backend in "${backends_compare_against[@]}"; do
        hyperfine --warmup 5 \
        "python ../ibis-quickstart $name --backend renoir --path_suffix _$file_size" \
        "python ../ibis-quickstart $name --backend $backend --path_suffix _$file_size" \
        --export-json log/hyperfine_${backend}_${trim}_${file_size}.json
    done
done