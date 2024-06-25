#!/bin/bash

# just need one istance of each different test class: all cases in a class use the same tables
names=("test.test_operators.TestNullableOperators.test_nullable_inner_join_select" "test.test_nexmark.TestNexmark.test_nexmark_query_1")

source .venv3.11/bin/activate
for name in "${names[@]}"; do
    printf "loading tables for test: ${name}\n"
    python3 ../ibis-renoir-compiler $name --backend duckdb --path_suffix $1 --table_origin load
done