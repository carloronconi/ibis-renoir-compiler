#!/bin/bash
# This script does all the work! Just pass a single parameter for dataset size (min 100000, or use 0 for dummy super-small data).
# Run it from its directory.
# Assuming you have a dbgen-tpc-h directory alongside the ibis-renoir-compiler directory, where you already ran `make`.

cd ../../../dbgen-tpc-h/ 

# Scale factor of 1 corresponds to suffix 1000000 (1M).
# Compute the division with python as bash does not support floating point arithmetic.
scale=$(python -c "print($1/1000000)")

echo "Generating data for scale factor $scale"
./dbgen -s $scale

files="*.tbl"
for f in $files; do
        # split the file name from the extension
        name=$(echo $f | cut -f 1 -d '.')
        mv ./${f} ../ibis-renoir-compiler/data/tpch/${name}_$1.tbl
done

cd ../ibis-renoir-compiler/data/tpc_data_gen/
cargo run -- ./../tpch --pattern $1.tbl
