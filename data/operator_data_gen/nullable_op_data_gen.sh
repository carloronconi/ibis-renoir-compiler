#!/bin/bash

cargo run --release -- $1 nullable_op --names ints_strings many_ints --types isi iii --headers "int1,string1,int4" "int1,int2,int3"