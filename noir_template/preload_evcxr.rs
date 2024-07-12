use renoir::prelude::*;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::cmp::max;
use renoir::operator::cache::StreamCache;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_data_nullable_op_ints_strings {int1: Option<i64>,string1: Option<String>,int4: Option<i64>,}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_data_nullable_op_many_ints {int1: Option<i64>,int2: Option<i64>,int3: Option<i64>,}

fn cache() -> (StreamCache<Struct_data_nullable_op_ints_strings>,StreamCache<Struct_data_nullable_op_many_ints>,){
let ctx = StreamContext::new_local();
let (data_nullable_op_ints_strings, data_nullable_op_ints_strings_temp) = ctx.stream_csv::<Struct_data_nullable_op_ints_strings>("/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/ints_strings.csv").batch_mode(BatchMode::fixed(16000)).cache();
data_nullable_op_ints_strings_temp.for_each(|x| {std::hint::black_box(x);});
let (data_nullable_op_many_ints, data_nullable_op_many_ints_temp) = ctx.stream_csv::<Struct_data_nullable_op_many_ints>("/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/many_ints.csv").batch_mode(BatchMode::fixed(16000)).cache();
data_nullable_op_many_ints_temp.for_each(|x| {std::hint::black_box(x);});
ctx.execute_blocking();
return (data_nullable_op_ints_strings, data_nullable_op_many_ints, );
}