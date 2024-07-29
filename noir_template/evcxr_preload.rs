use renoir::prelude::*;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::cmp::max;
use renoir::operator::cache::StreamCache;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_data_nullable_op_ints_strings_10 {int1: Option<i64>,string1: Option<String>,int4: Option<i64>,}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_data_nullable_op_many_ints_10 {int1: Option<i64>,int2: Option<i64>,int3: Option<i64>,}

fn cache() -> (StreamCache<Struct_data_nullable_op_ints_strings_10>,StreamCache<Struct_data_nullable_op_many_ints_10>,){
let ctx = StreamContext::new_local();
let (data_nullable_op_ints_strings_10, data_nullable_op_ints_strings_10_temp) = ctx.stream_csv::<Struct_data_nullable_op_ints_strings_10>("/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/ints_strings_10.csv").batch_mode(BatchMode::fixed(16000)).group_by(|x| (x.string1.clone())).reduce(|a, b| {a.int1 = a.int1.zip(b.int1).map(|(x, y)| max(x, y));a.int4 = a.int4.zip(b.int4).map(|(x, y)| x + y)}).drop_key().cache();
data_nullable_op_ints_strings_10_temp.for_each(|x| {std::hint::black_box(x);});
let (data_nullable_op_many_ints_10, data_nullable_op_many_ints_10_temp) = ctx.stream_csv::<Struct_data_nullable_op_many_ints_10>("/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/many_ints_10.csv").batch_mode(BatchMode::fixed(16000)).cache();
data_nullable_op_many_ints_10_temp.for_each(|x| {std::hint::black_box(x);});
ctx.execute_blocking();
return (data_nullable_op_ints_strings_10, data_nullable_op_many_ints_10, );
}let (data_nullable_op_ints_strings_10, data_nullable_op_many_ints_10, ) = cache();
