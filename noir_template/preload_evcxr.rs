use renoir::prelude::*;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::cmp::max;
use renoir::operator::cache::StreamCache;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Structdatanullableopintsstrings {int1: Option<i64>,string1: Option<String>,int4: Option<i64>,}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Structdatanullableopmanyints {int1: Option<i64>,int2: Option<i64>,int3: Option<i64>,}
fn cache() -> (StreamCache<Structdatanullableopintsstrings>,
StreamCache<Structdatanullableopmanyints>) {
let ctx = StreamContext::new_local();
let (datanullableopintsstrings, a) = ctx.stream_csv::<Structdatanullableopintsstrings>("/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/ints_strings.csv").batch_mode(BatchMode::fixed(16000)).cache();
a.for_each(|x| {
    std::hint::black_box(x);
});
let (datanullableopmanyints, b) = ctx.stream_csv::<Structdatanullableopmanyints>("/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/many_ints.csv").batch_mode(BatchMode::fixed(16000)).cache();
b.for_each(|x| {
    std::hint::black_box(x);
});
ctx.execute_blocking();
return (datanullableopintsstrings, datanullableopmanyints);
}
