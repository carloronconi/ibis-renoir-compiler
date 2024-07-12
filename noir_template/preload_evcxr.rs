use renoir::prelude::*;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::cmp::max;
use renoir::operator::cache::StreamCache;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Structdatanullableopintsstrings {int1: Option<i64>,string1: Option<String>,int4: Option<i64>,}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Structdatanullableopmanyints {int1: Option<i64>,int2: Option<i64>,int3: Option<i64>,}

let ctx = StreamContext::new_local();
let (datanullableopintsstrings, _): (StreamCache<Structdatanullableopintsstrings>, _) = ctx.stream_csv::<Structdatanullableopintsstrings>("../data/nullable_op/ints_strings.csv").batch_mode(BatchMode::fixed(16000)).cache();
let (datanullableopmanyints, _): (StreamCache<Structdatanullableopmanyints>, _) = ctx.stream_csv::<Structdatanullableopmanyints>("../data/nullable_op/many_ints.csv").batch_mode(BatchMode::fixed(16000)).cache();
ctx.execute_blocking();
