use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    int2: Option<i64>,
    int3: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    int1: Option<i64>,
    int2: Option<i64>,
    int3: Option<i64>,
    sum: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_3 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    mul: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    mul: Option<i64>,
    int1_right: Option<i64>,
    int2: Option<i64>,
    int3: Option<i64>,
    sum: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_5 {
    string1: Option<String>,
    int1: Option<i64>,
    int3: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    int1: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/many_ints.csv",
    );
    let var_1 = var_0.map(|x| Struct_var_1 {
        int1: x.int1,
        int2: x.int2,
        int3: x.int3,
        sum: x.int3.map(|v| v + 100),
    });
    let var_2 = ctx.stream_csv::<Struct_var_2>(
        "/home/carlo/Projects/ibis-renoir-compiler/data/nullable_op/ints_strings.csv",
    );
    let var_5 = var_2
        .filter(|x| x.int1.clone().is_some_and(|v| v < 200))
        .map(|x| Struct_var_3 {
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            mul: x.int1.map(|v| v * 20),
        })
        .join(var_1, |x| x.int1.clone(), |y| y.int1.clone())
        .map(|(_, x)| Struct_var_4 {
            int1: x.0.int1,
            string1: x.0.string1,
            int4: x.0.int4,
            mul: x.0.mul,
            int1_right: x.1.int1,
            int2: x.1.int2,
            int3: x.1.int3,
            sum: x.1.sum,
        })
        .map(|(_, x)| Struct_var_5 {
            string1: x.string1,
            int1: x.int1,
            int3: x.int3,
        });
    let out = var_5.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();
    let out = out.get().unwrap();
    let out = out
        .iter()
        .map(|(k, v)| (Struct_collect { int1: k.clone() }, v))
        .collect::<Vec<_>>();
    let file = File::create("../out/noir-result.csv").unwrap();
    let mut wtr = csv::WriterBuilder::new().from_writer(file);

    for e in out {
        wtr.serialize(e).unwrap();
    }
    wtr.flush().unwrap();
}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();
    tracing_subscriber::fmt::init();

    let ctx = StreamContext::new_local();

    tracing::info!("building graph");
    logic(ctx);

    tracing::info!("finished execution");

    Ok(())
}
