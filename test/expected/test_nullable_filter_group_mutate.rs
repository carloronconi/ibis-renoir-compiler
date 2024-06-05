use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    string1: Option<String>,
    int1_agg: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    string1: Option<String>,
    int1_agg: Option<i64>,
    mul: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    string1: Option<String>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-quickstart/data/nullable_op/ints_strings.csv",
    );
    let var_2 = var_0
        .filter(|x| x.string1.clone().is_some_and(|v| v == "unduetre"))
        .group_by(|x| (x.string1.clone()))
        .reduce(|a, b| {
            a.int1 = a.int1.zip(b.int1).map(|(x, y)| x);
        })
        .map(|(k, x)| Struct_var_1 {
            string1: k.clone(),
            int1_agg: x.int1,
        })
        .map(|(_, x)| Struct_var_2 {
            string1: x.string1,
            int1_agg: x.int1_agg,
            mul: x.int1_agg.map(|v| v * 20),
        });
    let out = var_2.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();
    let out = out.get().unwrap();
    let out = out
        .iter()
        .map(|(k, v)| (Struct_collect { string1: k.clone() }, v))
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
