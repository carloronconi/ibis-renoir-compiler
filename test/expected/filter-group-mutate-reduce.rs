use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_0 {
    int1: i64,
    string1: String,
    int4: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_1 {
    int1: i64,
    string1: String,
    int4: i64,
    mul: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_2 {
    agg: i64,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_2 = var_0
        .filter(|x| x.int1 > 200)
        .map(|x| Struct_var_1 {
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            mul: x.int1 * 20,
        })
        .group_by(|x| x.string1.clone())
        .reduce(|a, b| a.mul = a.mul + b.mul)
        .map(|(_, x)| Struct_var_2 { agg: x.mul });
    let out = var_2.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();

    let out = out.get().unwrap();
    let file = File::create("../out/noir-result.csv").unwrap();
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

    for e in out {
        wtr.serialize(e).unwrap();
    }
    wtr.flush().unwrap();
}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();
    tracing_subscriber::fmt::init();

    let ctx = StreamContext::default();

    tracing::info!("building graph");
    logic(ctx);

    tracing::info!("finished execution");

    Ok(())
}
