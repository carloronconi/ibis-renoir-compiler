use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Cols_table0 {
    int1: i64,
    string1: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Cols_table1 {
    int1: i64,
    int2: i64,
    int3: i64,
}

fn logic(ctx: &StreamContext) {
    let table0 = ctx
        .stream_csv::<Cols_table0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let table1 =
        ctx.stream_csv::<Cols_table1>("/home/carlo/Projects/ibis-quickstart/data/int-3.csv");
    table0
        .left_join(table1, |x| x.int1, |y| y.int1)
        .for_each(|x| println!("{x:?}"));
}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();
    tracing_subscriber::fmt::init();

    let ctx = StreamContext::default();

    tracing::info!("building graph");
    logic(&ctx);

    tracing::info!("starting execution");
    ctx.execute_blocking();
    tracing::info!("finished execution");

    Ok(())
}
