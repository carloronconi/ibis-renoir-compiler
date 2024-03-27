use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Cols_table0 {
    int1: i64,
    string1: String,
}

fn logic(ctx: &StreamContext) {
    let table0 = ctx
        .stream_csv::<Cols_table0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    table0
        .filter(|x| x.string1 == "unduetre")
        .reduce(|a, b| *a = *a)
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
