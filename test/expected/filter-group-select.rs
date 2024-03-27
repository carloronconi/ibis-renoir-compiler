use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_0 {
    int1: i64,
    string1: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_1 {
    int1_agg: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_2 {
    int1_agg: i64,
}

fn logic(ctx: &StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    var_0
        .filter(|x| x.string1 == "unduetre")
        .group_by(|x| x.string1.clone())
        .reduce(|a, b| a.int1 = a.int1)
        .map(|(_, x)| Struct_var_1 { int1_agg: x.int1 })
        .map(|(_, x)| Struct_var_2 {
            int1_agg: x.int1_agg,
        })
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
