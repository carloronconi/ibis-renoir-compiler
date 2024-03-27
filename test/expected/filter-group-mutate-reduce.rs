use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_0 {
    int1: i64,
    string1: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_1 {
    int1: i64,
    string1: String,
    mul: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_2 {
    agg: i64,
}

fn logic(ctx: &StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    var_0
        .filter(|x| x.int1 > 200)
        .map(|x| Struct_var_1 {
            int1: x.int1,
            string1: x.string1,
            mul: x.int1 * 20,
        })
        .group_by(|x| x.string1.clone())
        .reduce(|a, b| a.mul = a.mul + b.mul)
        .map(|(_, x)| Struct_var_2 { agg: x.mul })
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
