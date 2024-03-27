use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_0 {
    int1: i64,
    int2: i64,
    int3: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_1 {
    int1: i64,
    int2: i64,
    int3: i64,
    sum: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_2 {
    int1: i64,
    string1: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_3 {
    int1: i64,
    string1: String,
    mul: i64,
}

fn logic(ctx: &StreamContext) {
    let var_0 =
        ctx.stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-3.csv");
    let var_1 = var_0.map(|x| Struct_var_1 {
        int1: x.int1,
        int2: x.int2,
        int3: x.int3,
        sum: x.int3 + 100,
    });
    let var_2 = ctx
        .stream_csv::<Struct_var_2>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_3 = var_2
        .filter(|x| x.int1 < 200)
        .map(|x| Struct_var_3 {
            int1: x.int1,
            string1: x.string1,
            mul: x.int1 * 20,
        })
        .join(var_1, |x| x.int1, |y| y.int1);
    var_3.for_each(|x| println!("{x:?}"));
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
