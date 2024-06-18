use mimalloc::MiMalloc;
use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    mul: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    string1: Option<String>,
    agg: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    string1: Option<String>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("../data/nullable_op/ints_strings.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_2 = var_0
        .filter(|x| x.int1.clone().is_some_and(|v| v > 200))
        .map(|x| Struct_var_1 {
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            mul: x.int1.map(|v| v * 20),
        })
        .group_by(|x| (x.string1.clone()))
        .reduce(|a, b| {
            a.mul = a.mul.zip(b.mul).map(|(x, y)| x + y);
        })
        .map(|(k, x)| Struct_var_2 {
            string1: k.clone(),
            agg: x.mul,
        });
    var_2
        .map(|(k, v)| (Struct_collect { string1: k.clone() }, v))
        .drop_key()
        .write_csv_one("../out/noir-result.csv", true);
    File::create("../out/noir-result.csv").unwrap();
    tracing::info!("starting execution");
    ctx.execute_blocking();
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
