use mimalloc::MiMalloc;
use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    fruit: String,
    weight: i64,
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    fruit: String,
    weight: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("../data/non_nullable_op/fruit_left.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_2 = var_0
        .filter(|x| x.price.clone().is_some_and(|v| v > 3))
        .filter(|x| x.fruit == "Apple")
        .map(|x| Struct_var_1 {
            fruit: x.fruit,
            weight: x.weight,
        })
        .map(|x| Struct_var_2 { fruit: x.fruit });
    var_2.write_csv_one("../out/noir-result.csv", true);
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
