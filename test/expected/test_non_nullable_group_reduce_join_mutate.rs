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
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    fruit: String,
    agg2: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_3 {
    fruit: Option<String>,
    agg2: Option<i64>,
    fruit_right: Option<String>,
    weight: Option<i64>,
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    fruit: Option<String>,
    agg2: Option<i64>,
    fruit_right: Option<String>,
    weight: Option<i64>,
    price: Option<i64>,
    mut4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("../data/non_nullable_op/fruit_left.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_0 = var_0;
    let var_1 = ctx
        .stream_csv::<Struct_var_1>("../data/non_nullable_op/fruit_right.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_4 = var_1
        .group_by(|x| (x.fruit.clone()))
        .reduce(|a, b| a.weight = a.weight + b.weight)
        .map(|(k, x)| Struct_var_2 {
            fruit: k.clone(),
            agg2: Some(x.weight),
        })
        .join(var_0.group_by(|x| x.fruit.clone()))
        .map(|(_, x)| Struct_var_3 {
            fruit: Some(x.0.fruit),
            agg2: x.0.agg2,
            fruit_right: Some(x.1.fruit),
            weight: Some(x.1.weight),
            price: x.1.price,
        })
        .map(|(_, x)| Struct_var_4 {
            fruit: x.fruit,
            agg2: x.agg2,
            fruit_right: x.fruit_right,
            weight: x.weight,
            price: x.price,
            mut4: x.price.map(|v| v + 100),
        });
    var_4
        .map(|(k, v)| (Struct_collect { fruit: k.clone() }, v))
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
