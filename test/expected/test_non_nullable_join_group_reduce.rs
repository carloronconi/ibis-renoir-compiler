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
    agg4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    fruit: String,
    weight: i64,
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_3 {
    fruit: Option<String>,
    weight: Option<i64>,
    price: Option<i64>,
    fruit_right: Option<String>,
    agg4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>("../data/non_nullable_op/fruit_left.csv");
    let var_1 = var_0
        .group_by(|x| (x.fruit.clone()))
        .reduce(|a, b| {
            a.price = a.price.zip(b.price).map(|(x, y)| x + y);
        })
        .map(|(k, x)| Struct_var_1 {
            fruit: k.clone(),
            agg4: x.price,
        });
    let var_2 = ctx.stream_csv::<Struct_var_2>("../data/non_nullable_op/fruit_right.csv");
    let var_3 = var_2
        .group_by(|x| x.fruit.clone())
        .join(var_1)
        .map(|(_, x)| Struct_var_3 {
            fruit: Some(x.0.fruit),
            weight: Some(x.0.weight),
            price: x.0.price,
            fruit_right: Some(x.1.fruit),
            agg4: x.1.agg4,
        });
    var_3
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
