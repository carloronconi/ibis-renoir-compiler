use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;
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
    sum: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    fruit: String,
    weight: i64,
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_3 {
    fruit: String,
    weight: i64,
    price: Option<i64>,
    mul: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    fruit: Option<String>,
    weight: Option<i64>,
    price: Option<i64>,
    mul: Option<i64>,
    fruit_right: Option<String>,
    weight_right: Option<i64>,
    price_right: Option<i64>,
    sum: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_5 {
    fruit: Option<String>,
    weight: Option<i64>,
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-renoir-compiler/data/non_nullable_op/fruit_right.csv",
    );
    let var_1 = var_0.map(|x| Struct_var_1 {
        fruit: x.fruit,
        weight: x.weight,
        price: x.price,
        sum: x.price.map(|v| v + 100),
    });
    let var_2 = ctx.stream_csv::<Struct_var_2>(
        "/home/carlo/Projects/ibis-renoir-compiler/data/non_nullable_op/fruit_left.csv",
    );
    let var_5 = var_2
        .filter(|x| x.weight > 2)
        .map(|x| Struct_var_3 {
            fruit: x.fruit,
            weight: x.weight,
            price: x.price,
            mul: x.price.map(|v| v + 10),
        })
        .join(var_1, |x| x.fruit.clone(), |y| y.fruit.clone())
        .map(|(_, x)| Struct_var_4 {
            fruit: Some(x.0.fruit),
            weight: Some(x.0.weight),
            price: x.0.price,
            mul: x.0.mul,
            fruit_right: Some(x.1.fruit),
            weight_right: Some(x.1.weight),
            price_right: x.1.price,
            sum: x.1.sum,
        })
        .map(|(_, x)| Struct_var_5 {
            fruit: x.fruit,
            weight: x.weight,
            price: x.price,
        });
    let out = var_5.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();
    let out = out.get().unwrap();
    let out = out
        .iter()
        .map(|(k, v)| (Struct_collect { fruit: k.clone() }, v))
        .collect::<Vec<_>>();
    let file = File::create("../out/noir-result.csv").unwrap();
    let mut wtr = csv::WriterBuilder::new().from_writer(file);

    for e in out {
        wtr.serialize(e).unwrap();
    }
    wtr.flush().unwrap();
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
