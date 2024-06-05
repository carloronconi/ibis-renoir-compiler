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
    int1_agg: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    int1_agg: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-quickstart/data/non_nullable_op/fruit_left.csv",
    );
    let var_2 = var_0
        .filter(|x| x.fruit == "Orange")
        .group_by(|x| (x.fruit.clone()))
        .reduce(|a, b| {
            a.price = a.price.zip(b.price).map(|(x, y)| x);
        })
        .map(|(k, x)| Struct_var_1 {
            fruit: k.clone(),
            int1_agg: x.price,
        })
        .map(|(_, x)| Struct_var_2 {
            int1_agg: x.int1_agg,
        });
    let out = var_2.collect_vec();
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
