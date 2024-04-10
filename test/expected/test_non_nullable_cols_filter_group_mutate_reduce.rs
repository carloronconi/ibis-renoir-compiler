use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_0 {
    fruit: String,
    weight: i64,
    price: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_1 {
    fruit: String,
    weight: i64,
    price: Option<i64>,
    mul: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_2 {
    agg: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_collect {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 =
        ctx.stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/fruit.csv");
    let var_2 = var_0
        .filter(|x| x.weight > 4)
        .map(|x| Struct_var_1 {
            fruit: x.fruit,
            weight: x.weight,
            price: x.price,
            mul: x.price.map(|v| v * 20),
        })
        .group_by(|x| x.fruit.clone())
        .reduce(|a, b| {
            a.mul = a.mul.zip(b.mul).map(|(x, y)| x + y);
        })
        .map(|(_, x)| Struct_var_2 { agg: x.mul });
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

    let ctx = StreamContext::default();

    tracing::info!("building graph");
    logic(ctx);

    tracing::info!("finished execution");

    Ok(())
}
