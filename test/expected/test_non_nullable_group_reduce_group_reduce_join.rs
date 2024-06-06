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
    fruit: String,
    agg2: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    fruit: Option<String>,
    agg2: Option<i64>,
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
        .reduce(|a, b| a.weight = a.weight + b.weight)
        .map(|(k, x)| Struct_var_1 {
            fruit: k.clone(),
            agg4: Some(x.weight),
        });
    let var_2 = ctx.stream_csv::<Struct_var_2>("../data/non_nullable_op/fruit_right.csv");
    let var_4 = var_2
        .group_by(|x| (x.fruit.clone()))
        .reduce(|a, b| {
            a.price = a.price.zip(b.price).map(|(x, y)| x + y);
        })
        .map(|(k, x)| Struct_var_3 {
            fruit: k.clone(),
            agg2: x.price,
        })
        .join(var_1)
        .map(|(_, x)| Struct_var_4 {
            fruit: Some(x.0.fruit),
            agg2: x.0.agg2,
            fruit_right: Some(x.1.fruit),
            agg4: x.1.agg4,
        });
    let out = var_4.collect_vec();
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
