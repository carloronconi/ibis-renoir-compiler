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
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    fruit: String,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-renoir-compiler/data/non_nullable_op/fruit_left.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_2 = var_0
        .filter(|x| x.price.clone().is_some_and(|v| v > 3))
        .filter(|x| x.fruit == "Apple")
        .map(|x| Struct_var_1 {
            fruit: x.fruit,
            weight: x.weight,
        })
        .map(|x| Struct_var_2 { fruit: x.fruit })
        .collect_vec();
    println!("starting execution");
    ctx.execute_blocking();
    for ele in var_2.get().unwrap() {
        println!("{:?}", ele);
    }
}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();

    let ctx = StreamContext::new_local();

    println!("building graph");
    logic(ctx);

    println!("finished execution");

    Ok(())
}
