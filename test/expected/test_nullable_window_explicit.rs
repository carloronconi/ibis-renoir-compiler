use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_new {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    sum: Option<i64>,
    perc: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_1 {
    sum: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_1 = var_0
        .group_by(|x| x.string1.clone())
        .window(CountWindow::new(2, 1, true))
        .fold(Struct_var_new{
                int1: None,
                string1: None,
                int4: None,
                sum: Some(0),
                perc: Some(0),
            }, |acc, x| {
                acc.int1 = x.int1;
                acc.string1 = x.string1;
                acc.int4 = x.int4;
                acc.sum = acc.sum.zip(x.int4).map(|(x, y)| x + y);
                acc.perc = acc.int4.zip(acc.sum).map(|(x, y)| x * 100 / y);
        }).drop_key();



    let out = var_1.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();
    let out = out.get().unwrap();
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
