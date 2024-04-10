use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_1 {
    int1: Option<i64>,
    string1: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_2 {
    string1: Option<String>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_2 = var_0
        .filter(|x| x.string1.clone().is_some_and(|v| v == "unduetre"))
        .filter(|x| x.int1.clone().is_some_and(|v| v == 123))
        .map(|x| Struct_var_1 {
            int1: x.int1,
            string1: x.string1,
        })
        .map(|x| Struct_var_2 { string1: x.string1 });
    let out = var_2.collect_vec();
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

    let ctx = StreamContext::default();

    tracing::info!("building graph");
    logic(ctx);

    tracing::info!("finished execution");

    Ok(())
}
