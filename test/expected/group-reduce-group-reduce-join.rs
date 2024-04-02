use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_0 {
    int1: i64,
    string1: String,
    int4: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_1 {
    agg4: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_2 {
    int1: i64,
    int2: i64,
    int3: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_3 {
    agg2: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Struct_var_4 {
    agg2: i64,
    agg4: i64,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_1 = var_0
        .group_by(|x| x.int1.clone())
        .reduce(|a, b| a.int4 = a.int4 + b.int4)
        .map(|(_, x)| Struct_var_1 { agg4: x.int4 });
    let var_2 =
        ctx.stream_csv::<Struct_var_2>("/home/carlo/Projects/ibis-quickstart/data/int-3.csv");
    let var_4 = var_2
        .group_by(|x| x.int1.clone())
        .reduce(|a, b| a.int2 = a.int2 + b.int2)
        .map(|(_, x)| Struct_var_3 { agg2: x.int2 })
        .join(var_1)
        .map(|(_, x)| Struct_var_4 {
            agg2: x.0.agg2,
            agg4: x.1.agg4,
        });
    let out = var_4.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();

    let out = out.get().unwrap();
    let file = File::create("../out/noir-result.csv").unwrap();
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

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
