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
    int2: Option<i64>,
    int3: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_2 {
    agg2: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_3 {
    agg2: Option<i64>,
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Struct_var_4 {
    agg2: Option<i64>,
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    mut4: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_0 = var_0;
    let var_1 =
        ctx.stream_csv::<Struct_var_1>("/home/carlo/Projects/ibis-quickstart/data/int-3.csv");
    let var_4 = var_1
        .group_by(|x| x.int1.clone())
        .reduce(|a, b| {
            a.int2 = a.int2.zip(b.int2).map(|(x, y)| x + y);
        })
        .map(|(_, x)| Struct_var_2 { agg2: x.int2 })
        .join(var_0.group_by(|x| x.int1.clone()))
        .map(|(_, x)| Struct_var_3 {
            agg2: x.0.agg2,
            int1: x.1.int1,
            string1: x.1.string1,
            int4: x.1.int4,
        })
        .map(|(_, x)| Struct_var_4 {
            agg2: x.agg2,
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            mut4: x.int4.map(|v| v + 100),
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
