use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    group_sum: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    group_sum: Option<i64>,
    group_percent: Option<f64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("/home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv");
    let var_2 = var_0
        .window_all(CountWindow::new(2, 1, true))
        .fold(
            Struct_var_1 {
                int1: None,
                string1: None,
                int4: None,
                group_sum: Some(0),
            },
            |acc, x| {
                acc.int1 = x.int1;
                acc.string1 = x.string1;
                acc.int4 = x.int4;
                acc.group_sum = acc.group_sum.zip(x.int4).map(|(a, b)| a + b);
            },
        )
        .drop_key()
        .map(|x| Struct_var_2 {
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            group_sum: x.group_sum,
            group_percent: x
                .int4
                .map(|v| v * 100)
                .zip(x.group_sum)
                .map(|(a, b)| a as f64 / b as f64),
        });
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

    let ctx = StreamContext::new_local();

    tracing::info!("building graph");
    logic(ctx);

    tracing::info!("finished execution");

    Ok(())
}
