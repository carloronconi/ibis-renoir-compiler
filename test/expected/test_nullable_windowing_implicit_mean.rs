use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
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
    sum: i64,
    count: i64,
    int4_mean: Option<f64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    sum: i64,
    count: i64,
    int4_mean: Option<f64>,
    int4_demean: Option<f64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-quickstart/data/nullable_op/ints_strings.csv",
    );
    let var_2 = var_0
        .reduce_scan(
            |x| (x.int4.unwrap_or(0), 1),
            |(a_sum, a_count), (b_sum, b_count)| (a_sum + b_sum, a_count + b_count),
            |x, (sum, count)| Struct_var_1 {
                int1: x.int1,
                string1: x.string1,
                int4: x.int4,
                sum: *sum,
                count: *count,
                int4_mean: Some(*sum as f64 / *count as f64),
            },
        )
        .map(|x| Struct_var_2 {
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            sum: x.sum,
            count: x.count,
            int4_mean: x.int4_mean,
            int4_demean: x.int4.zip(x.int4_mean).map(|(a, b)| a as f64 - b as f64),
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
