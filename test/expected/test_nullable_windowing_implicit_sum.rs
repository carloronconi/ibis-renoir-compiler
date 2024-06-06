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
    int4_sum: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>("../data/nullable_op/ints_strings.csv");
    let var_1 = var_0.reduce_scan(
        |x| (x.int4.unwrap_or(0),),
        |(a_sum,), (b_sum,)| (a_sum + b_sum,),
        |x, (sum,)| Struct_var_1 {
            int1: x.int1,
            string1: x.string1,
            int4: x.int4,
            sum: *sum,
            int4_sum: Some(*sum),
        },
    );
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
