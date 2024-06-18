use mimalloc::MiMalloc;
use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    int2: Option<i64>,
    int3: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
    int1_right: Option<i64>,
    int2: Option<i64>,
    int3: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    int1: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("../data/nullable_op/many_ints.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_0 = var_0;
    let var_1 = ctx
        .stream_csv::<Struct_var_1>("../data/nullable_op/ints_strings.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_2 = var_1
        .left_join(var_0, |x| x.int1.clone(), |y| y.int1.clone())
        .map(|(_, x)| {
            let mut v = Struct_var_2 {
                int1: x.0.int1,
                string1: x.0.string1,
                int4: x.0.int4,
                int1_right: None,
                int2: None,
                int3: None,
            };
            if let Some(i) = x.1 {
                v.int1_right = i.int1;
                v.int2 = i.int2;
                v.int3 = i.int3;
            };
            v
        });
    var_2
        .map(|(k, v)| (Struct_collect { int1: k.clone() }, v))
        .drop_key()
        .write_csv_one("../out/noir-result.csv", true);
    File::create("../out/noir-result.csv").unwrap();
    tracing::info!("starting execution");
    ctx.execute_blocking();
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
