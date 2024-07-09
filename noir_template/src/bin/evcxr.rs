use mimalloc::MiMalloc;
use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    auction: Option<i64>,
    bidder: Option<i64>,
    price: Option<i64>,
    channel: Option<String>,
    url: Option<String>,
    date_time: Option<i64>,
    extra: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    id: Option<i64>,
    item_name: Option<String>,
    description: Option<String>,
    initial_bid: Option<i64>,
    reserve: Option<i64>,
    date_time: Option<i64>,
    expires: Option<i64>,
    seller: Option<i64>,
    category: Option<i64>,
    extra: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    id: Option<i64>,
    item_name: Option<String>,
    description: Option<String>,
    initial_bid: Option<i64>,
    reserve: Option<i64>,
    date_time: Option<i64>,
    expires: Option<i64>,
    seller: Option<i64>,
    category: Option<i64>,
    extra: Option<String>,
    auction: Option<i64>,
    bidder: Option<i64>,
    price: Option<i64>,
    channel: Option<String>,
    url: Option<String>,
    date_time_right: Option<i64>,
    extra_right: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_3 {
    id: Option<i64>,
    category: Option<i64>,
    final_p: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    category: Option<i64>,
    avg_final_p: Option<f64>,
}

fn logic(ctx: StreamContext) {
    let (var_0_cache, _) = ctx
        .stream_csv::<Struct_var_0>("../data/nexmark/bid.csv")
        .batch_mode(BatchMode::fixed(16000))
        .cache();
    // let var_0 = var_0;
    let (var_1_cache, _) = ctx
        .stream_csv::<Struct_var_1>("../data/nexmark/auction.csv")
        .batch_mode(BatchMode::fixed(16000))
        .cache();

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
