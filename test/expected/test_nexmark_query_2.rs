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
    auction: Option<i64>,
    price: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx
        .stream_csv::<Struct_var_0>("../data/nexmark/bid.csv")
        .batch_mode(BatchMode::fixed(16000));
    let var_1 = var_0
        .filter(|x| {
            x.auction.clone().is_some_and(|v| v == 1007)
                | x.auction.clone().is_some_and(|v| v == 1020)
                | x.auction.clone().is_some_and(|v| v == 2001)
                | x.auction.clone().is_some_and(|v| v == 2019)
                | x.auction.clone().is_some_and(|v| v == 2087)
        })
        .map(|x| Struct_var_1 {
            auction: x.auction,
            price: x.price,
        });
    var_1.write_csv_one("../out/noir-result.csv", true);
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
