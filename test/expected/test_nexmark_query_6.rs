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
    seller: Option<i64>,
    final_p: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    id: Option<i64>,
    seller: Option<i64>,
    final_p: Option<i64>,
    grp_sum: Option<i64>,
    grp_count: Option<i64>,
    avg_final_p: Option<f64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    seller: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>("../data/nexmark/bid.csv");
    let var_0 = var_0;
    let var_1 = ctx.stream_csv::<Struct_var_1>("../data/nexmark/auction.csv");
    let var_4 = var_1
        .join(var_0, |x| x.id.clone(), |y| y.auction.clone())
        .map(|(_, x)| Struct_var_2 {
            id: x.0.id,
            item_name: x.0.item_name,
            description: x.0.description,
            initial_bid: x.0.initial_bid,
            reserve: x.0.reserve,
            date_time: x.0.date_time,
            expires: x.0.expires,
            seller: x.0.seller,
            category: x.0.category,
            extra: x.0.extra,
            auction: x.1.auction,
            bidder: x.1.bidder,
            price: x.1.price,
            channel: x.1.channel,
            url: x.1.url,
            date_time_right: x.1.date_time,
            extra_right: x.1.extra,
        })
        .filter(|(_, x)| {
            x.date_time_right
                .clone()
                .zip(x.expires.clone())
                .map_or(false, |(a, b)| a < b)
        })
        .filter(|(_, x)| x.expires.clone().is_some_and(|v| v < 2330277279926))
        .drop_key()
        .group_by(|x| (x.id.clone(), x.seller.clone()))
        .reduce(|a, b| {
            a.price = a.price.zip(b.price).map(|(x, y)| max(x, y));
        })
        .map(|(k, x)| Struct_var_3 {
            id: k.0,
            seller: k.1,
            final_p: x.price,
        })
        .drop_key()
        .group_by(|x| (x.seller.clone()))
        .window(CountWindow::new(10, 1, true))
        .fold(
            Struct_var_4 {
                id: None,
                seller: None,
                final_p: None,
                grp_sum: Some(0),
                grp_count: Some(0),
                avg_final_p: Some(0.0),
            },
            |acc, x| {
                acc.id = x.id;
                acc.seller = x.seller;
                acc.final_p = x.final_p;
                acc.grp_sum = acc.grp_sum.zip(x.final_p).map(|(a, b)| a + b);
                acc.grp_count = acc.grp_count.map(|v| v + 1);
            },
        )
        .map(|(_, x)| Struct_var_4 {
            avg_final_p: x.grp_sum.zip(x.grp_count).map(|(a, b)| a as f64 / b as f64),
            ..x
        });
    var_4
        .map(|(k, v)| (Struct_collect { seller: k.clone() }, v))
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
