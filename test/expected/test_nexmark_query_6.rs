use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fs::File;
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
    final_p: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    final_p: Option<i64>,
    seller: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_5 {
    seller: Option<i64>,
    avg_final_p: Option<f64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    seller: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-renoir-compiler/data/nexmark/bid.csv",
    );
    let var_0 = var_0;
    let var_1 = ctx.stream_csv::<Struct_var_1>(
        "/home/carlo/Projects/ibis-renoir-compiler/data/nexmark/auction.csv",
    );
    let var_5 = var_1
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
        .window(CountWindow::new(3, 1, true))
        .fold(
            Struct_var_3 {
                id: None,
                item_name: None,
                description: None,
                initial_bid: None,
                reserve: None,
                date_time: None,
                expires: None,
                seller: None,
                category: None,
                extra: None,
                auction: None,
                bidder: None,
                price: None,
                channel: None,
                url: None,
                date_time_right: None,
                extra_right: None,
                final_p: Some(0),
            },
            |acc, x| {
                acc.id = x.id;
                acc.item_name = x.item_name;
                acc.description = x.description;
                acc.initial_bid = x.initial_bid;
                acc.reserve = x.reserve;
                acc.date_time = x.date_time;
                acc.expires = x.expires;
                acc.seller = x.seller;
                acc.category = x.category;
                acc.extra = x.extra;
                acc.auction = x.auction;
                acc.bidder = x.bidder;
                acc.price = x.price;
                acc.channel = x.channel;
                acc.url = x.url;
                acc.date_time_right = x.date_time_right;
                acc.extra_right = x.extra_right;
                acc.final_p = acc.final_p.zip(x.price).map(|(a, b)| max(a, b));
            },
        )
        .map(|(_, x)| Struct_var_4 {
            final_p: x.final_p,
            seller: x.seller,
        })
        .drop_key()
        .group_by_avg(|x| (x.seller.clone()), |x| x.final_p.unwrap_or(0) as f64)
        .map(|(k, x)| Struct_var_5 {
            seller: k.clone(),
            avg_final_p: Some(x),
        });
    let out = var_5.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();
    let out = out.get().unwrap();
    let out = out
        .iter()
        .map(|(k, v)| (Struct_collect { seller: k.clone() }, v))
        .collect::<Vec<_>>();
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
