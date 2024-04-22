use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::{cmp::max, fs::File};
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
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
struct Struct_var_1 {
    auction: Option<i64>,
    bidder: Option<i64>,
    price: Option<i64>,
    channel: Option<String>,
    url: Option<String>,
    date_time: Option<i64>,
    extra: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    auction: Option<i64>,
    bidder: Option<i64>,
    price: Option<i64>,
    channel: Option<String>,
    url: Option<String>,
    date_time: Option<i64>,
    extra: Option<String>,
    id: Option<i64>,
    item_name: Option<String>,
    description: Option<String>,
    initial_bid: Option<i64>,
    reserve: Option<i64>,
    date_time_right: Option<i64>,
    expires: Option<i64>,
    seller: Option<i64>,
    category: Option<i64>,
    extra_right: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_3 {
    final_p: Option<i64>,

    // modified: added key contents
    category: Option<i64>,
    id: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_4 {
    avg_final_p: Option<f64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_collect {
    category: Option<i64>,
}

fn logic(ctx: StreamContext) {
    let var_0 = ctx.stream_csv::<Struct_var_0>(
        "/home/carlo/Projects/ibis-quickstart/data/nexmark/auction.csv",
    );
    let var_0 = var_0;
    let var_1 =
        ctx.stream_csv::<Struct_var_1>("/home/carlo/Projects/ibis-quickstart/data/nexmark/bid.csv");
    let var_4 = var_1
        .join(var_0, |x| x.auction.clone(), |y| y.id.clone())
        .map(|(_, x)| Struct_var_2 {
            auction: x.0.auction,
            bidder: x.0.bidder,
            price: x.0.price,
            channel: x.0.channel,
            url: x.0.url,
            date_time: x.0.date_time,
            extra: x.0.extra,
            id: x.1.id,
            item_name: x.1.item_name,
            description: x.1.description,
            initial_bid: x.1.initial_bid,
            reserve: x.1.reserve,
            date_time_right: x.1.date_time,
            expires: x.1.expires,
            seller: x.1.seller,
            category: x.1.category,
            extra_right: x.1.extra,
        })
        .filter(|(_, x)| x.expires.clone().is_some_and(|v| v < 2330277279926))

        // added: can drop because join preserves keys inside the struct
        .drop_key()

        // modified: tuple group by wasn't generated don't know why instead got two group_bys
        // store tuple into struct so can refer to names later!
        .group_by(|x| (x.id.clone(), x.category.clone()))
        //.group_by(|x| x.category.clone())
        .reduce(|a, b| {
            a.price = a.price.zip(b.price).map(|(x, y)| max(x, y));
        })
        // modified: map should also store keys into the struct so we can drop key always
        .map(|(k, x)| Struct_var_3 { final_p: x.price,  id: k.id, category: k.category})
        // added: drop key
        .drop_key()

        // again, here move group_by contents into struct and drop key (didn't do it...)
        .group_by_avg(|x| (x.category.clone()), |x| x.final_p.unwrap_or(0) as f64)
        .map(|(_, x)| Struct_var_4 {
            avg_final_p: x.final_p,
        });
    let out = var_4.collect_vec();
    tracing::info!("starting execution");
    ctx.execute_blocking();
    let out = out.get().unwrap();
    let out = out
        .iter()
        .map(|(k, v)| {
            (
                Struct_collect {
                    category: k.clone(),
                },
                v,
            )
        })
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
