use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Cols_ibis_read_csv_253sxydlhvcprdgabpz3fxdyqq {
    int1: i64,
    string1: String,
}

fn logic(ctx: &StreamContext) {
    let ibis_read_csv_253sxydlhvcprdgabpz3fxdyqq = ctx
        .stream_csv::<Cols_ibis_read_csv_253sxydlhvcprdgabpz3fxdyqq>("../data/int-1-string-1.csv");
    ibis_read_csv_253sxydlhvcprdgabpz3fxdyqq
        .filter(|x| x.string1 == "unduetre")
        .group_by(|x| x.string1.clone())
        .map(|x| x.1.int1 * 20)
        .reduce(|a, b| *a = (*a).max(b))
        .for_each(|x| println!("{x:?}"));
}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();
    tracing_subscriber::fmt::init();

    let ctx = StreamContext::default();

    tracing::info!("building graph");
    logic(&ctx);

    tracing::info!("starting execution");
    ctx.execute_blocking();
    tracing::info!("finished execution");

    Ok(())
}
