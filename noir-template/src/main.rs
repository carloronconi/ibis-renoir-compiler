use noir_compute::prelude::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Cols_ibis_read_csv_vox7lkozu5hntgfyp2sju3a64m {
    int1: i64,
    string1: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
struct Cols_ibis_read_csv_nzrjux2bfzcgxcenitdyfnggvq {
    int1: i64,
    int2: i64,
    int3: i64,
}

fn logic(ctx: &StreamContext) {
    let ibis_read_csv_vox7lkozu5hntgfyp2sju3a64m = ctx
        .stream_csv::<Cols_ibis_read_csv_vox7lkozu5hntgfyp2sju3a64m>("../data/int-1-string-1.csv");
    let ibis_read_csv_nzrjux2bfzcgxcenitdyfnggvq =
        ctx.stream_csv::<Cols_ibis_read_csv_nzrjux2bfzcgxcenitdyfnggvq>("../data/int-3.csv");
    ibis_read_csv_vox7lkozu5hntgfyp2sju3a64m
        .join(
            ibis_read_csv_nzrjux2bfzcgxcenitdyfnggvq,
            |x| x.int1,
            |y| y.int1,
        )
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
