use noir_compute::prelude::*;

fn logic(ctx: &StreamContext) {
    ctx.stream_csv::<(i32, String)>("../int-1-string-1.csv")
        .filter(|x| x.1 == "unduetre")
        .group_by(|x| x.1.clone())
        .map(|x| x.1)
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
