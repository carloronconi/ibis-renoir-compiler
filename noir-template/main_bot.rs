    let file = File::create("../out/noir-result.csv").unwrap();
    let mut wtr = csv::WriterBuilder::new()
        .from_writer(file);

    for e in out {
        wtr.serialize(e).unwrap();
    }
    wtr.flush().unwrap();

}

fn main() -> eyre::Result<()> {
    color_eyre::install().ok();
    tracing_subscriber::fmt::init();

    let ctx = StreamContext::default();

    tracing::info!("building graph");
    logic(ctx);

    tracing::info!("finished execution");

    Ok(())
}