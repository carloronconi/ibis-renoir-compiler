use nexmark::config::NexmarkConfig;

use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::env;

use serde::Serialize;
use nexmark::event::*;

fn csv_writer(path: impl AsRef<Path>) -> csv::Writer<impl Write> {
    let file = File::create(path).unwrap();
    let writer = BufWriter::new(file);
    csv::WriterBuilder::new().from_writer(writer)
}

fn main() -> eyre::Result<()> {
    let conf = NexmarkConfig {
        num_event_generators: 1,
        first_rate: 10_000_000,
        next_rate: 10_000_000,
        ..Default::default()
    };

    let size = env::args().collect::<Vec<_>>()[1].parse::<usize>().unwrap();
    let mut bid = csv_writer(format!("../nexmark/bid_{}.csv", size));
    let mut person = csv_writer(format!("../nexmark/person_{}.csv", size));
    let mut auction = csv_writer(format!("../nexmark/auction_{}.csv", size));

    for e in nexmark::EventGenerator::new(conf).take(size) {
        match e {
            Event::Person(p) => {
                person.serialize(p)?;
            }
            Event::Auction(a) => {
                auction.serialize(a)?;
            }
            Event::Bid(b) => {
                bid.serialize(b)?;
            }
        }
    }

    Ok(())
}