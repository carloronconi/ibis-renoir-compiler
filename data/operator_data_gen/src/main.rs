use std::borrow::BorrowMut;
use std::fs::File;
use std::i32::MAX;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use clap::Parser;

static NUM_FILES: usize = 2;

#[derive(Parser)]
#[command(about, long_about = None)]
struct Args {
    // #[arg(short, long, default_value = "shuttle")]
    size: usize,
    parent: String,
    #[arg(short, long, num_args=NUM_FILES)]
    names: Vec<String>,
    #[arg(short, long, num_args=NUM_FILES)]
    types: Vec<String>,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();

    for i in 0..NUM_FILES {
        let name = args.names[i].clone();
        let type_ = args.types[i].clone();

        let file_name = format!("{}_{}.csv", name, args.size);
        let path = Path::new("../").join(args.parent.clone()).join(file_name);
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        let header = type_.chars()
            .enumerate()
            .map(|(i, c)| 
                match c {
                    'i' => format!("int{}", i),
                    's' => format!("string{}", i),
                    _ => panic!("Invalid type character"),
                })
            .collect::<Vec<String>>();
        writeln!(writer, "{}", header.join(","))?;

        let seed = [0; 32];
        let mut rng = StdRng::from_seed(seed);
        for _ in 0..args.size {
            let row = type_.chars()
                .map(|c| 
                    match c {
                        'i' => rng.gen_range(0..MAX).to_string(),
                        's' => rng.borrow_mut().sample_iter(&Alphanumeric).take(10).map(char::from).collect(),
                        _ => panic!("Invalid type character"),
                    })
                .collect::<Vec<String>>();
            writeln!(writer, "{}", row.join(","))?;
        }
    }

    println!("Done generating nullable operators data.");
    Ok(())
}