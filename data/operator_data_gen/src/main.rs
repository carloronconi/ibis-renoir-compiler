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
    #[arg(short, long, num_args=NUM_FILES, default_value=None)]
    headers: Option<Vec<String>>,
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

        let header = args.headers.as_ref()
            .map(|h| h[i].clone())
            .unwrap_or(
                type_.chars()
                    .enumerate()
                    .map(|(i, c)| 
                        match c {
                            'i' | 'I' => format!("int{}", i),
                            's' => format!("string{}", i),
                            _ => panic!("Invalid type character"),
                        })
                    .collect::<Vec<String>>()
                    .join(","));
        writeln!(writer, "{}", header)?;

        let seed = [0; 32];
        let mut rng_loop = StdRng::from_seed(seed);
        let mut rng_string = StdRng::from_seed(seed);
        let range = args.size / 4;

        // generate `range` unique strings
        let mut unique_strings = Vec::new();
        for _ in 0..range {
            let unique_str: String = (&mut rng_string)
                .sample_iter(&Alphanumeric).take(10)
                .map(char::from)
                .collect::<String>()
                .to_lowercase();
            unique_strings.push(unique_str);
        }

        // generate `range` unique ints
        let mut unique_ints = Vec::new();
        // always have 0 as one of the ints
        unique_ints.push(0);
        for _ in 1..range {
            let unique_int = rng_loop.gen_range(1..MAX);
            unique_ints.push(unique_int);
        }

        for _ in 0..args.size {
            let row = type_.chars()
                .map(|c|{ 
                    let index = rng_loop.gen_range(0..range);
                    match c {
                        'i' => {
                            unique_ints[index].to_string()
                        },
                        'I' => {
                            // interpret I type as nullable int: 0s are turned to blanks
                            let value = unique_ints[index];
                            if value == 0 {
                                "".to_owned()
                            } else {
                                value.to_string()
                            }
                        },
                        's' => {
                            // randomly select one of the pre-generated strings
                            unique_strings[index].clone()
                        },
                        _ => panic!("Invalid type character"),
                    }})
                .collect::<Vec<String>>();
            writeln!(writer, "{}", row.join(","))?;
        }
    }

    println!("Done generating nullable operators data.");
    Ok(())
}