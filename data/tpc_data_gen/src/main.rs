use std::path::Path;
use clap::Parser;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

#[derive(Parser)]
#[command(about, long_about=None)]
struct Args {
    dir: String,
    #[arg(short, long, default_value=".tbl")]
    pattern: String,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let dir = Path::new(&args.dir);
    
    let file_list: Vec<_> = std::fs::read_dir(dir)?
        .map(|e| e.unwrap().path().as_os_str().to_os_string().into_string().unwrap())
        .filter(|n| n.contains(&args.pattern))
        .collect();

    for f in &file_list {
        print!("Transforming file: {}\n", f);
        let new_file = f.replace(&args.pattern, ".csv");
        let original_file = File::open(f)?;
        let reader = BufReader::new(original_file);
        let mut new_file = File::create(new_file)?;

        for line in reader.lines() {
            let line = line?;
            let modified_line = line.replace(",", ";").replace("|", ",");
            let modified_line = modified_line[..modified_line.len() - 1].to_string();
            writeln!(new_file, "{}", modified_line)?;
        }
    }

    println!("Done generating data.");
    Ok(())
}