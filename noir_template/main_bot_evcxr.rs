    File::create("../out/noir-result.csv").unwrap();
    println!("starting execution");
    ctx.execute_blocking();
    println!("finished execution");
}
