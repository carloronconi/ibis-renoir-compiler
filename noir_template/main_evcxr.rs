// everything is already imported in evcxr
// use mimalloc::MiMalloc;
// use renoir::{prelude::*, Stream};
// use serde::{Deserialize, Serialize};
// use std::cmp::max;
// use std::fs::File;
// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_0 {
    int1: Option<i64>,
    string1: Option<String>,
    int4: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_1 {
    int1: Option<i64>,
    string1: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]
struct Struct_var_2 {
    string1: Option<String>,
}

// changed signature, cache() function always returns cache for all tables, while here we only have the tables we need
fn logic(data_nullable_op_ints_strings: StreamCache<Struct_data_nullable_op_ints_strings>,) {

    // added: always create context from first of tables
    let ctx = StreamContext::new(data_nullable_op_ints_strings.config());
    
    // changed
    let var_0 = data_nullable_op_ints_strings.stream_in(&ctx);

    let var_2 = var_0
        .filter(|x| x.int1.clone().is_some_and(|v| v == 123))
        .filter(|x| x.string1.clone().is_some_and(|v| v == "unduetre"))
        .map(|x| Struct_var_1 {
            int1: x.int1,
            string1: x.string1,
        })
        .map(|x| Struct_var_2 { string1: x.string1 });
    var_2.write_csv_one("/home/carlo/Projects/ibis-renoir-compiler/out/noir-result.csv", true);
    File::create("/home/carlo/Projects/ibis-renoir-compiler/out/noir-result.csv").unwrap();
    // tracing::info!("starting execution");
    println!("starting execution");
    
    ctx.execute_blocking();
    
    // added
    println!("finished execution");
}

// fn main() -> eyre::Result<()> {
    // color_eyre::install().ok();
    // tracing_subscriber::fmt::init();

    // let ctx = StreamContext::new_local();
    // tracing::info!("building graph");
    // logic(ctx);

    // tracing::info!("finished execution");
    // Ok(())
// }
