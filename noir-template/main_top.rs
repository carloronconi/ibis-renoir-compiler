use noir_compute::prelude::*;

fn logic(ctx: &StreamContext) {
    ctx.stream_csv::<(i32, String)>("../codegen/int-1-string-1.csv")