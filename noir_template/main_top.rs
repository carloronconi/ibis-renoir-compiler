use renoir::prelude::*;
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::cmp::max;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
