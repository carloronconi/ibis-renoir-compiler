#!/bin/bash
rustup component add rust-src
cargo install --locked evcxr_repl
# important! only way to cache dependencies
export EVCXR_TMPDIR="/home/$USER/evcxr_temp/"