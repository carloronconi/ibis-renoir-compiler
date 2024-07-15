#!/bin/bash
rustup component add rust-src
cargo install --locked evcxr_repl
# important! only way to cache dependencies
# also start evcxr and pass the command :cache 5000
export EVCXR_TMPDIR="/home/$USER/evcxr_temp/"