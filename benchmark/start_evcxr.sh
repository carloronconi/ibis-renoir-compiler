#!/bin/bash
rustup component add rust-src
cargo install --locked evcxr_repl
# important! only way to cache dependencies
export EVCXR_TMPDIR="/home/$USER/evcxr_temp/"
# also start evcxr and pass the command :cache 5000 and then manually copy and paste init.evcxr into evcxr
# then check :last_compile_dir to see it's the one set with the env var.
# sometimes it doesn't run there, probably when there's a evcxr instance already running that hasn't been
# killed correctly
