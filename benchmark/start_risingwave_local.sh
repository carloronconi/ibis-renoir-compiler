#!/bin/bash

# download official risingwave install script
if [ ! -f risingwave ]; then
    curl https://risingwave.com/sh | sh
fi

# in case there is an error with libssl1.1, download and install it manually
# https://stackoverflow.com/a/73603200
# for sola, this wasn't required
# wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
# sudo dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb

# to run risingwave, just do `./risingwave`

# check if directory risingwave_state_dir exists: if it does, delete it to start fresh
if [ -d risingwave_state_dir ]; then
    rm -rf risingwave_state_dir
fi

mkdir risingwave_state_dir
./risingwave --store-directory risingwave_state_dir