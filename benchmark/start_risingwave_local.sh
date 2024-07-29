#!/bin/bash

# download official risingwave install script
curl https://risingwave.com/sh | sh
# to run risingwave, just do `./risingwave`

# in case there is an error with libssl1.1, download and install it manually
# https://stackoverflow.com/a/73603200
# for debian, not sure what to do
wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
sudo dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb
