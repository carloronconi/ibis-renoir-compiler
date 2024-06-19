#!/bin/bash
for i in $(cat requirements.txt); do
    clean="$( cut -d '=' -f 1 <<< "$i" )"
    pip install $clean
done  