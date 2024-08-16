#!/bin/bash
docker run -itd --pull=always -p 4566:4566 -p 5691:5691 --add-host host.docker.internal:host-gateway risingwavelabs/risingwave:latest single_node