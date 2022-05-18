#!/bin/bash

git submodule update --init --recursive > /dev/null || echo -e "failed to update git submodule"

if [ ! -d debug ]; then
    mkdir debug || echo -e "failed to make directory build"
fi

cd build && cmake .. -DBUILD_TOOLS=true && make

