#!/bin/bash

git submodule update --init --recursive > /dev/null || echo -e "failed to update git submodule"

if [ ! -d debug ]; then
    mkdir debug || echo -e "failed to make directory for build"
fi

cd debug && cmake .. -DBUILD_TOOLS=true && make

