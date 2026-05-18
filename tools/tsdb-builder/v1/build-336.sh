#!/bin/bash
# AMD64 /data/tsdb Build Script
#
# Passes cmake thread detection variables explicitly because CMake's FindThreads
# module fails to auto-detect pthreads on manylinux2014 (tries -lpthreads which
# does not exist; the correct flag is -lpthread).

docker run --rm \
  -v /data/tsdb:/mnt \
  tsdb-builder:amd64 bash -c "
rm -rf /mnt/debug /mnt/.externals && mkdir -p /mnt/debug && \
cd /mnt/debug && \
cmake .. \
    -DBUILD_ENTERPRISE=ON \
    -DBUILD_ENGINE=ON \
    -DBUILD_ADAPTER=ON \
    -DBUILD_KEEPER=ON \
    -DBUILD_TOOLS=ON \
    -DBUILD_GEN=ON \
    -DBUILD_TAOSX=OFF \
    -DBUILD_INSIGHT=ON \
    && \
make -j 2>&1 | tee /mnt/build.log; \
tail -100 /mnt/build.log"
