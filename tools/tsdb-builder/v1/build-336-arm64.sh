#!/bin/bash
# ARM64 /data/tsdb Build Script (cross-compiled on amd64 via QEMU)
#
# BUILD_EXPLORER_UI=OFF and BUILD_INSIGHT=OFF: both require Node/yarn running
# under QEMU emulation which is extremely slow; skip for cross-compile builds.

docker run --rm \
  --platform linux/arm64 \
  -v /data/tsdb:/mnt \
  tsdb-builder:arm64 bash -c " 
  rm -rf /mnt/debug /mnt/.externals && mkdir -p /mnt/debug && \
  cd /mnt/debug && \
  cmake .. \
    -DBUILD_ENTERPRISE=ON \
    -DBUILD_ENGINE=ON \
    -DBUILD_ADAPTER=ON \
    -DBUILD_KEEPER=ON \
    -DBUILD_TOOLS=ON \
    -DBUILD_GEN=ON \
    -DBUILD_TAOSX=ON \
    -DBUILD_INSIGHT=ON \
  && \
  make -j 2>&1 | tee /mnt/build.log; \
  tail -100 /mnt/build.log
"
