#!/bin/bash
# ARM64 TDengine OSS Build Script
#
# The -DTD_PTHREAD_TWEAK:BOOL=ON flag is required because libuv (a TDengine dependency)
# uses pthread symbols in certain code paths. Even though pthread is available on the
# system, the linker needs this flag to explicitly include -lpthread when linking the
# transport module, ensuring all pthread symbols are properly resolved.
#
# This is not just a legacy workaround for CentOS 7.9/Ubuntu 18, but a necessary
# dependency of the libuv library used by TDengine.

docker run --rm -v ~/.:/mnt tsdb-builder-v2:arm64 bash -c "cd /mnt/TDengine && ./build.sh gen -DTD_PTHREAD_TWEAK:BOOL=ON && ./build.sh bld"

