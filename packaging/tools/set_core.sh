#!/bin/bash
#
# This file is used to set config for core when taosd crash

set -e
# set -x

csudo=""
if command -v sudo > /dev/null; then
  csudo="sudo"
fi

#ulimit -c unlimited
${csudo} sed -i '/ulimit -c unlimited/d' /etc/profile ||:
${csudo} sed -i '$a\ulimit -c unlimited' /etc/profile ||:
source /etc/profile

${csudo} mkdir -p /coredump  ||:
${csudo} sysctl -w kernel.core_pattern='/coredump/core-%e-%p'  ||:
${csudo} echo '/coredump/core-%e-%p' | ${csudo} tee /proc/sys/kernel/core_pattern  ||:
