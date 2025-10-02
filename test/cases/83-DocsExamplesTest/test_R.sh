#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/R
wget -N https://maven.aliyun.com/repository/central/com/taosdata/jdbc/taos-jdbcdriver/3.2.5/taos-jdbcdriver-3.2.5-dist.jar

jar_path=`find . -name taos-jdbcdriver-*-dist.jar`
echo jar_path=$jar_path
R -f connect_native.r --args $jar_path
# R -f connect_rest.r --args $jar_path # bug 14704

