#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/R
wget https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.2.4/taos-jdbcdriver-3.2.4-dist.jar

jar_path=`find . -name taos-jdbcdriver-*-dist.jar`
echo jar_path=$jar_path
R -f connect_native.r --args $jar_path
# R -f connect_rest.r --args $jar_path # bug 14704

