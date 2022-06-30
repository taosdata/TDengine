#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/R

git clone -b main --depth 1 git@github.com:taosdata/taos-connector-jdbc.git
cd taos-connector-jdbc
mvn clean package -DskipTests
jar_path=`find ./ -name taos-jdbcdriver-*-dist.jar`
echo jar_path=$jar_path
R -f connect_native.r --args $jar_path
# R -f connect_rest.r --args $jar_path # bug 14704

