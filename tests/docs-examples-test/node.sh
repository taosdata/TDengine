#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/node

npm install
cd restexample;

node connect.js

cd ../nativeexample

node connect.js

taos -s "drop database if exists power"
node insert_example.js

node query_example.js 

node async_query_example.js

# node subscribe_demo.js

taos -s "drop topic if exists topic_name_example"
taos -s "drop database if exists power"
node param_bind_example.js 

taos -s "drop database if exists power"
node multi_bind_example.js

taos -s "drop database if exists test"
node influxdb_line_example.js

taos -s "drop database if exists test"
node opentsdb_telnet_example.js

taos -s "drop database if exists test"
node opentsdb_json_example.js
