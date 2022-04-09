#!/bin/bash

set -e

taosd >> /dev/null 2>&1 &
taosadapter >> /dev/null 2>&1 &
cd ../../docs-examples/node

node connect.js

node insert_example.js

node query_example.js 

taos -s "drop database if exists power"
node param_bind_example.js 

taos -s "drop database if exists power"
node multi_bind_example.js

node influxdb_line_example.js

taos -s "drop database if exists test"
node opentsdb_telnet_example.js

taos -s "drop database if exists test"
node opentsdb_json_example.js






