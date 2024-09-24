#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/node

npm install

cd websocketet

node all_type_query.js

node all_type_stmt.js

node json_line_example.js

node line_example.js

node nodejsChecker.js

node sql_example.js

node stmt_example.js

node telnet_line_example.js

node tmq_example.js

node tmq_seek_example.js