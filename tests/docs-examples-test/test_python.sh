#!/bin/bash

set -e

taosd >> /dev/null 2>&1 &
taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/python

# 1
taos -s "create database if not exists log"
python3 connect_example.py

# 2
taos -s "drop database if exists power"
python3 native_insert_example.py

# 3
taos -s "drop database power"
python3 bind_param_example.py

# 4
taos -s "drop database power"
python3  multi_bind_example.py

# 5
python3 query_example.py

# 6
python3 async_query_example.py

# 7
taos -s "drop database if exists test"
python3 line_protocol_example.py

# 8
taos -s "drop database test"
python3 telnet_line_protocol_example.py

# 9
taos -s "drop database test"
python3 json_protocol_example.py

# 10
# python3 subscribe_demo.py


