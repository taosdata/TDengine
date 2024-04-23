#!/bin/bash

set -e

taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &

sleep 10

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
python3 multi_bind_example.py

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
pip install SQLAlchemy
pip install pandas
taosBenchmark -y -d power -t 10 -n 10
python3 conn_native_pandas.py
python3 conn_rest_pandas.py
taos -s "drop database if exists power"

# 11
taos -s "create database if not exists test wal_retention_period 3600"
python3 connect_native_reference.py

# 12
python3 connect_rest_examples.py

# 13
python3 handle_exception.py

# 14
taosBenchmark -y -d power -t 2 -n 10
python3 rest_client_example.py
taos -s "drop database if exists power"

# 15
python3 result_set_examples.py

# 16
python3 tmq_example.py

# 17
python3 sql_writer.py

# 18
python3 mockdatasource.py

# 19
python3 fast_write_example.py

# 20
pip3 install kafka-python
python3 kafka_example_consumer.py

# 21
pip3 install taos-ws-py==0.3.1
python3 conn_websocket_pandas.py

# 22
python3 connect_websocket_examples.py
