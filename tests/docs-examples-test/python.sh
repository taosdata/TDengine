#!/bin/bash

set -e

check_transactions() {
    for i in {1..30}
    do
        output=$(taos -s "show transactions;")
        if [[ $output == *"Query OK, 0 row(s)"* ]]; then
            echo "Success: No transactions are in progress."
            return 0
        fi
        sleep 1
    done

    echo "Error: Transactions are still in progress after 30 attempts."
    return 1
}

reset_cache() {
  response=$(curl --location -uroot:taosdata 'http://127.0.0.1:6041/rest/sql' --data 'reset query cache')

  if [[ $response == \{\"code\":0* ]]; then
    echo "Success: Query cache reset successfully."
  else
    echo "Error: Failed to reset query cache. Response: $response"
    return 1
  fi
}



taosd >>/dev/null 2>&1 &
taosadapter >>/dev/null 2>&1 &

sleep 10

cd ../../docs/examples/python

# 1
taos -s "create database if not exists log"
check_transactions || exit 1
reset_cache || exit 1
python3 connect_example.py

# 2
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
python3 native_insert_example.py

# 3
taos -s "drop database power"
check_transactions || exit 1
reset_cache || exit 1
python3 bind_param_example.py

# 4
taos -s "drop database power"
check_transactions || exit 1
reset_cache || exit 1
python3 multi_bind_example.py

# 5
python3 query_example.py

# 6
python3 async_query_example.py

# 7
taos -s "drop database if exists test"
check_transactions || exit 1
reset_cache || exit 1
python3 line_protocol_example.py

# 8
taos -s "drop database test"
check_transactions || exit 1
reset_cache || exit 1
python3 telnet_line_protocol_example.py

# 9
taos -s "drop database test"
check_transactions || exit 1
reset_cache || exit 1
python3 json_protocol_example.py

# 10
pip install SQLAlchemy
pip install pandas
taosBenchmark -y -d power -t 10 -n 10
check_transactions || exit 1
reset_cache || exit 1
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
pip3 install taos-ws-py==0.3.8
python3 conn_websocket_pandas.py

# 22
python3 connect_websocket_examples.py

# 23
python3 create_db_ws.py

# 24
python3 create_db_native.py

# 25
python3 create_db_rest.py

python3 insert_native.py

python3 insert_rest.py

python3 insert_ws.py

python3 query_native.py

python3 query_rest.py

python3 query_ws.py

python3 reqid_native.py

python3 reqid_rest.py

python3 reqid_ws.py

taos -s "drop database power"
check_transactions || exit 1
reset_cache || exit 1
python3 schemaless_native.py

taos -s "drop database power"
check_transactions || exit 1
reset_cache || exit 1
python3 schemaless_ws.py

taos -s "drop database power"
check_transactions || exit 1
reset_cache || exit 1
python3 stmt_native.py

python3 stmt_ws.py

taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
python3 tmq_native.py

taos -s "drop topic if exists topic_meters"
check_transactions || exit 1
reset_cache || exit 1
taos -s "drop database if exists power"
check_transactions || exit 1
reset_cache || exit 1
python3 tmq_websocket_example.py

python3 stmt2_native.py