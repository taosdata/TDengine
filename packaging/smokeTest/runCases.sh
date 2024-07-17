#!/bin/bash
ulimit -c unlimited

cd ${TDENGINE_ROOT_DIR}/tests/army
python3 ./test.py -f query/query_basic.py -N 3

cd ${TDENGINE_ROOT_DIR}/tests/system-test
python3 ./test.py -f 1-insert/insert_column_value.py
python3 ./test.py -f 2-query/primary_ts_base_5.py
python3 ./test.py -f 2-query/case_when.py
python3 ./test.py -f 2-query/partition_limit_interval.py
python3 ./test.py -f 2-query/join.py
python3 ./test.py -f 2-query/fill.py
