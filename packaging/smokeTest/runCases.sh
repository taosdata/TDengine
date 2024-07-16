#!/bin/bash
ulimit -c unlimited

cd ${TDENGINE_ROOT_DIR}/tests/army
python3 ./test.py -f query/query_basic.py -N 3