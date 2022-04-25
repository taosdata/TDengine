#!/bin/bash
set -e

#python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/varchar.py
#python3 ./test.py -f 2-query/cast.py
