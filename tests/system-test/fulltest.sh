#!/bin/bash
set -e
set -x

python3 ./test.py -f 0-others/taosShell.py


#python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/varchar.py

#python3 ./test.py -f 2-query/timezone.py
python3 ./test.py -f 2-query/Now.py
python3 ./test.py -f 2-query/Today.py

#python3 ./test.py -f 2-query/cast.py

