#!/bin/bash
set -e
set -x

python3 ./test.py -f 0-others/taosShell.py
python3 ./test.py -f 0-others/taosShellError.py
python3 ./test.py -f 0-others/taosShellNetChk.py


#python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/varchar.py

#python3 ./test.py -f 2-query/timezone.py
python3 ./test.py -f 2-query/Now.py
python3 ./test.py -f 2-query/Today.py

#python3 ./test.py -f 2-query/cast.py


python3 ./test.py -f 2-query/abs.py
python3 ./test.py -f 2-query/ceil.py
python3 ./test.py -f 2-query/floor.py
python3 ./test.py -f 2-query/round.py
python3 ./test.py -f 2-query/log.py
python3 ./test.py -f 2-query/pow.py
python3 ./test.py -f 2-query/sin.py