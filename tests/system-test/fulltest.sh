#!/bin/bash
set -e
set -x

python3 ./test.py -f 0-others/taosShell.py
python3 ./test.py -f 0-others/taosShellError.py
python3 ./test.py -f 0-others/taosShellNetChk.py
python3 ./test.py -f 0-others/telemetry.py


#python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/varchar.py

python3 ./test.py -f 2-query/timezone.py
python3 ./test.py -f 2-query/Now.py
python3 ./test.py -f 2-query/Today.py
python3 ./test.py -f 2-query/max.py
python3 ./test.py -f 2-query/min.py
python3 ./test.py -f 2-query/count.py
python3 ./test.py -f 2-query/last.py
#python3 ./test.py -f 2-query/To_iso8601.py
python3 ./test.py -f 2-query/To_unixtimestamp.py
python3 ./test.py -f 2-query/timetruncate.py

python3 ./test.py -f 2-query/Timediff.py
# python3 ./test.py -f 2-query/diff.py
#python3 ./test.py -f 2-query/cast.py


python3 ./test.py -f 2-query/abs.py
python3 ./test.py -f 2-query/ceil.py
python3 ./test.py -f 2-query/floor.py
python3 ./test.py -f 2-query/round.py
python3 ./test.py -f 2-query/log.py
python3 ./test.py -f 2-query/pow.py
python3 ./test.py -f 2-query/sqrt.py
python3 ./test.py -f 2-query/sin.py
python3 ./test.py -f 2-query/cos.py
python3 ./test.py -f 2-query/tan.py
python3 ./test.py -f 2-query/arcsin.py
python3 ./test.py -f 2-query/arccos.py
python3 ./test.py -f 2-query/arctan.py