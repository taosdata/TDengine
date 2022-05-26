#!/bin/bash
set -e
set -x

python3 ./test.py -f 0-others/taosShell.py
python3 ./test.py -f 0-others/taosShellError.py
python3 ./test.py -f 0-others/taosShellNetChk.py
python3 ./test.py -f 0-others/telemetry.py
python3 ./test.py -f 0-others/taosdMonitor.py
python3 ./test.py -f 0-others/udfTest.py
python3 ./test.py -f 0-others/udf_create.py
python3 ./test.py -f 0-others/udf_restart_taosd.py

python3 ./test.py -f 0-others/user_control.py
python3 ./test.py -f 0-others/fsync.py

#python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/varchar.py
python3 ./test.py -f 2-query/ltrim.py
python3 ./test.py -f 2-query/rtrim.py
python3 ./test.py -f 2-query/length.py
python3 ./test.py -f 2-query/char_length.py
python3 ./test.py -f 2-query/upper.py
python3 ./test.py -f 2-query/lower.py
#python3 ./test.py -f 2-query/join.py
python3 ./test.py -f 2-query/cast.py
#python3 ./test.py -f 2-query/concat.py
#python3 ./test.py -f 2-query/concat_ws.py
python3 ./test.py -f 2-query/check_tsdb.py
# python3 ./test.py -f 2-query/union.py
# python3 ./test.py -f 2-query/union2.py
# python3 ./test.py -f 2-query/union3.py
# python3 ./test.py -f 2-query/union4.py

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
# python3 ./test.py -f 2-query/diff.py
python3 ./test.py -f 2-query/Timediff.py

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
python3 ./test.py -f 2-query/query_cols_tags_and_or.py
#python3 ./test.py -f 2-query/nestedQuery.py

python3 ./test.py -f 7-tmq/basic5.py
python3 ./test.py -f 7-tmq/subscribeDb.py
python3 ./test.py -f 7-tmq/subscribeDb1.py
python3 ./test.py -f 7-tmq/subscribeStb.py
python3 ./test.py -f 7-tmq/subscribeStb0.py
python3 ./test.py -f 7-tmq/subscribeStb1.py
python3 ./test.py -f 7-tmq/subscribeStb2.py

