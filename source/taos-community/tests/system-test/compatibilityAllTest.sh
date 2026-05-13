#!/bin/bash
ulimit -c unlimited
#======================p1-insert===============

python3 ./test.py -f 0-others/taosShell.py
python3 ./test.py -f 0-others/taosShellError.py
python3 ./test.py -f 0-others/taosShellNetChk.py
python3 ./test.py -f 1-insert/alter_database.py
python3 ./test.py -f 1-insert/influxdb_line_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_telnet_line_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_json_taosc_insert.py
python3 ./test.py -f 1-insert/test_stmt_muti_insert_query.py
python3 ./test.py -f 1-insert/test_stmt_set_tbname_tag.py
python3 ./test.py -f 1-insert/alter_stable.py
python3 ./test.py -f 1-insert/alter_table.py
python3 ./test.py -f 1-insert/boundary.py
python3 ./test.py -f 2-query/top.py
python3 ./test.py -f 2-query/top.py -R
python3 ./test.py -f 2-query/tsbsQuery.py
python3 ./test.py -f 2-query/tsbsQuery.py -R
python3 ./test.py -f 2-query/ttl_comment.py
python3 ./test.py -f 2-query/ttl_comment.py -R
python3 ./test.py -f 2-query/twa.py
python3 ./test.py -f 2-query/twa.py -R
python3 ./test.py -f 2-query/union.py
python3 ./test.py -f 2-query/union.py -R
python3 ./test.py -f 6-cluster/5dnode1mnode.py
python3 ./test.py -f 6-cluster/5dnode2mnode.py -N 5
python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3 -i False
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -N 5 -M 3 -i False
python3 ./test.py -f 6-cluster/5dnode3mnodeStopLoop.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 6 -M 3 -n 3
python3 ./test.py -f 7-tmq/subscribeStb4.py
python3 ./test.py -f 7-tmq/db.py
python3 ./test.py -f 7-tmq/tmqError.py
python3 ./test.py -f 7-tmq/schema.py
python3 ./test.py -f 7-tmq/stbFilter.py
python3 ./test.py -f 7-tmq/tmqCheckData.py
python3 ./test.py -f 7-tmq/tmqCheckData1.py
python3 ./test.py -f 7-tmq/tmqConsumerGroup.py
python3 ./test.py -f 7-tmq/tmqShow.py
python3 ./test.py -f 7-tmq/tmqAlterSchema.py
python3 ./test.py -f 99-TDcase/TD-20582.py
