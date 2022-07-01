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
python3 ./test.py -f 0-others/cachelast.py

python3 ./test.py -f 0-others/user_control.py
python3 ./test.py -f 0-others/fsync.py

python3 ./test.py -f 1-insert/influxdb_line_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_telnet_line_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_json_taosc_insert.py
# BUG python3 ./test.py -f 1-insert/test_stmt_muti_insert_query.py
python3 ./test.py -f 1-insert/alter_stable.py
python3 ./test.py -f 1-insert/alter_table.py
python3 ./test.py -f 1-insert/insertWithMoreVgroup.py
python3 ./test.py -f 1-insert/table_comment.py
#python3 ./test.py -f 1-insert/table_param_ttl.py
python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/varchar.py
python3 ./test.py -f 2-query/ltrim.py
python3 ./test.py -f 2-query/rtrim.py
python3 ./test.py -f 2-query/length.py
python3 ./test.py -f 2-query/char_length.py
python3 ./test.py -f 2-query/upper.py
python3 ./test.py -f 2-query/lower.py
python3 ./test.py -f 2-query/join.py
python3 ./test.py -f 2-query/join2.py
python3 ./test.py -f 2-query/cast.py
python3 ./test.py -f 2-query/substr.py
python3 ./test.py -f 2-query/union.py
python3 ./test.py -f 2-query/union1.py
python3 ./test.py -f 2-query/concat.py
python3 ./test.py -f 2-query/concat2.py
python3 ./test.py -f 2-query/concat_ws.py
python3 ./test.py -f 2-query/concat_ws2.py
python3 ./test.py -f 2-query/check_tsdb.py
python3 ./test.py -f 2-query/spread.py
python3 ./test.py -f 2-query/hyperloglog.py
python3 ./test.py -f 2-query/explain.py
python3 ./test.py -f 2-query/leastsquares.py


python3 ./test.py -f 2-query/timezone.py
python3 ./test.py -f 2-query/Now.py
python3 ./test.py -f 2-query/Today.py
python3 ./test.py -f 2-query/max.py
python3 ./test.py -f 2-query/min.py
python3 ./test.py -f 2-query/count.py
python3 ./test.py -f 2-query/last.py
python3 ./test.py -f 2-query/first.py
python3 ./test.py -f 2-query/To_iso8601.py
python3 ./test.py -f 2-query/To_unixtimestamp.py
python3 ./test.py -f 2-query/timetruncate.py
python3 ./test.py -f 2-query/diff.py
python3 ./test.py -f 2-query/Timediff.py
#python3 ./test.py -f 2-query/json_tag.py

python3 ./test.py -f 2-query/top.py
python3 ./test.py -f 2-query/bottom.py
python3 ./test.py -f 2-query/percentile.py
python3 ./test.py -f 2-query/apercentile.py
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
# python3 ./test.py -f 2-query/nestedQuery.py
# TD-15983 subquery output duplicate name column.
# Please Xiangyang Guo modify the following script
# python3 ./test.py -f 2-query/nestedQuery_str.py

python3 ./test.py -f 2-query/avg.py
python3 ./test.py -f 2-query/elapsed.py
python3 ./test.py -f 2-query/csum.py
python3 ./test.py -f 2-query/mavg.py
python3 ./test.py -f 2-query/diff.py
python3 ./test.py -f 2-query/sample.py
python3 ./test.py -f 2-query/function_diff.py
python3 ./test.py -f 2-query/unique.py
python3 ./test.py -f 2-query/stateduration.py
python3 ./test.py -f 2-query/function_stateduration.py
python3 ./test.py -f 2-query/statecount.py
python3 ./test.py -f 2-query/tail.py
python3 ./test.py -f 2-query/ttl_comment.py
python3 ./test.py -f 2-query/distribute_agg_count.py
python3 ./test.py -f 2-query/distribute_agg_max.py
python3 ./test.py -f 2-query/distribute_agg_min.py
python3 ./test.py -f 2-query/distribute_agg_sum.py
python3 ./test.py -f 2-query/distribute_agg_spread.py
python3 ./test.py -f 2-query/distribute_agg_apercentile.py
python3 ./test.py -f 2-query/distribute_agg_avg.py
python3 ./test.py -f 2-query/distribute_agg_stddev.py
python3 ./test.py -f 2-query/twa.py
python3 ./test.py -f 2-query/irate.py

python3 ./test.py -f 2-query/function_null.py
python3 ./test.py -f 2-query/queryQnode.py 

python3 ./test.py -f 6-cluster/5dnode1mnode.py 
python3 ./test.py -f 6-cluster/5dnode2mnode.py  -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStopLoop.py -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -N 5 -M 3 
# python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py  -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -N 5 -M 3 
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py  -N 5 -M 3 
# python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py  -N 5 -M 3
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeStopInsert.py 
# python3 ./test.py -f 6-cluster/5dnode3mnodeDrop.py -N 5
# python3 test.py -f 6-cluster/5dnode3mnodeStopConnect.py -N 5 -M 3


python3 ./test.py -f 7-tmq/basic5.py
python3 ./test.py -f 7-tmq/subscribeDb.py
python3 ./test.py -f 7-tmq/subscribeDb0.py
python3 ./test.py -f 7-tmq/subscribeDb1.py
python3 ./test.py -f 7-tmq/subscribeDb2.py
python3 ./test.py -f 7-tmq/subscribeDb3.py
#python3 ./test.py -f 7-tmq/subscribeDb4.py
python3 ./test.py -f 7-tmq/subscribeStb.py
python3 ./test.py -f 7-tmq/subscribeStb0.py
python3 ./test.py -f 7-tmq/subscribeStb1.py
python3 ./test.py -f 7-tmq/subscribeStb2.py
python3 ./test.py -f 7-tmq/subscribeStb3.py
python3 ./test.py -f 7-tmq/subscribeStb4.py
python3 ./test.py -f 7-tmq/db.py
python3 ./test.py -f 7-tmq/tmqError.py
python3 ./test.py -f 7-tmq/schema.py
python3 ./test.py -f 7-tmq/stbFilter.py
python3 ./test.py -f 7-tmq/tmqCheckData.py
python3 ./test.py -f 7-tmq/tmqCheckData1.py
python3 ./test.py -f 7-tmq/tmqUdf.py
#python3 ./test.py -f 7-tmq/tmq3mnodeSwitch.py -N 5
python3 ./test.py -f 7-tmq/tmqConsumerGroup.py
python3 ./test.py -f 7-tmq/tmqShow.py
python3 ./test.py -f 7-tmq/tmqAlterSchema.py
