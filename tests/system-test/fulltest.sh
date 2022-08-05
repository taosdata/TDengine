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
python3 ./test.py -f 0-others/cachemodel.py
python3 ./test.py -f 0-others/udf_cfg1.py
python3 ./test.py -f 0-others/udf_cfg2.py

python3 ./test.py -f 0-others/sysinfo.py
python3 ./test.py -f 0-others/user_control.py
python3 ./test.py -f 0-others/fsync.py

python3 ./test.py -f 1-insert/influxdb_line_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_telnet_line_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_json_taosc_insert.py
python3 ./test.py -f 1-insert/test_stmt_muti_insert_query.py
python3 ./test.py -f 1-insert/test_stmt_set_tbname_tag.py
python3 ./test.py -f 1-insert/alter_stable.py
python3 ./test.py -f 1-insert/alter_table.py
python3 ./test.py -f 1-insert/insertWithMoreVgroup.py
python3 ./test.py -f 1-insert/table_comment.py
#python3 ./test.py -f 1-insert/time_range_wise.py  #TD-18130
python3 ./test.py -f 1-insert/block_wise.py
python3 ./test.py -f 1-insert/create_retentions.py
python3 ./test.py -f 1-insert/table_param_ttl.py
python3 ./test.py -f 1-insert/mutil_stage.py

python3 ./test.py -f 1-insert/update_data_muti_rows.py
python3 ./test.py -f 1-insert/db_tb_name_check.py

python3 ./test.py -f 2-query/abs.py
python3 ./test.py -f 2-query/abs.py -R
python3 ./test.py -f 2-query/and_or_for_byte.py
python3 ./test.py -f 2-query/and_or_for_byte.py -R
python3 ./test.py -f 2-query/apercentile.py
python3 ./test.py -f 2-query/apercentile.py -R
python3 ./test.py -f 2-query/arccos.py
python3 ./test.py -f 2-query/arccos.py -R
python3 ./test.py -f 2-query/arcsin.py
python3 ./test.py -f 2-query/arcsin.py -R
python3 ./test.py -f 2-query/arctan.py
python3 ./test.py -f 2-query/arctan.py -R
python3 ./test.py -f 2-query/avg.py
python3 ./test.py -f 2-query/avg.py -R
python3 ./test.py -f 2-query/between.py
python3 ./test.py -f 2-query/between.py -R
python3 ./test.py -f 2-query/bottom.py
python3 ./test.py -f 2-query/bottom.py -R
python3 ./test.py -f 2-query/cast.py
python3 ./test.py -f 2-query/cast.py -R
python3 ./test.py -f 2-query/ceil.py
python3 ./test.py -f 2-query/ceil.py -R
python3 ./test.py -f 2-query/char_length.py
python3 ./test.py -f 2-query/char_length.py -R
python3 ./test.py -f 2-query/check_tsdb.py
python3 ./test.py -f 2-query/check_tsdb.py -R
python3 ./test.py -f 2-query/concat.py
python3 ./test.py -f 2-query/concat.py -R
python3 ./test.py -f 2-query/concat_ws.py
python3 ./test.py -f 2-query/concat_ws.py -R
python3 ./test.py -f 2-query/concat_ws2.py
python3 ./test.py -f 2-query/concat_ws2.py -R
python3 ./test.py -f 2-query/cos.py
python3 ./test.py -f 2-query/cos.py -R
python3 ./test.py -f 2-query/count_partition.py
python3 ./test.py -f 2-query/count_partition.py -R
python3 ./test.py -f 2-query/count.py
python3 ./test.py -f 2-query/count.py -R
python3 ./test.py -f 2-query/db.py
python3 ./test.py -f 2-query/db.py -R
python3 ./test.py -f 2-query/diff.py
python3 ./test.py -f 2-query/diff.py -R
python3 ./test.py -f 2-query/distinct.py
python3 ./test.py -f 2-query/distinct.py -R
python3 ./test.py -f 2-query/distribute_agg_apercentile.py
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -R
python3 ./test.py -f 2-query/distribute_agg_avg.py
python3 ./test.py -f 2-query/distribute_agg_avg.py -R
python3 ./test.py -f 2-query/distribute_agg_count.py
python3 ./test.py -f 2-query/distribute_agg_count.py -R
python3 ./test.py -f 2-query/distribute_agg_max.py
python3 ./test.py -f 2-query/distribute_agg_max.py -R
python3 ./test.py -f 2-query/distribute_agg_min.py
python3 ./test.py -f 2-query/distribute_agg_min.py -R




python3 ./test.py -f 1-insert/update_data.py

python3 ./test.py -f 1-insert/delete_data.py

python3 ./test.py -f 2-query/varchar.py
python3 ./test.py -f 2-query/ltrim.py
python3 ./test.py -f 2-query/rtrim.py
python3 ./test.py -f 2-query/length.py
python3 ./test.py -f 2-query/upper.py
python3 ./test.py -f 2-query/lower.py
python3 ./test.py -f 2-query/join.py
python3 ./test.py -f 2-query/join2.py
python3 ./test.py -f 2-query/substr.py
python3 ./test.py -f 2-query/union.py
python3 ./test.py -f 2-query/union1.py
python3 ./test.py -f 2-query/concat2.py
python3 ./test.py -f 2-query/spread.py
python3 ./test.py -f 2-query/hyperloglog.py
python3 ./test.py -f 2-query/explain.py
python3 ./test.py -f 2-query/leastsquares.py
python3 ./test.py -f 2-query/histogram.py


python3 ./test.py -f 2-query/timezone.py
python3 ./test.py -f 2-query/Now.py
python3 ./test.py -f 2-query/Today.py
python3 ./test.py -f 2-query/max.py
python3 ./test.py -f 2-query/min.py
python3 ./test.py -f 2-query/last.py
python3 ./test.py -f 2-query/first.py
python3 ./test.py -f 2-query/To_iso8601.py
python3 ./test.py -f 2-query/To_unixtimestamp.py
python3 ./test.py -f 2-query/timetruncate.py
python3 ./test.py -f 2-query/Timediff.py
python3 ./test.py -f 2-query/json_tag.py

python3 ./test.py -f 2-query/top.py
python3 ./test.py -f 2-query/percentile.py
python3 ./test.py -f 2-query/floor.py
python3 ./test.py -f 2-query/round.py
python3 ./test.py -f 2-query/log.py
python3 ./test.py -f 2-query/pow.py
python3 ./test.py -f 2-query/sqrt.py
python3 ./test.py -f 2-query/sin.py
python3 ./test.py -f 2-query/tan.py
python3 ./test.py -f 2-query/query_cols_tags_and_or.py
# python3 ./test.py -f 2-query/nestedQuery.py
# TD-15983 subquery output duplicate name column.
# Please Xiangyang Guo modify the following script
# python3 ./test.py -f 2-query/nestedQuery_str.py

python3 ./test.py -f 2-query/elapsed.py
python3 ./test.py -f 2-query/csum.py
#python3 ./test.py -f 2-query/mavg.py
python3 ./test.py -f 2-query/sample.py
python3 ./test.py -f 2-query/function_diff.py
python3 ./test.py -f 2-query/unique.py
python3 ./test.py -f 2-query/stateduration.py
python3 ./test.py -f 2-query/function_stateduration.py
python3 ./test.py -f 2-query/statecount.py
python3 ./test.py -f 2-query/tail.py
python3 ./test.py -f 2-query/ttl_comment.py
python3 ./test.py -f 2-query/distribute_agg_sum.py
python3 ./test.py -f 2-query/distribute_agg_spread.py
python3 ./test.py -f 2-query/distribute_agg_stddev.py
python3 ./test.py -f 2-query/twa.py
python3 ./test.py -f 2-query/irate.py
python3 ./test.py -f 2-query/function_null.py
python3 ./test.py -f 2-query/queryQnode.py
python3 ./test.py -f 2-query/max_partition.py
python3 ./test.py -f 2-query/last_row.py
python3 ./test.py -f 2-query/tsbsQuery.py

python3 ./test.py -f 6-cluster/5dnode1mnode.py
python3 ./test.py -f 6-cluster/5dnode2mnode.py  -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStopLoop.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py  -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDbRep3.py -N 5 -M 3

python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py  -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py  -N 5 -M 3

python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertData.py -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertDataAsync.py -N 5 -M 3
# python3 ./test.py -f 6-cluster/5dnode3mnodeRestartMnodeInsertData.py -N 5 -M 3
# python3 ./test.py -f 6-cluster/5dnode3mnodeRestartVnodeInsertData.py -N 5 -M 3

python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -N 6 -M 3 -C 5
# BUG python3 ./test.py -f 6-cluster/5dnode3mnodeStopInsert.py
# python3 ./test.py -f 6-cluster/5dnode3mnodeDrop.py -N 5
# python3 test.py -f 6-cluster/5dnode3mnodeStopConnect.py -N 5 -M 3

python3 ./test.py -f 6-cluster/5dnode3mnodeRecreateMnode.py  -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStopFollowerLeader.py  -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py  -N 5 -M 3

# vnode case 
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_createDb_replica1.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas_querys.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_force_stop_all_dnodes.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_all_vnode.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_follower.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_leader.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_all_dnodes.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_follower_sync.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_follower_unsync_force_stop.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_follower_unsync.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_leader_forece_stop.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_stop_leader.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_mnode3_insertdatas_querys.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_follower_force_stop.py -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_follower.py -N 4 -M 1
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_leader_force_stop.py -N 4 -M 1 
# python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_querydatas_stop_leader.py -N 4 -M 1 
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_vgroups.py  -N 4 -M 1
python3 test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_vgroups_stopOne.py -N 4 -M 1


python3 ./test.py -f 7-tmq/dropDbR3ConflictTransaction.py -N 3
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
#python3 ./test.py -f 7-tmq/tmq3mnodeSwitch.py -N 5
python3 ./test.py -f 7-tmq/tmqConsumerGroup.py
python3 ./test.py -f 7-tmq/tmqShow.py
python3 ./test.py -f 7-tmq/tmqAlterSchema.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-1ctb.py
# python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-1ctb.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-1ctb-funcNFilter.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg-mutilCtb-funcNFilter.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg-mutilCtb.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-1ctb-funcNFilter.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg-mutilCtb-funcNFilter.py
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg-mutilCtb.py
#python3 ./test.py -f 7-tmq/tmqAutoCreateTbl.py
#python3 ./test.py -f 7-tmq/tmqDnodeRestart.py
python3 ./test.py -f 7-tmq/tmqUpdate-1ctb.py
python3 ./test.py -f 7-tmq/tmqUpdateWithConsume.py
python3 ./test.py -f 7-tmq/tmqUpdate-multiCtb-snapshot0.py
python3 ./test.py -f 7-tmq/tmqUpdate-multiCtb-snapshot1.py
python3 ./test.py -f 7-tmq/tmqDelete-1ctb.py
python3 ./test.py -f 7-tmq/tmqDelete-multiCtb.py
python3 ./test.py -f 7-tmq/tmqDropStb.py
python3 ./test.py -f 7-tmq/tmqDropStbCtb.py
python3 ./test.py -f 7-tmq/tmqDropNtb-snapshot0.py
python3 ./test.py -f 7-tmq/tmqDropNtb-snapshot1.py
python3 ./test.py -f 7-tmq/tmqUdf.py
python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot0.py
python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot1.py
python3 ./test.py -f 7-tmq/stbTagFilter-1ctb.py
python3 ./test.py -f 7-tmq/dataFromTsdbNWal.py
python3 ./test.py -f 7-tmq/dataFromTsdbNWal-multiCtb.py
python3 ./test.py -f 7-tmq/tmq_taosx.py
# python3 ./test.py -f 7-tmq/stbTagFilter-multiCtb.py

#------------querPolicy  2-----------

python3 ./test.py -f 2-query/between.py  -Q 2
python3 ./test.py -f 2-query/distinct.py -Q 2
python3 ./test.py -f 2-query/varchar.py -Q 2
python3 ./test.py -f 2-query/ltrim.py -Q 2
python3 ./test.py -f 2-query/rtrim.py -Q 2
python3 ./test.py -f 2-query/length.py -Q 2
python3 ./test.py -f 2-query/char_length.py -Q 2
python3 ./test.py -f 2-query/upper.py -Q 2
python3 ./test.py -f 2-query/lower.py -Q 2
python3 ./test.py -f 2-query/join.py -Q 2
python3 ./test.py -f 2-query/join2.py -Q 2
python3 ./test.py -f 2-query/cast.py -Q 2
python3 ./test.py -f 2-query/substr.py -Q 2
python3 ./test.py -f 2-query/union.py -Q 2
python3 ./test.py -f 2-query/union1.py -Q 2
python3 ./test.py -f 2-query/concat.py -Q 2
python3 ./test.py -f 2-query/concat2.py -Q 2
python3 ./test.py -f 2-query/concat_ws.py -Q 2
python3 ./test.py -f 2-query/concat_ws2.py -Q 2
#python3 ./test.py -f 2-query/check_tsdb.py -Q 2
python3 ./test.py -f 2-query/spread.py -Q 2
python3 ./test.py -f 2-query/hyperloglog.py -Q 2
python3 ./test.py -f 2-query/explain.py -Q 2
python3 ./test.py -f 2-query/leastsquares.py -Q 2
python3 ./test.py -f 2-query/timezone.py -Q 2
python3 ./test.py -f 2-query/Now.py -Q 2
python3 ./test.py -f 2-query/Today.py -Q 2
python3 ./test.py -f 2-query/max.py -Q 2
python3 ./test.py -f 2-query/min.py -Q 2
python3 ./test.py -f 2-query/count.py -Q 2
python3 ./test.py -f 2-query/last.py -Q 2
python3 ./test.py -f 2-query/first.py -Q 2
python3 ./test.py -f 2-query/To_iso8601.py -Q 2
python3 ./test.py -f 2-query/To_unixtimestamp.py -Q 2
python3 ./test.py -f 2-query/timetruncate.py -Q 2
python3 ./test.py -f 2-query/diff.py -Q 2
python3 ./test.py -f 2-query/Timediff.py -Q 2
python3 ./test.py -f 2-query/json_tag.py -Q 2
python3 ./test.py -f 2-query/top.py -Q 2
python3 ./test.py -f 2-query/bottom.py -Q 2
python3 ./test.py -f 2-query/percentile.py -Q 2
python3 ./test.py -f 2-query/apercentile.py -Q 2
python3 ./test.py -f 2-query/abs.py -Q 2
python3 ./test.py -f 2-query/ceil.py -Q 2
python3 ./test.py -f 2-query/floor.py -Q 2
python3 ./test.py -f 2-query/round.py -Q 2
python3 ./test.py -f 2-query/log.py -Q 2
python3 ./test.py -f 2-query/pow.py -Q 2
python3 ./test.py -f 2-query/sqrt.py -Q 2
python3 ./test.py -f 2-query/sin.py -Q 2
python3 ./test.py -f 2-query/cos.py -Q 2
python3 ./test.py -f 2-query/tan.py -Q 2
python3 ./test.py -f 2-query/arcsin.py -Q 2
python3 ./test.py -f 2-query/arccos.py -Q 2
python3 ./test.py -f 2-query/arctan.py -Q 2
python3 ./test.py -f 2-query/query_cols_tags_and_or.py  -Q 2

# python3 ./test.py -f 2-query/nestedQuery.py  -Q 2
# python3 ./test.py -f 2-query/nestedQuery_str.py  -Q 2

python3 ./test.py -f 2-query/avg.py   -Q 2
# python3 ./test.py -f 2-query/elapsed.py  -Q 2
python3 ./test.py -f 2-query/csum.py  -Q 2
#python3 ./test.py -f 2-query/mavg.py  -Q 2
python3 ./test.py -f 2-query/sample.py  -Q 2
python3 ./test.py -f 2-query/function_diff.py  -Q 2
python3 ./test.py -f 2-query/unique.py  -Q 2
python3 ./test.py -f 2-query/stateduration.py  -Q 2
python3 ./test.py -f 2-query/function_stateduration.py  -Q 2
python3 ./test.py -f 2-query/statecount.py  -Q 2
python3 ./test.py -f 2-query/tail.py  -Q 2
python3 ./test.py -f 2-query/ttl_comment.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_count.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_max.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_min.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_sum.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_spread.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_apercentile.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_avg.py  -Q 2
python3 ./test.py -f 2-query/distribute_agg_stddev.py  -Q 2
python3 ./test.py -f 2-query/twa.py  -Q 2
python3 ./test.py -f 2-query/irate.py  -Q 2
python3 ./test.py -f 2-query/function_null.py  -Q 2
python3 ./test.py -f 2-query/count_partition.py -Q 2
python3 ./test.py -f 2-query/max_partition.py -Q 2
python3 ./test.py -f 2-query/last_row.py -Q 2
python3 ./test.py -f 2-query/tsbsQuery.py -Q 2
#------------querPolicy  3-----------

python3 ./test.py -f 2-query/between.py -Q  3
python3 ./test.py -f 2-query/distinct.py -Q  3
python3 ./test.py -f 2-query/varchar.py -Q  3
python3 ./test.py -f 2-query/ltrim.py -Q  3
python3 ./test.py -f 2-query/rtrim.py -Q  3
python3 ./test.py -f 2-query/length.py -Q  3
python3 ./test.py -f 2-query/char_length.py -Q  3
python3 ./test.py -f 2-query/upper.py -Q  3
python3 ./test.py -f 2-query/lower.py -Q  3
python3 ./test.py -f 2-query/join.py -Q  3
python3 ./test.py -f 2-query/join2.py -Q  3
python3 ./test.py -f 2-query/cast.py -Q  3
python3 ./test.py -f 2-query/substr.py -Q  3
python3 ./test.py -f 2-query/union.py -Q  3
python3 ./test.py -f 2-query/union1.py -Q  3
python3 ./test.py -f 2-query/concat.py -Q  3
python3 ./test.py -f 2-query/concat2.py -Q  3
python3 ./test.py -f 2-query/concat_ws.py -Q  3
python3 ./test.py -f 2-query/concat_ws2.py -Q  3
#python3 ./test.py -f 2-query/check_tsdb.py -Q  3
python3 ./test.py -f 2-query/spread.py -Q  3
python3 ./test.py -f 2-query/hyperloglog.py -Q  3
python3 ./test.py -f 2-query/explain.py -Q  3
python3 ./test.py -f 2-query/leastsquares.py -Q  3
python3 ./test.py -f 2-query/timezone.py -Q  3
python3 ./test.py -f 2-query/Now.py -Q  3
python3 ./test.py -f 2-query/Today.py -Q  3
python3 ./test.py -f 2-query/max.py -Q  3
python3 ./test.py -f 2-query/min.py -Q  3
python3 ./test.py -f 2-query/count.py -Q  3
#python3 ./test.py -f 2-query/last.py -Q  3
python3 ./test.py -f 2-query/first.py -Q  3
python3 ./test.py -f 2-query/To_iso8601.py -Q  3
python3 ./test.py -f 2-query/To_unixtimestamp.py -Q  3
python3 ./test.py -f 2-query/timetruncate.py -Q  3
python3 ./test.py -f 2-query/diff.py -Q  3
python3 ./test.py -f 2-query/Timediff.py -Q  3
python3 ./test.py -f 2-query/json_tag.py -Q  3
python3 ./test.py -f 2-query/top.py -Q  3
python3 ./test.py -f 2-query/bottom.py -Q  3
python3 ./test.py -f 2-query/percentile.py -Q  3
python3 ./test.py -f 2-query/apercentile.py -Q  3
python3 ./test.py -f 2-query/abs.py -Q  3
python3 ./test.py -f 2-query/ceil.py -Q  3
python3 ./test.py -f 2-query/floor.py -Q  3
python3 ./test.py -f 2-query/round.py -Q  3
python3 ./test.py -f 2-query/log.py -Q  3
python3 ./test.py -f 2-query/pow.py -Q  3
python3 ./test.py -f 2-query/sqrt.py -Q  3
python3 ./test.py -f 2-query/sin.py -Q  3
python3 ./test.py -f 2-query/cos.py -Q  3
python3 ./test.py -f 2-query/tan.py -Q  3
python3 ./test.py -f 2-query/arcsin.py -Q  3
python3 ./test.py -f 2-query/arccos.py -Q  3
python3 ./test.py -f 2-query/arctan.py -Q  3
python3 ./test.py -f 2-query/query_cols_tags_and_or.py -Q  3
# python3 ./test.py -f 2-query/nestedQuery.py -Q  3
# python3 ./test.py -f 2-query/nestedQuery_str.py -Q  3
# python3 ./test.py -f 2-query/avg.py -Q  3
# python3 ./test.py -f 2-query/elapsed.py -Q  3
python3 ./test.py -f 2-query/csum.py -Q  3
#python3 ./test.py -f 2-query/mavg.py -Q  3
python3 ./test.py -f 2-query/sample.py -Q  3
python3 ./test.py -f 2-query/function_diff.py -Q  3
python3 ./test.py -f 2-query/unique.py -Q  3
python3 ./test.py -f 2-query/stateduration.py -Q  3
python3 ./test.py -f 2-query/function_stateduration.py -Q  3
python3 ./test.py -f 2-query/statecount.py -Q  3
python3 ./test.py -f 2-query/tail.py -Q  3
python3 ./test.py -f 2-query/ttl_comment.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_count.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_max.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_min.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_sum.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_spread.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_avg.py -Q  3
python3 ./test.py -f 2-query/distribute_agg_stddev.py -Q  3
python3 ./test.py -f 2-query/twa.py -Q  3
python3 ./test.py -f 2-query/irate.py -Q  3
python3 ./test.py -f 2-query/function_null.py -Q  3
python3 ./test.py -f 2-query/count_partition.py -Q 3
python3 ./test.py -f 2-query/max_partition.py -Q 3
python3 ./test.py -f 2-query/last_row.py -Q 3
python3 ./test.py -f 2-query/tsbsQuery.py -Q 3
python3 ./test.py -f 2-query/sml.py -Q 3
