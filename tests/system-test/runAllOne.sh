# start -N3
echo " **********  -N 3 *************"
python3 ./test.py -f 7-tmq/tmqDelete-multiCtb.py -N 3 -n 3
python3 ./test.py -f 1-insert/alter_database.py -P
python3 ./test.py -f 1-insert/influxdb_line_taosc_insert.py -P
python3 ./test.py -f 1-insert/opentsdb_telnet_line_taosc_insert.py -P
python3 ./test.py -f 1-insert/opentsdb_json_taosc_insert.py -P
python3 ./test.py -f 1-insert/test_stmt_muti_insert_query.py -P
python3 ./test.py -f 1-insert/test_stmt_set_tbname_tag.py -P
python3 ./test.py -f 1-insert/alter_stable.py -P
python3 ./test.py -f 1-insert/alter_table.py -P
python3 ./test.py -f 1-insert/boundary.py -P
python3 ./test.py -f 1-insert/insertWithMoreVgroup.py -P
python3 ./test.py -f 1-insert/table_comment.py -P
#python3 ./test.py -f 1-insert/time_range_wise.py -P
#python3 ./test.py -f 1-insert/block_wise.py -P
#python3 ./test.py -f 1-insert/create_retentions.py -P
python3 ./test.py -f 1-insert/mutil_stage.py -P
python3 ./test.py -f 1-insert/table_param_ttl.py -P
python3 ./test.py -f 1-insert/table_param_ttl.py -P -R
python3 ./test.py -f 1-insert/update_data_muti_rows.py -P
python3 ./test.py -f 1-insert/db_tb_name_check.py -P
python3 ./test.py -f 1-insert/InsertFuturets.py -P
python3 ./test.py -f 1-insert/insert_wide_column.py -P
python3 ./test.py -f 2-query/nestedQuery.py -P
python3 ./test.py -f 2-query/nestedQuery_str.py -P
python3 ./test.py -f 2-query/nestedQuery_math.py -P
python3 ./test.py -f 2-query/nestedQuery_time.py -P
python3 ./test.py -f 2-query/nestedQuery_26.py -P
python3 ./test.py -f 2-query/nestedQuery_str.py -P -Q 2
python3 ./test.py -f 2-query/nestedQuery_math.py -P -Q 2
python3 ./test.py -f 2-query/nestedQuery_time.py -P -Q 2
python3 ./test.py -f 2-query/nestedQuery.py -P -Q 2
python3 ./test.py -f 2-query/nestedQuery_26.py -P -Q 2
python3 ./test.py -f 2-query/columnLenUpdated.py -P 
python3 ./test.py -f 2-query/columnLenUpdated.py -P -Q 2
python3 ./test.py -f 2-query/columnLenUpdated.py -P -Q 3
python3 ./test.py -f 2-query/columnLenUpdated.py -P -Q 4
python3 ./test.py -f 2-query/nestedQuery.py -P -Q 4
python3 ./test.py -f 2-query/nestedQuery_str.py -P -Q 4
python3 ./test.py -f 2-query/nestedQuery_math.py -P -Q 4
python3 ./test.py -f 2-query/nestedQuery_time.py -P -Q 4
python3 ./test.py -f 2-query/nestedQuery_26.py -P -Q 4
python3 ./test.py -f 7-tmq/tmqShow.py -P
python3 ./test.py -f 7-tmq/tmqDropStb.py -P
python3 ./test.py -f 7-tmq/subscribeStb0.py -P
python3 ./test.py -f 7-tmq/subscribeStb1.py -P
python3 ./test.py -f 7-tmq/subscribeStb2.py -P
python3 ./test.py -f 7-tmq/subscribeStb3.py -P
python3 ./test.py -f 7-tmq/subscribeDb0.py -P -N 3 -n 3
python3 ./test.py -f 1-insert/delete_stable.py -P
python3 ./test.py -f 2-query/out_of_order.py -P -Q 3
python3 ./test.py -f 2-query/out_of_order.py -P
python3 ./test.py -f 2-query/insert_null_none.py -P
python3 ./test.py -f 2-query/insert_null_none.py -P -R
python3 ./test.py -f 2-query/insert_null_none.py -P -Q 2
python3 ./test.py -f 2-query/insert_null_none.py -P -Q 3
python3 ./test.py -f 2-query/insert_null_none.py -P -Q 4
python3 ./test.py -f 1-insert/database_pre_suf.py -P
python3 ./test.py -f 2-query/concat.py -P -Q 3
python3 ./test.py -f 2-query/out_of_order.py -P -Q 2
python3 ./test.py -f 2-query/out_of_order.py -P -Q 4
python3 ./test.py -f 2-query/nestedQuery.py -P -Q 3
python3 ./test.py -f 2-query/nestedQuery_str.py -P -Q 3
python3 ./test.py -f 2-query/nestedQuery_math.py -P -Q 3
python3 ./test.py -f 2-query/nestedQuery_time.py -P -Q 3
python3 ./test.py -f 2-query/nestedQuery_26.py -P -Q 3
python3 ./test.py -f 7-tmq/create_wrong_topic.py -P
python3 ./test.py -f 7-tmq/dropDbR3ConflictTransaction.py -P -N 3
python3 ./test.py -f 7-tmq/basic5.py -P
python3 ./test.py -f 7-tmq/subscribeDb.py -P -N 3 -n 3
python3 ./test.py -f 7-tmq/subscribeDb1.py -P
python3 ./test.py -f 7-tmq/subscribeDb2.py -P
python3 ./test.py -f 7-tmq/subscribeDb3.py -P
python3 ./test.py -f 7-tmq/subscribeDb4.py -P
python3 ./test.py -f 7-tmq/subscribeStb.py -P
python3 ./test.py -f 7-tmq/subscribeStb4.py -P
python3 ./test.py -f 7-tmq/db.py -P
python3 ./test.py -f 7-tmq/tmqError.py -P
python3 ./test.py -f 7-tmq/schema.py -P
python3 ./test.py -f 7-tmq/stbFilter.py -P
python3 ./test.py -f 7-tmq/tmqCheckData.py -P
python3 ./test.py -f 7-tmq/tmqCheckData1.py -P
python3 ./test.py -f 7-tmq/tmqConsumerGroup.py -P
python3 ./test.py -f 7-tmq/tmqAlterSchema.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb.py -P -N 3 -n 3
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1.py -P -N 3 -n 3
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-1ctb.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-1ctb.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-1ctb-funcNFilter.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg-mutilCtb-funcNFilter.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb-mutilVg-mutilCtb.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-1ctb-funcNFilter.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg-mutilCtb-funcNFilter.py -P
python3 ./test.py -f 7-tmq/tmqConsFromTsdb1-mutilVg-mutilCtb.py -P
python3 ./test.py -f 7-tmq/tmqAutoCreateTbl.py -P
python3 ./test.py -f 7-tmq/tmqDnodeRestart.py -P
python3 ./test.py -f 7-tmq/tmqDnodeRestart1.py -P
python3 ./test.py -f 7-tmq/tmqUpdate-1ctb.py -P
python3 ./test.py -f 7-tmq/tmqUpdate-multiCtb-snapshot0.py -P
python3 ./test.py -f 7-tmq/tmqUpdate-multiCtb-snapshot1.py -P
python3 ./test.py -f 7-tmq/tmqDropStbCtb.py -P
python3 ./test.py -f 7-tmq/tmqDropNtb-snapshot0.py -P
python3 ./test.py -f 7-tmq/tmqDropNtb-snapshot1.py -P
python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot0.py -P
python3 ./test.py -f 7-tmq/tmqUdf-multCtb-snapshot1.py -P
python3 ./test.py -f 7-tmq/stbTagFilter-1ctb.py -P
python3 ./test.py -f 7-tmq/dataFromTsdbNWal.py -P
python3 ./test.py -f 7-tmq/dataFromTsdbNWal-multiCtb.py -P
python3 ./test.py -f 7-tmq/tmq_taosx.py -P
python3 ./test.py -f 7-tmq/raw_block_interface_test.py -P
python3 ./test.py -f 7-tmq/stbTagFilter-multiCtb.py -P
python3 ./test.py -f 99-TDcase/TD-19201.py -P
python3 ./test.py -f 99-TDcase/TD-21561.py -P
python3 ./test.py -f 0-others/taosShell.py -P
python3 ./test.py -f 0-others/taosShellError.py -P
python3 ./test.py -f 0-others/taosShellNetChk.py -P
python3 ./test.py -f 0-others/telemetry.py -P
python3 ./test.py -f 0-others/backquote_check.py -P
python3 ./test.py -f 0-others/taosdMonitor.py -P
python3 ./test.py -f 0-others/udfTest.py -P
python3 ./test.py -f 0-others/udf_create.py -P
python3 ./test.py -f 0-others/udf_restart_taosd.py -P
python3 ./test.py -f 0-others/udf_cfg1.py -P
python3 ./test.py -f 0-others/udf_cfg2.py -P
python3 ./test.py -f 0-others/cachemodel.py -P
python3 ./test.py -f 0-others/sysinfo.py -P
python3 ./test.py -f 0-others/user_control.py -P
python3 ./test.py -f 0-others/user_manage.py -P
python3 ./test.py -f 0-others/fsync.py -P
python3 ./test.py -f 0-others/tag_index_basic.py -P
python3 ./test.py -f 0-others/show.py -P
python3 ./test.py -f 0-others/information_schema.py -P
python3 ./test.py -f 2-query/abs.py -P
python3 ./test.py -f 2-query/abs.py -P -R
python3 ./test.py -f 2-query/and_or_for_byte.py -P
python3 ./test.py -f 2-query/and_or_for_byte.py -P -R
python3 ./test.py -f 2-query/apercentile.py -P
python3 ./test.py -f 2-query/apercentile.py -P -R
python3 ./test.py -f 2-query/arccos.py -P
python3 ./test.py -f 2-query/arccos.py -P -R
python3 ./test.py -f 2-query/arcsin.py -P
python3 ./test.py -f 2-query/arcsin.py -P -R
python3 ./test.py -f 2-query/arctan.py -P
python3 ./test.py -f 2-query/arctan.py -P -R
python3 ./test.py -f 2-query/avg.py -P
python3 ./test.py -f 2-query/avg.py -P -R
python3 ./test.py -f 2-query/between.py -P
python3 ./test.py -f 2-query/between.py -P -R
python3 ./test.py -f 2-query/bottom.py -P
python3 ./test.py -f 2-query/bottom.py -P -R
python3 ./test.py -f 2-query/cast.py -P
python3 ./test.py -f 2-query/cast.py -P -R
python3 ./test.py -f 2-query/ceil.py -P
python3 ./test.py -f 2-query/ceil.py -P -R
python3 ./test.py -f 2-query/char_length.py -P
python3 ./test.py -f 2-query/char_length.py -P -R
python3 ./test.py -f 2-query/check_tsdb.py -P
python3 ./test.py -f 2-query/check_tsdb.py -P -R
python3 ./test.py -f 2-query/concat.py -P
python3 ./test.py -f 2-query/concat.py -P -R
python3 ./test.py -f 2-query/concat_ws.py -P
python3 ./test.py -f 2-query/concat_ws.py -P -R
python3 ./test.py -f 2-query/concat_ws2.py -P
python3 ./test.py -f 2-query/concat_ws2.py -P -R
python3 ./test.py -f 2-query/cos.py -P
python3 ./test.py -f 2-query/cos.py -P -R
python3 ./test.py -f 2-query/count_partition.py -P
python3 ./test.py -f 2-query/count_partition.py -P -R
python3 ./test.py -f 2-query/count.py -P
python3 ./test.py -f 2-query/count.py -P -R
python3 ./test.py -f 2-query/countAlwaysReturnValue.py -P
python3 ./test.py -f 2-query/countAlwaysReturnValue.py -P -R
python3 ./test.py -f 2-query/db.py -P 
python3 ./test.py -f 2-query/diff.py -P
python3 ./test.py -f 2-query/diff.py -P -R
python3 ./test.py -f 2-query/distinct.py -P
python3 ./test.py -f 2-query/distinct.py -P -R
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -P
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -P -R
python3 ./test.py -f 2-query/distribute_agg_avg.py -P
python3 ./test.py -f 2-query/distribute_agg_avg.py -P -R
python3 ./test.py -f 2-query/distribute_agg_count.py -P
python3 ./test.py -f 2-query/distribute_agg_count.py -P -R
python3 ./test.py -f 2-query/distribute_agg_max.py -P
python3 ./test.py -f 2-query/distribute_agg_max.py -P -R
python3 ./test.py -f 2-query/distribute_agg_min.py -P
python3 ./test.py -f 2-query/distribute_agg_min.py -P -R
python3 ./test.py -f 2-query/distribute_agg_spread.py -P
python3 ./test.py -f 2-query/distribute_agg_spread.py -P -R
python3 ./test.py -f 2-query/distribute_agg_stddev.py -P
python3 ./test.py -f 2-query/distribute_agg_stddev.py -P -R
python3 ./test.py -f 2-query/distribute_agg_sum.py -P
python3 ./test.py -f 2-query/distribute_agg_sum.py -P -R
python3 ./test.py -f 2-query/explain.py -P
python3 ./test.py -f 2-query/explain.py -P -R
python3 ./test.py -f 2-query/first.py -P
python3 ./test.py -f 2-query/first.py -P -R
python3 ./test.py -f 2-query/floor.py -P
python3 ./test.py -f 2-query/floor.py -P -R
python3 ./test.py -f 2-query/function_null.py -P
python3 ./test.py -f 2-query/function_null.py -P -R
python3 ./test.py -f 2-query/function_stateduration.py -P
python3 ./test.py -f 2-query/function_stateduration.py -P -R
python3 ./test.py -f 2-query/histogram.py -P
python3 ./test.py -f 2-query/histogram.py -P -R
python3 ./test.py -f 2-query/hyperloglog.py -P
python3 ./test.py -f 2-query/hyperloglog.py -P -R
python3 ./test.py -f 2-query/interp.py -P
python3 ./test.py -f 2-query/interp.py -P -R
python3 ./test.py -f 2-query/fill.py -P
python3 ./test.py -f 2-query/irate.py -P
python3 ./test.py -f 2-query/irate.py -P -R
python3 ./test.py -f 2-query/join.py -P
python3 ./test.py -f 2-query/join.py -P -R
python3 ./test.py -f 2-query/last_row.py -P
python3 ./test.py -f 2-query/last_row.py -P -R
python3 ./test.py -f 2-query/last.py -P
python3 ./test.py -f 2-query/last.py -P -R
python3 ./test.py -f 2-query/leastsquares.py -P
python3 ./test.py -f 2-query/leastsquares.py -P -R
python3 ./test.py -f 2-query/length.py -P
python3 ./test.py -f 2-query/length.py -P -R
python3 ./test.py -f 2-query/limit.py -P
python3 ./test.py -f 2-query/log.py -P
python3 ./test.py -f 2-query/log.py -P -R
python3 ./test.py -f 2-query/lower.py -P
python3 ./test.py -f 2-query/lower.py -P -R
python3 ./test.py -f 2-query/ltrim.py -P
python3 ./test.py -f 2-query/ltrim.py -P -R
python3 ./test.py -f 2-query/mavg.py -P
python3 ./test.py -f 2-query/mavg.py -P -R
python3 ./test.py -f 2-query/max_partition.py -P
python3 ./test.py -f 2-query/max_partition.py -P -R
python3 ./test.py -f 2-query/max_min_last_interval.py -P
python3 ./test.py -f 2-query/last_row_interval.py -P
python3 ./test.py -f 2-query/max.py -P
python3 ./test.py -f 2-query/max.py -P -R
python3 ./test.py -f 2-query/min.py -P
python3 ./test.py -f 2-query/min.py -P -R
python3 ./test.py -f 2-query/mode.py -P
python3 ./test.py -f 2-query/mode.py -P -R
python3 ./test.py -f 2-query/Now.py -P
python3 ./test.py -f 2-query/Now.py -P -R
python3 ./test.py -f 2-query/percentile.py -P
python3 ./test.py -f 2-query/percentile.py -P -R
python3 ./test.py -f 2-query/pow.py -P
python3 ./test.py -f 2-query/pow.py -P -R
python3 ./test.py -f 2-query/query_cols_tags_and_or.py -P
python3 ./test.py -f 2-query/query_cols_tags_and_or.py -P -R
python3 ./test.py -f 2-query/round.py -P
python3 ./test.py -f 2-query/round.py -P -R
python3 ./test.py -f 2-query/rtrim.py -P
python3 ./test.py -f 2-query/rtrim.py -P -R
python3 ./test.py -f 1-insert/drop.py -P -N 3 -M 3 -i False -n 3
python3 ./test.py -f 7-tmq/tmqUpdateWithConsume.py -P -N 3 -n 3
python3 ./test.py -f 2-query/db.py -P -N 3 -n 3 -R
python3 ./test.py -f 2-query/stablity.py -P
python3 ./test.py -f 2-query/stablity_1.py -P
python3 ./test.py -f 2-query/elapsed.py -P
python3 ./test.py -f 2-query/csum.py -P
python3 ./test.py -f 2-query/function_diff.py -P
python3 ./test.py -f 2-query/tagFilter.py -P
python3 ./test.py -f 2-query/projectionDesc.py -P
python3 ./test.py -f 2-query/queryQnode.py -P
python3 ./test.py -f 6-cluster/5dnode1mnode.py -P


# -N 4
echo " **********  -N 4 *************"
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_createDb_replica1.py -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas.py -P -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica1_insertdatas_querys.py -P -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas.py -P -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_insertdatas_querys.py -P -N 4 -M 1
python3 ./test.py -f 6-cluster/vnode/4dnode1mnode_basic_replica3_vgroups.py -P -N 4 -M 1
python3 ./test.py -f 2-query/varchar.py -P -R
python3 ./test.py -f 2-query/case_when.py -P 
python3 ./test.py -f 2-query/case_when.py -P -R
python3 ./test.py -f 2-query/blockSMA.py -P
python3 ./test.py -f 2-query/blockSMA.py -P -R
python3 ./test.py -f 2-query/projectionDesc.py -P
python3 ./test.py -f 2-query/projectionDesc.py -P -R
python3 ./test.py -f 1-insert/update_data.py -P
python3 ./test.py -f 1-insert/tb_100w_data_order.py -P
python3 ./test.py -f 1-insert/delete_childtable.py -P
python3 ./test.py -f 1-insert/delete_normaltable.py -P
python3 ./test.py -f 1-insert/keep_expired.py -P
python3 ./test.py -f 1-insert/stmt_error.py -P
python3 ./test.py -f 1-insert/drop.py -P
python3 ./test.py -f 2-query/join2.py -P
python3 ./test.py -f 2-query/union1.py -P
python3 ./test.py -f 2-query/concat2.py -P
python3 ./test.py -f 2-query/json_tag.py -P
python3 ./test.py -f 2-query/nestedQueryInterval.py -P


# -N 5
echo " **********  -N 5 *************"
python3 ./test.py -f 6-cluster/5dnode2mnode.py -P -N 5
python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -P -N 5 -M 3 -i False
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -P -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -P -N 5 -M 3 -i False
python3 ./test.py -f 6-cluster/5dnode3mnodeStopLoop.py -P -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeRecreateMnode.py -P -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStopFollowerLeader.py -P -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop2Follower.py -P -N 5 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -P -N 5 -M 3
python3 ./test.py -f 0-others/taosdShell.py -P -N 5 -M 3 -Q 3
python3 ./test.py -f 7-tmq/tmqSubscribeStb-r3.py -P -N 5
python3 ./test.py -f 2-query/timezone.py -P
python3 ./test.py -f 2-query/timezone.py -P -R
python3 ./test.py -f 2-query/To_iso8601.py -P
python3 ./test.py -f 2-query/To_iso8601.py -P -R
python3 ./test.py -f 2-query/To_unixtimestamp.py -P
python3 ./test.py -f 2-query/To_unixtimestamp.py -P -R
python3 ./test.py -f 2-query/Today.py -P
python3 ./test.py -f 2-query/Today.py -P -R
python3 ./test.py -f 2-query/top.py -P
python3 ./test.py -f 2-query/top.py -P -R
python3 ./test.py -f 2-query/tsbsQuery.py -P
python3 ./test.py -f 2-query/tsbsQuery.py -P -R
python3 ./test.py -f 2-query/ttl_comment.py -P
python3 ./test.py -f 2-query/ttl_comment.py -P -R
python3 ./test.py -f 2-query/twa.py -P
python3 ./test.py -f 2-query/twa.py -P -R
python3 ./test.py -f 2-query/union.py -P
python3 ./test.py -f 2-query/union.py -P -R
python3 ./test.py -f 2-query/unique.py -P
python3 ./test.py -f 2-query/unique.py -P -R
python3 ./test.py -f 2-query/upper.py -P
python3 ./test.py -f 2-query/upper.py -P -R
python3 ./test.py -f 2-query/varchar.py -P

# -N6
echo " **********  -N 6 *************"
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateDb.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -P -N 6 -M 3
#python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateDb.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py -P -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateDb.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeModifyMeta.py -P  -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeModifyMeta.py -P  -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -P -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopDnodeCreateStb.py -P -N 6 -M 3 -n 3 
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py -P -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopMnodeCreateStb.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py -P -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeSep1VnodeStopVnodeCreateStb.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertData.py -P -N 6 -M 3
python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertData.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertDataAsync.py -P -N 6 -M 3
#$python3 ./test.py -f 6-cluster/5dnode3mnodeRestartDnodeInsertDataAsync.py -P -N 6 -M 3 -n 3
python3 ./test.py -f 7-tmq/tmq3mnodeSwitch.py -P -N 6 -M 3 
python3 ./test.py -f 7-tmq/tmq3mnodeSwitch.py -P -N 6 -M 3 -n 3 
python3 ./test.py -f 0-others/balance_vgroups_r1.py -P -N 6

python3 ./test.py -f 2-query/sample.py -P
python3 ./test.py -f 2-query/sample.py -P -R
python3 ./test.py -f 2-query/sin.py -P
python3 ./test.py -f 2-query/sin.py -P -R
python3 ./test.py -f 2-query/smaTest.py -P
python3 ./test.py -f 2-query/smaTest.py -P -R
python3 ./test.py -f 2-query/sml.py -P
python3 ./test.py -f 2-query/sml.py -P -R
python3 ./test.py -f 2-query/spread.py -P
python3 ./test.py -f 2-query/spread.py -P -R
python3 ./test.py -f 2-query/sqrt.py -P
python3 ./test.py -f 2-query/sqrt.py -P -R
python3 ./test.py -f 2-query/statecount.py -P
python3 ./test.py -f 2-query/statecount.py -P -R
python3 ./test.py -f 2-query/stateduration.py -P
python3 ./test.py -f 2-query/stateduration.py -P -R
python3 ./test.py -f 2-query/substr.py -P
python3 ./test.py -f 2-query/substr.py -P -R
python3 ./test.py -f 2-query/sum.py -P
python3 ./test.py -f 2-query/sum.py -P -R
python3 ./test.py -f 2-query/tail.py -P
python3 ./test.py -f 2-query/tail.py -P -R
python3 ./test.py -f 2-query/tan.py -P
python3 ./test.py -f 2-query/tan.py -P -R
python3 ./test.py -f 2-query/Timediff.py -P
python3 ./test.py -f 2-query/Timediff.py -P -R
python3 ./test.py -f 2-query/timetruncate.py -P
python3 ./test.py -f 2-query/timetruncate.py -P -R

# N-7
echo " **********  -N 7 *************"
python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -N 7 -M 3 -C 6
python3 ./test.py -f 6-cluster/5dnode3mnodeAdd1Ddnoe.py -P -N 7 -M 3 -C 6 -n 3

python3 ./test.py -f 2-query/between.py -P -Q 2
python3 ./test.py -f 2-query/distinct.py -P -Q 2
python3 ./test.py -f 2-query/varchar.py -P -Q 2
python3 ./test.py -f 2-query/ltrim.py -P -Q 2
python3 ./test.py -f 2-query/rtrim.py -P -Q 2
python3 ./test.py -f 2-query/length.py -P -Q 2
python3 ./test.py -f 2-query/char_length.py -P -Q 2
python3 ./test.py -f 2-query/upper.py -P -Q 2
python3 ./test.py -f 2-query/lower.py -P -Q 2
python3 ./test.py -f 2-query/join.py -P -Q 2
python3 ./test.py -f 2-query/join2.py -P -Q 2
python3 ./test.py -f 2-query/cast.py -P -Q 2
python3 ./test.py -f 2-query/substr.py -P -Q 2
python3 ./test.py -f 2-query/union.py -P -Q 2
python3 ./test.py -f 2-query/union1.py -P -Q 2
python3 ./test.py -f 2-query/concat.py -P -Q 2
python3 ./test.py -f 2-query/concat2.py -P -Q 2
python3 ./test.py -f 2-query/concat_ws.py -P -Q 2
python3 ./test.py -f 2-query/concat_ws2.py -P -Q 2
python3 ./test.py -f 2-query/check_tsdb.py -P -Q 2
python3 ./test.py -f 2-query/spread.py -P -Q 2
python3 ./test.py -f 2-query/hyperloglog.py -P -Q 2
python3 ./test.py -f 2-query/explain.py -P -Q 2
python3 ./test.py -f 2-query/leastsquares.py -P -Q 2
python3 ./test.py -f 2-query/timezone.py -P -Q 2
python3 ./test.py -f 2-query/Now.py -P -Q 2
python3 ./test.py -f 2-query/Today.py -P -Q 2
python3 ./test.py -f 2-query/max.py -P -Q 2
python3 ./test.py -f 2-query/min.py -P -Q 2
python3 ./test.py -f 2-query/mode.py -P -Q 2
python3 ./test.py -f 2-query/count.py -P -Q 2
python3 ./test.py -f 2-query/countAlwaysReturnValue.py -P -Q 2
python3 ./test.py -f 2-query/last.py -P -Q 2
python3 ./test.py -f 2-query/first.py -P -Q 2
python3 ./test.py -f 2-query/To_iso8601.py -P -Q 2
python3 ./test.py -f 2-query/To_unixtimestamp.py -P -Q 2
python3 ./test.py -f 2-query/timetruncate.py -P -Q 2
python3 ./test.py -f 2-query/diff.py -P -Q 2
python3 ./test.py -f 2-query/Timediff.py -P -Q 2
python3 ./test.py -f 2-query/json_tag.py -P -Q 2
python3 ./test.py -f 2-query/top.py -P -Q 2
python3 ./test.py -f 2-query/bottom.py -P -Q 2
python3 ./test.py -f 2-query/percentile.py -P -Q 2
python3 ./test.py -f 2-query/apercentile.py -P -Q 2
python3 ./test.py -f 2-query/abs.py -P -Q 2
python3 ./test.py -f 2-query/ceil.py -P -Q 2
python3 ./test.py -f 2-query/floor.py -P -Q 2
python3 ./test.py -f 2-query/round.py -P -Q 2
python3 ./test.py -f 2-query/log.py -P -Q 2
python3 ./test.py -f 2-query/pow.py -P -Q 2
python3 ./test.py -f 2-query/sqrt.py -P -Q 2
python3 ./test.py -f 2-query/sin.py -P -Q 2
python3 ./test.py -f 2-query/cos.py -P -Q 2
python3 ./test.py -f 2-query/tan.py -P -Q 2
python3 ./test.py -f 2-query/arcsin.py -P -Q 2
python3 ./test.py -f 2-query/arccos.py -P -Q 2
python3 ./test.py -f 2-query/arctan.py -P -Q 2
python3 ./test.py -f 2-query/query_cols_tags_and_or.py -P -Q 2
python3 ./test.py -f 2-query/interp.py -P -Q 2
python3 ./test.py -f 2-query/fill.py -P -Q 2
python3 ./test.py -f 2-query/nestedQueryInterval.py -P -Q 2
python3 ./test.py -f 2-query/stablity.py -P -Q 2
python3 ./test.py -f 2-query/stablity_1.py -P -Q 2
python3 ./test.py -f 2-query/avg.py -P -Q 2
python3 ./test.py -f 2-query/elapsed.py -P -Q 2
python3 ./test.py -f 2-query/csum.py -P -Q 2
python3 ./test.py -f 2-query/mavg.py -P -Q 2
python3 ./test.py -f 2-query/sample.py -P -Q 2
python3 ./test.py -f 2-query/function_diff.py -P -Q 2
python3 ./test.py -f 2-query/unique.py -P -Q 2
python3 ./test.py -f 2-query/stateduration.py -P -Q 2
python3 ./test.py -f 2-query/function_stateduration.py -P -Q 2
python3 ./test.py -f 2-query/statecount.py -P -Q 2
python3 ./test.py -f 2-query/tail.py -P -Q 2
python3 ./test.py -f 2-query/ttl_comment.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_count.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_max.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_min.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_sum.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_spread.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_avg.py -P -Q 2
python3 ./test.py -f 2-query/distribute_agg_stddev.py -P -Q 2
python3 ./test.py -f 2-query/twa.py -P -Q 2
python3 ./test.py -f 2-query/irate.py -P -Q 2
python3 ./test.py -f 2-query/function_null.py -P -Q 2
python3 ./test.py -f 2-query/count_partition.py -P -Q 2
python3 ./test.py -f 2-query/max_partition.py -P -Q 2
python3 ./test.py -f 2-query/max_min_last_interval.py -P -Q 2
python3 ./test.py -f 2-query/last_row_interval.py -P -Q 2
python3 ./test.py -f 2-query/last_row.py -P -Q 2
python3 ./test.py -f 2-query/tsbsQuery.py -P -Q 2
python3 ./test.py -f 2-query/sml.py -P -Q 2
python3 ./test.py -f 2-query/case_when.py -P -Q 2
python3 ./test.py -f 2-query/blockSMA.py -P -Q 2
python3 ./test.py -f 2-query/projectionDesc.py -P -Q 2
python3 ./test.py -f 99-TDcase/TD-21561.py -P -Q 2

python3 ./test.py -f 2-query/between.py -P -Q 3
python3 ./test.py -f 2-query/distinct.py -P -Q 3
python3 ./test.py -f 2-query/varchar.py -P -Q 3
python3 ./test.py -f 2-query/ltrim.py -P -Q 3
python3 ./test.py -f 2-query/rtrim.py -P -Q 3
python3 ./test.py -f 2-query/length.py -P -Q 3
python3 ./test.py -f 2-query/char_length.py -P -Q 3
python3 ./test.py -f 2-query/upper.py -P -Q 3
python3 ./test.py -f 2-query/lower.py -P -Q 3
python3 ./test.py -f 2-query/join.py -P -Q 3
python3 ./test.py -f 2-query/join2.py -P -Q 3
python3 ./test.py -f 2-query/cast.py -P -Q 3
python3 ./test.py -f 2-query/substr.py -P -Q 3
python3 ./test.py -f 2-query/union.py -P -Q 3
python3 ./test.py -f 2-query/union1.py -P -Q 3
python3 ./test.py -f 2-query/concat2.py -P -Q 3
python3 ./test.py -f 2-query/concat_ws.py -P -Q 3
python3 ./test.py -f 2-query/concat_ws2.py -P -Q 3
python3 ./test.py -f 2-query/check_tsdb.py -P -Q 3
python3 ./test.py -f 2-query/spread.py -P -Q 3
python3 ./test.py -f 2-query/hyperloglog.py -P -Q 3
python3 ./test.py -f 2-query/explain.py -P -Q 3
python3 ./test.py -f 2-query/leastsquares.py -P -Q 3
python3 ./test.py -f 2-query/timezone.py -P -Q 3
python3 ./test.py -f 2-query/Now.py -P -Q 3
python3 ./test.py -f 2-query/Today.py -P -Q 3
python3 ./test.py -f 2-query/max.py -P -Q 3
python3 ./test.py -f 2-query/min.py -P -Q 3
python3 ./test.py -f 2-query/mode.py -P -Q 3
python3 ./test.py -f 2-query/count.py -P -Q 3
python3 ./test.py -f 2-query/countAlwaysReturnValue.py -P -Q 3
python3 ./test.py -f 2-query/last.py -P -Q 3
python3 ./test.py -f 2-query/first.py -P -Q 3
python3 ./test.py -f 2-query/To_iso8601.py -P -Q 3
python3 ./test.py -f 2-query/To_unixtimestamp.py -P -Q 3
python3 ./test.py -f 2-query/timetruncate.py -P -Q 3
python3 ./test.py -f 2-query/diff.py -P -Q 3
python3 ./test.py -f 2-query/Timediff.py -P -Q 3
python3 ./test.py -f 2-query/json_tag.py -P -Q 3
python3 ./test.py -f 2-query/top.py -P -Q 3
python3 ./test.py -f 2-query/bottom.py -P -Q 3
python3 ./test.py -f 2-query/percentile.py -P -Q 3
python3 ./test.py -f 2-query/apercentile.py -P -Q 3
python3 ./test.py -f 2-query/abs.py -P -Q 3
python3 ./test.py -f 2-query/ceil.py -P -Q 3
python3 ./test.py -f 2-query/floor.py -P -Q 3
python3 ./test.py -f 2-query/round.py -P -Q 3
python3 ./test.py -f 2-query/log.py -P -Q 3
python3 ./test.py -f 2-query/pow.py -P -Q 3
python3 ./test.py -f 2-query/sqrt.py -P -Q 3
python3 ./test.py -f 2-query/sin.py -P -Q 3
python3 ./test.py -f 2-query/cos.py -P -Q 3
python3 ./test.py -f 2-query/tan.py -P -Q 3
python3 ./test.py -f 2-query/arcsin.py -P -Q 3
python3 ./test.py -f 2-query/arccos.py -P -Q 3
python3 ./test.py -f 2-query/arctan.py -P -Q 3
python3 ./test.py -f 2-query/query_cols_tags_and_or.py -P -Q 3
python3 ./test.py -f 2-query/nestedQueryInterval.py -P -Q 3
python3 ./test.py -f 2-query/stablity.py -P -Q 3
python3 ./test.py -f 2-query/stablity_1.py -P -Q 3
python3 ./test.py -f 2-query/avg.py -P -Q 3
python3 ./test.py -f 2-query/elapsed.py -P -Q 3
python3 ./test.py -f 2-query/csum.py -P -Q 3
python3 ./test.py -f 2-query/mavg.py -P -Q 3
python3 ./test.py -f 2-query/sample.py -P -Q 3
python3 ./test.py -f 2-query/function_diff.py -P -Q 3
python3 ./test.py -f 2-query/unique.py -P -Q 3
python3 ./test.py -f 2-query/stateduration.py -P -Q 3
python3 ./test.py -f 2-query/function_stateduration.py -P -Q 3
python3 ./test.py -f 2-query/statecount.py -P -Q 3
python3 ./test.py -f 2-query/tail.py -P -Q 3
python3 ./test.py -f 2-query/ttl_comment.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_count.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_max.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_min.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_sum.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_spread.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_avg.py -P -Q 3
python3 ./test.py -f 2-query/distribute_agg_stddev.py -P -Q 3
python3 ./test.py -f 2-query/twa.py -P -Q 3
python3 ./test.py -f 2-query/irate.py -P -Q 3
python3 ./test.py -f 2-query/function_null.py -P -Q 3
python3 ./test.py -f 2-query/count_partition.py -P -Q 3
python3 ./test.py -f 2-query/max_partition.py -P -Q 3
python3 ./test.py -f 2-query/max_min_last_interval.py -P -Q 3
python3 ./test.py -f 2-query/last_row_interval.py -P -Q 3
python3 ./test.py -f 2-query/last_row.py -P -Q 3
python3 ./test.py -f 2-query/tsbsQuery.py -P -Q 3
python3 ./test.py -f 2-query/sml.py -P -Q 3
python3 ./test.py -f 2-query/interp.py -P -Q 3
python3 ./test.py -f 2-query/fill.py -P -Q 3
python3 ./test.py -f 2-query/case_when.py -P -Q 3
python3 ./test.py -f 2-query/blockSMA.py -P -Q 3
python3 ./test.py -f 2-query/projectionDesc.py -P -Q 3
python3 ./test.py -f 99-TDcase/TD-21561.py -P -Q 3
python3 ./test.py -f 2-query/between.py -P -Q 4
python3 ./test.py -f 2-query/distinct.py -P -Q 4
python3 ./test.py -f 2-query/varchar.py -P -Q 4
python3 ./test.py -f 2-query/ltrim.py -P -Q 4
python3 ./test.py -f 2-query/rtrim.py -P -Q 4
python3 ./test.py -f 2-query/length.py -P -Q 4
python3 ./test.py -f 2-query/char_length.py -P -Q 4
python3 ./test.py -f 2-query/upper.py -P -Q 4
python3 ./test.py -f 2-query/lower.py -P -Q 4
python3 ./test.py -f 2-query/join.py -P -Q 4
python3 ./test.py -f 2-query/join2.py -P -Q 4
python3 ./test.py -f 2-query/substr.py -P -Q 4
python3 ./test.py -f 2-query/union.py -P -Q 4
python3 ./test.py -f 2-query/union1.py -P -Q 4
python3 ./test.py -f 2-query/concat.py -P -Q 4
python3 ./test.py -f 2-query/concat2.py -P -Q 4
python3 ./test.py -f 2-query/concat_ws.py -P -Q 4
python3 ./test.py -f 2-query/concat_ws2.py -P -Q 4
python3 ./test.py -f 2-query/check_tsdb.py -P -Q 4
python3 ./test.py -f 2-query/spread.py -P -Q 4
python3 ./test.py -f 2-query/hyperloglog.py -P -Q 4
python3 ./test.py -f 2-query/explain.py -P -Q 4
python3 ./test.py -f 2-query/leastsquares.py -P -Q 4
python3 ./test.py -f 2-query/timezone.py -P -Q 4
python3 ./test.py -f 2-query/Now.py -P -Q 4
python3 ./test.py -f 2-query/Today.py -P -Q 4
python3 ./test.py -f 2-query/max.py -P -Q 4
python3 ./test.py -f 2-query/min.py -P -Q 4
python3 ./test.py -f 2-query/mode.py -P -Q 4
python3 ./test.py -f 2-query/count.py -P -Q 4
python3 ./test.py -f 2-query/countAlwaysReturnValue.py -P -Q 4
python3 ./test.py -f 2-query/last.py -P -Q 4
python3 ./test.py -f 2-query/first.py -P -Q 4
python3 ./test.py -f 2-query/To_iso8601.py -P -Q 4
python3 ./test.py -f 2-query/To_unixtimestamp.py -P -Q 4
python3 ./test.py -f 2-query/timetruncate.py -P -Q 4
python3 ./test.py -f 2-query/diff.py -P -Q 4
python3 ./test.py -f 2-query/Timediff.py -P -Q 4
python3 ./test.py -f 2-query/json_tag.py -P -Q 4
python3 ./test.py -f 2-query/top.py -P -Q 4
python3 ./test.py -f 2-query/bottom.py -P -Q 4
python3 ./test.py -f 2-query/percentile.py -P -Q 4
python3 ./test.py -f 2-query/apercentile.py -P -Q 4
python3 ./test.py -f 2-query/abs.py -P -Q 4
python3 ./test.py -f 2-query/ceil.py -P -Q 4
python3 ./test.py -f 2-query/floor.py -P -Q 4
python3 ./test.py -f 2-query/round.py -P -Q 4
python3 ./test.py -f 2-query/log.py -P -Q 4
python3 ./test.py -f 2-query/pow.py -P -Q 4
python3 ./test.py -f 2-query/sqrt.py -P -Q 4
python3 ./test.py -f 2-query/sin.py -P -Q 4
python3 ./test.py -f 2-query/cos.py -P -Q 4
python3 ./test.py -f 2-query/tan.py -P -Q 4
python3 ./test.py -f 2-query/arcsin.py -P -Q 4
python3 ./test.py -f 2-query/arccos.py -P -Q 4
python3 ./test.py -f 2-query/arctan.py -P -Q 4
python3 ./test.py -f 2-query/query_cols_tags_and_or.py -P -Q 4
python3 ./test.py -f 2-query/nestedQueryInterval.py -P -Q 4
python3 ./test.py -f 2-query/stablity.py -P -Q 4
python3 ./test.py -f 2-query/stablity_1.py -P -Q 4
python3 ./test.py -f 2-query/avg.py -P -Q 4
python3 ./test.py -f 2-query/elapsed.py -P -Q 4
python3 ./test.py -f 2-query/csum.py -P -Q 4
python3 ./test.py -f 2-query/mavg.py -P -Q 4
python3 ./test.py -f 2-query/sample.py -P -Q 4
python3 ./test.py -f 2-query/cast.py -P -Q 4
python3 ./test.py -f 2-query/function_diff.py -P -Q 4
python3 ./test.py -f 2-query/unique.py -P -Q 4
python3 ./test.py -f 2-query/stateduration.py -P -Q 4
python3 ./test.py -f 2-query/function_stateduration.py -P -Q 4
python3 ./test.py -f 2-query/statecount.py -P -Q 4
python3 ./test.py -f 2-query/tail.py -P -Q 4
python3 ./test.py -f 2-query/ttl_comment.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_count.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_max.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_min.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_sum.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_spread.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_apercentile.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_avg.py -P -Q 4
python3 ./test.py -f 2-query/distribute_agg_stddev.py -P -Q 4
python3 ./test.py -f 2-query/twa.py -P -Q 4
python3 ./test.py -f 2-query/irate.py -P -Q 4
python3 ./test.py -f 2-query/function_null.py -P -Q 4
python3 ./test.py -f 2-query/count_partition.py -P -Q 4
python3 ./test.py -f 2-query/max_partition.py -P -Q 4
python3 ./test.py -f 2-query/max_min_last_interval.py -P -Q 4
python3 ./test.py -f 2-query/last_row_interval.py -P -Q 4
python3 ./test.py -f 2-query/last_row.py -P -Q 4
python3 ./test.py -f 2-query/tsbsQuery.py -P -Q 4
python3 ./test.py -f 2-query/sml.py -P -Q 4
python3 ./test.py -f 2-query/interp.py -P -Q 4
python3 ./test.py -f 2-query/fill.py -P -Q 4
python3 ./test.py -f 2-query/case_when.py -P -Q 4
python3 ./test.py -f 2-query/insert_select.py -P
python3 ./test.py -f 2-query/insert_select.py -P -R
python3 ./test.py -f 2-query/insert_select.py -P -Q 2
python3 ./test.py -f 2-query/insert_select.py -P -Q 3
python3 ./test.py -f 2-query/insert_select.py -P -Q 4
python3 ./test.py -f 2-query/out_of_order.py -P -R
python3 ./test.py -f 2-query/blockSMA.py -P -Q 4
python3 ./test.py -f 2-query/projectionDesc.py -P -Q 4
python3 ./test.py -f 2-query/odbc.py -P
python3 ./test.py -f 99-TDcase/TD-21561.py -P -Q 4
python3 ./test.py -f 99-TDcase/TD-20582.py -P
