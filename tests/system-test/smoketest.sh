#!/bin/bash
set -e
set -x

python3 ./test.py -f 0-others/cachemodel.py
python3 ./test.py -f 0-others/fsync.py
python3 ./test.py -f 0-others/sysinfo.py
python3 ./test.py -f 0-others/taosdMonitor.py
python3 ./test.py -f 0-others/taosShellError.py
python3 ./test.py -f 0-others/taosShellNetChk.py
python3 ./test.py -f 0-others/telemetry.py
python3 ./test.py -f 0-others/udf_cfg1.py
python3 ./test.py -f 0-others/udf_cfg2.py
python3 ./test.py -f 0-others/udf_create.py
python3 ./test.py -f 0-others/udf_restart_taosd.py
python3 ./test.py -f 0-others/udfTest.py

python3 ./test.py -f 1-insert/alter_stable.py
python3 ./test.py -f 1-insert/alter_table.py
python3 ./test.py -f 1-insert/block_wise.py
python3 ./test.py -f 1-insert/create_retentions.py
python3 ./test.py -f 1-insert/database_pre_suf.py
python3 ./test.py -f 1-insert/db_tb_name_check.py
python3 ./test.py -f 1-insert/delete_data.py
python3 ./test.py -f 1-insert/influxdb_line_taosc_insert.py
python3 ./test.py -f 1-insert/insertWithMoreVgroup.py
python3 ./test.py -f 1-insert/opentsdb_json_taosc_insert.py
python3 ./test.py -f 1-insert/opentsdb_telnet_line_taosc_insert.py
python3 ./test.py -f 1-insert/table_comment.py
python3 ./test.py -f 1-insert/table_param_ttl.py
##python3 ./test.py -f 1-insert/table_param_ttl.py -R
python3 ./test.py -f 1-insert/test_stmt_muti_insert_query.py
python3 ./test.py -f 1-insert/test_stmt_set_tbname_tag.py
python3 ./test.py -f 1-insert/time_range_wise.py
python3 ./test.py -f 1-insert/update_data_muti_rows.py
python3 ./test.py -f 1-insert/update_data.py

python3 ./test.py -f 2-query/abs.py
##python3 ./test.py -f 2-query/abs.py -R
python3 ./test.py -f 2-query/and_or_for_byte.py
##python3 ./test.py -f 2-query/and_or_for_byte.py -R
python3 ./test.py -f 2-query/apercentile.py
#python3 ./test.py -f 2-query/apercentile.py -R
python3 ./test.py -f 2-query/arccos.py
#python3 ./test.py -f 2-query/arccos.py -R
python3 ./test.py -f 2-query/arcsin.py
#python3 ./test.py -f 2-query/arcsin.py -R
python3 ./test.py -f 2-query/arctan.py
#python3 ./test.py -f 2-query/arctan.py -R
python3 ./test.py -f 2-query/avg.py
#python3 ./test.py -f 2-query/avg.py -R
python3 ./test.py -f 2-query/between.py
#python3 ./test.py -f 2-query/between.py -R
python3 ./test.py -f 2-query/bottom.py
#python3 ./test.py -f 2-query/bottom.py -R
python3 ./test.py -f 2-query/cast.py
#python3 ./test.py -f 2-query/cast.py -R
python3 ./test.py -f 2-query/ceil.py
#python3 ./test.py -f 2-query/ceil.py -R
python3 ./test.py -f 2-query/char_length.py
#python3 ./test.py -f 2-query/char_length.py -R
python3 ./test.py -f 2-query/check_tsdb.py
#python3 ./test.py -f 2-query/check_tsdb.py -R
python3 ./test.py -f 2-query/concat_ws.py
#python3 ./test.py -f 2-query/concat_ws.py -R
python3 ./test.py -f 2-query/concat_ws2.py
#python3 ./test.py -f 2-query/concat_ws2.py -R
python3 ./test.py -f 2-query/concat.py
#python3 ./test.py -f 2-query/concat.py -R
python3 ./test.py -f 2-query/concat2.py
python3 ./test.py -f 2-query/cos.py
#python3 ./test.py -f 2-query/cos.py -R
python3 ./test.py -f 2-query/count_partition.py
#python3 ./test.py -f 2-query/count_partition.py -R
python3 ./test.py -f 2-query/count.py
#python3 ./test.py -f 2-query/count.py -R
python3 ./test.py -f 2-query/csum.py
python3 ./test.py -f 2-query/db.py
#python3 ./test.py -f 2-query/db.py -R
python3 ./test.py -f 2-query/diff.py
#python3 ./test.py -f 2-query/diff.py -R
python3 ./test.py -f 2-query/distinct.py
#python3 ./test.py -f 2-query/distinct.py -R
python3 ./test.py -f 2-query/distribute_agg_apercentile.py
#python3 ./test.py -f 2-query/distribute_agg_apercentile.py -R
python3 ./test.py -f 2-query/distribute_agg_avg.py
#python3 ./test.py -f 2-query/distribute_agg_avg.py -R
python3 ./test.py -f 2-query/distribute_agg_count.py
#python3 ./test.py -f 2-query/distribute_agg_count.py -R
python3 ./test.py -f 2-query/distribute_agg_max.py
#python3 ./test.py -f 2-query/distribute_agg_max.py -R
python3 ./test.py -f 2-query/distribute_agg_min.py
#python3 ./test.py -f 2-query/distribute_agg_min.py -R
python3 ./test.py -f 2-query/distribute_agg_spread.py
#python3 ./test.py -f 2-query/distribute_agg_spread.py -R
python3 ./test.py -f 2-query/distribute_agg_stddev.py
#python3 ./test.py -f 2-query/distribute_agg_stddev.py -R
python3 ./test.py -f 2-query/distribute_agg_sum.py
#python3 ./test.py -f 2-query/distribute_agg_sum.py -R
python3 ./test.py -f 2-query/elapsed.py
python3 ./test.py -f 2-query/explain.py
#python3 ./test.py -f 2-query/explain.py -R
python3 ./test.py -f 2-query/first.py
#python3 ./test.py -f 2-query/first.py -R
python3 ./test.py -f 2-query/floor.py
#python3 ./test.py -f 2-query/floor.py -R
python3 ./test.py -f 2-query/function_diff.py
python3 ./test.py -f 2-query/function_null.py
#python3 ./test.py -f 2-query/function_null.py -R
python3 ./test.py -f 2-query/function_stateduration.py
#python3 ./test.py -f 2-query/function_stateduration.py -R
python3 ./test.py -f 2-query/geometry.py
#python3 ./test.py -f 2-query/geometry.py -R
python3 ./test.py -f 2-query/histogram.py
#python3 ./test.py -f 2-query/histogram.py -R
python3 ./test.py -f 2-query/hyperloglog.py
#python3 ./test.py -f 2-query/hyperloglog.py -R
python3 ./test.py -f 2-query/interp.py
#python3 ./test.py -f 2-query/interp.py -R
python3 ./test.py -f 2-query/irate.py
#python3 ./test.py -f 2-query/irate.py -R
python3 ./test.py -f 2-query/join.py
#python3 ./test.py -f 2-query/join.py -R
python3 ./test.py -f 2-query/join2.py
python3 ./test.py -f 2-query/json_tag.py
python3 ./test.py -f 2-query/last_row.py
#python3 ./test.py -f 2-query/last_row.py -R
python3 ./test.py -f 2-query/last.py
#python3 ./test.py -f 2-query/last.py -R
python3 ./test.py -f 2-query/leastsquares.py
#python3 ./test.py -f 2-query/leastsquares.py -R
python3 ./test.py -f 2-query/length.py
#python3 ./test.py -f 2-query/length.py -R
python3 ./test.py -f 2-query/log.py
python3 ./test.py -f 2-query/lower.py
#python3 ./test.py -f 2-query/lower.py -R
python3 ./test.py -f 2-query/ltrim.py
#python3 ./test.py -f 2-query/ltrim.py -R
python3 ./test.py -f 2-query/mavg.py
#python3 ./test.py -f 2-query/mavg.py -R
python3 ./test.py -f 2-query/max_partition.py
#python3 ./test.py -f 2-query/max_partition.py -R
python3 ./test.py -f 2-query/max.py
#python3 ./test.py -f 2-query/max.py -R
python3 ./test.py -f 2-query/min.py
#python3 ./test.py -f 2-query/min.py -R
python3 ./test.py -f 2-query/Now.py
#python3 ./test.py -f 2-query/Now.py -R
python3 ./test.py -f 2-query/percentile.py
#python3 ./test.py -f 2-query/percentile.py -R
python3 ./test.py -f 2-query/pow.py
#python3 ./test.py -f 2-query/pow.py -R
python3 ./test.py -f 2-query/query_cols_tags_and_or.py
#python3 ./test.py -f 2-query/query_cols_tags_and_or.py -R
python3 ./test.py -f 2-query/queryQnode.py
python3 ./test.py -f 2-query/round.py
#python3 ./test.py -f 2-query/round.py -R
python3 ./test.py -f 2-query/rtrim.py
#python3 ./test.py -f 2-query/rtrim.py -R
python3 ./test.py -f 2-query/sample.py
#python3 ./test.py -f 2-query/sample.py -R
python3 ./test.py -f 2-query/sin.py
#python3 ./test.py -f 2-query/sin.py -R
python3 ./test.py -f 2-query/smaTest.py
#python3 ./test.py -f 2-query/smaTest.py -R
python3 ./test.py -f 2-query/sml.py
#python3 ./test.py -f 2-query/sml.py -R
python3 ./test.py -f 2-query/spread.py
#python3 ./test.py -f 2-query/spread.py -R
python3 ./test.py -f 2-query/sqrt.py
#python3 ./test.py -f 2-query/sqrt.py -R
python3 ./test.py -f 2-query/statecount.py
#python3 ./test.py -f 2-query/statecount.py -R
python3 ./test.py -f 2-query/stateduration.py
#python3 ./test.py -f 2-query/stateduration.py -R
python3 ./test.py -f 2-query/substr.py
#python3 ./test.py -f 2-query/substr.py -R
python3 ./test.py -f 2-query/sum.py
#python3 ./test.py -f 2-query/sum.py -R
python3 ./test.py -f 2-query/tail.py
#python3 ./test.py -f 2-query/tail.py -R
python3 ./test.py -f 2-query/tan.py
python3 ./test.py -f 2-query/Timediff.py
#python3 ./test.py -f 2-query/Timediff.py -R
python3 ./test.py -f 2-query/timetruncate.py
python3 ./test.py -f 2-query/timezone.py
#python3 ./test.py -f 2-query/timezone.py -R
python3 ./test.py -f 2-query/To_iso8601.py
#python3 ./test.py -f 2-query/To_iso8601.py -R
python3 ./test.py -f 2-query/To_unixtimestamp.py
#python3 ./test.py -f 2-query/To_unixtimestamp.py -R
python3 ./test.py -f 2-query/Today.py
python3 ./test.py -f 2-query/top.py
#python3 ./test.py -f 2-query/top.py -R
python3 ./test.py -f 2-query/tsbsQuery.py
#python3 ./test.py -f 2-query/tsbsQuery.py -R
python3 ./test.py -f 2-query/ttl_comment.py
#python3 ./test.py -f 2-query/ttl_comment.py -R
python3 ./test.py -f 2-query/twa.py
#python3 ./test.py -f 2-query/twa.py -R
python3 ./test.py -f 2-query/union.py
#python3 ./test.py -f 2-query/union.py -R
python3 ./test.py -f 2-query/union1.py
python3 ./test.py -f 2-query/unique.py
#python3 ./test.py -f 2-query/unique.py -R
python3 ./test.py -f 2-query/upper.py
#python3 ./test.py -f 2-query/upper.py -R
python3 ./test.py -f 2-query/varchar.py
#python3 ./test.py -f 2-query/varchar.py -R
