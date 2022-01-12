#!/bin/bash
ulimit -c unlimited
#======================p1-start===============
# timezone
python3 ./test.py -f TimeZone/TestCaseTimeZone.py
#stable
python3 ./test.py -f stable/insert.py
python3 ./test.py -f stable/query_after_reset.py
#table
python3 ./test.py -f table/alter_wal0.py
python3 ./test.py -f table/column_name.py
python3 ./test.py -f table/column_num.py
python3 ./test.py -f table/db_table.py
python3 ./test.py -f table/create_sensitive.py
python3 ./test.py -f table/tablename-boundary.py
python3 ./test.py -f table/max_table_length.py
python3 ./test.py -f table/alter_column.py
python3 ./test.py -f table/boundary.py
#python3 ./test.py -f table/create.py
python3 ./test.py -f table/del_stable.py
python3 ./test.py -f table/create_db_from_normal_db.py
# tag
python3 ./test.py -f tag_lite/filter.py
python3 ./test.py -f tag_lite/create-tags-boundary.py
python3 ./test.py -f tag_lite/3.py
python3 ./test.py -f tag_lite/4.py
python3 ./test.py -f tag_lite/5.py
python3 ./test.py -f tag_lite/6.py
python3 ./test.py -f tag_lite/add.py
python3 ./test.py -f tag_lite/bigint.py
python3 ./test.py -f tag_lite/binary_binary.py
python3 ./test.py -f tag_lite/binary.py
python3 ./test.py -f tag_lite/bool_binary.py
python3 ./test.py -f tag_lite/bool_int.py
python3 ./test.py -f tag_lite/bool.py
python3 ./test.py -f tag_lite/change.py
#======================p1-end===============
#======================p2-start===============
python3 ./test.py -f tag_lite/column.py
python3 ./test.py -f tag_lite/commit.py
python3 ./test.py -f tag_lite/create.py
python3 ./test.py -f tag_lite/datatype.py
python3 ./test.py -f tag_lite/datatype-without-alter.py
python3 ./test.py -f tag_lite/delete.py
python3 ./test.py -f tag_lite/double.py
python3 ./test.py -f tag_lite/float.py
python3 ./test.py -f tag_lite/int_binary.py
python3 ./test.py -f tag_lite/int_float.py
python3 ./test.py -f tag_lite/int.py
python3 ./test.py -f tag_lite/set.py
python3 ./test.py -f tag_lite/smallint.py
python3 ./test.py -f tag_lite/tinyint.py
python3 ./test.py -f tag_lite/timestamp.py
python3 ./test.py -f tag_lite/TestModifyTag.py
python3 ./test.py -f tag_lite/unsignedInt.py
python3 ./test.py -f tag_lite/unsignedBigint.py
python3 ./test.py -f tag_lite/unsignedSmallint.py
python3 ./test.py -f tag_lite/unsignedTinyint.py
python3 ./test.py -f tag_lite/alter_tag.py
python3 ./test.py -f tag_lite/drop_auto_create.py
python3 ./test.py -f tag_lite/json_tag_extra.py
#======================p2-end===============
#======================p3-start===============
#query
python3 ./test.py -f query/distinctOneColTb.py
python3 ./test.py -f query/filter.py
python3 ./test.py -f query/filterCombo.py
python3 ./test.py -f query/queryNormal.py
python3 ./test.py -f query/queryError.py
python3 ./test.py -f query/filterAllIntTypes.py
python3 ./test.py -f query/filterFloatAndDouble.py
python3 ./test.py -f query/filterOtherTypes.py
python3 ./test.py -f query/querySort.py
python3 ./test.py -f query/queryJoin.py
python3 ./test.py -f query/select_last_crash.py
python3 ./test.py -f query/queryNullValueTest.py
python3 ./test.py -f query/queryInsertValue.py
python3 ./test.py -f query/queryConnection.py
python3 ./test.py -f query/queryCountCSVData.py
python3 ./test.py -f query/natualInterval.py
python3 ./test.py -f query/bug1471.py
#python3 ./test.py -f query/dataLossTest.py
python3 ./test.py -f query/bug1874.py
python3 ./test.py -f query/bug1875.py
python3 ./test.py -f query/bug1876.py
python3 ./test.py -f query/bug2218.py
python3 ./test.py -f query/bug2117.py
python3 ./test.py -f query/bug2118.py
python3 ./test.py -f query/bug2143.py
python3 ./test.py -f query/sliding.py
python3 ./test.py -f query/unionAllTest.py
python3 ./test.py -f query/bug2281.py
python3 ./test.py -f query/udf.py
python3 ./test.py -f query/bug2119.py
python3 ./test.py -f query/isNullTest.py
python3 ./test.py -f query/queryWithTaosdKilled.py
python3 ./test.py -f query/floatCompare.py
python3 ./test.py -f query/query1970YearsAf.py
python3 ./test.py -f query/bug3351.py
python3 ./test.py -f query/bug3375.py
python3 ./test.py -f query/queryJoin10tables.py
python3 ./test.py -f query/queryStddevWithGroupby.py
python3 ./test.py -f query/querySecondtscolumnTowherenow.py
python3 ./test.py -f query/queryFilterTswithDateUnit.py
python3 ./test.py -f query/queryTscomputWithNow.py
python3 ./test.py -f query/queryStableJoin.py
python3 ./test.py -f query/computeErrorinWhere.py
python3 ./test.py -f query/queryTsisNull.py
python3 ./test.py -f query/subqueryFilter.py
python3 ./test.py -f query/nestedQuery/queryInterval.py
python3 ./test.py -f query/queryStateWindow.py
# python3 ./test.py -f query/nestedQuery/queryWithOrderLimit.py
#======================p3-end===============
#======================p4-start===============
python3 ./test.py -f query/nestquery_last_row.py
python3 ./test.py -f query/nestedQuery/nestedQuery.py
python3 ./test.py -f query/nestedQuery/nestedQuery_datacheck.py
python3 ./test.py -f query/queryCnameDisplay.py
# python3 ./test.py -f query/operator_cost.py
# python3 ./test.py -f query/long_where_query.py
python3 test.py -f query/nestedQuery/queryWithSpread.py
python3 ./test.py -f query/bug6586.py
# python3 ./test.py -f query/bug5903.py
python3 test.py -f query/queryInterval.py
python3 test.py -f query/queryFillTest.py
python3 ./test.py -f query/last_cache.py
python3 ./test.py -f query/last_row_cache.py
python3 ./test.py -f query/queryGroupbySort.py
python3 ./test.py -f query/filterAllUnsignedIntTypes.py
python3 ./test.py -f query/queryBetweenAnd.py
python3 ./test.py -f query/querySession.py
python3 ./test.py -f query/queryWildcardLength.py
python3 ./test.py -f query/queryTbnameUpperLower.py
python3 ./test.py -f query/query.py
python3 ./test.py -f query/queryDiffColsTagsAndOr.py
python3 ./test.py -f query/queryGroupTbname.py
python3 ./test.py -f query/queryRegex.py
#stream
python3 ./test.py -f stream/metric_1.py
python3 ./test.py -f stream/metric_n.py
python3 ./test.py -f stream/new.py
python3 ./test.py -f stream/stream1.py
python3 ./test.py -f stream/stream2.py
#python3 ./test.py -f stream/parser.py
python3 ./test.py -f stream/history.py
#python3 ./test.py -f stream/sys.py
python3 ./test.py -f stream/table_1.py
python3 ./test.py -f stream/table_n.py
python3 ./test.py -f stream/showStreamExecTimeisNull.py
python3 ./test.py -f stream/cqSupportBefore1970.py
python3 ./test.py -f query/queryGroupbyWithInterval.py
python3 queryCount.py
# subscribe
python3 test.py -f subscribe/singlemeter.py
#python3 test.py -f subscribe/stability.py
python3 test.py -f subscribe/supertable.py
#======================p4-end===============
#======================p5-start===============
# functions
python3 ./test.py -f functions/all_null_value.py
python3 ./test.py -f functions/function_avg.py -r 1
python3 ./test.py -f functions/function_bottom.py -r 1
python3 ./test.py -f functions/function_count.py -r 1
python3 ./test.py -f functions/function_count_last_stab.py
python3 ./test.py -f functions/function_diff.py -r 1
python3 ./test.py -f functions/function_first.py -r 1
python3 ./test.py -f functions/function_last.py -r 1
python3 ./test.py -f functions/function_last_row.py -r 1
python3 ./test.py -f functions/function_leastsquares.py -r 1
python3 ./test.py -f functions/function_max.py -r 1
python3 ./test.py -f functions/function_min.py -r 1
python3 ./test.py -f functions/function_operations.py -r 1
python3 ./test.py -f functions/function_percentile.py -r 1
python3 ./test.py -f functions/function_spread.py -r 1
python3 ./test.py -f functions/function_stddev.py -r 1
python3 ./test.py -f functions/function_sum.py -r 1
python3 ./test.py -f functions/function_top.py -r 1
python3 ./test.py -f functions/function_sample.py -r 1
python3 ./test.py -f functions/function_twa.py -r 1
python3 ./test.py -f functions/function_twa_test2.py
python3 ./test.py -f functions/function_stddev_td2555.py
python3 ./test.py -f functions/showOfflineThresholdIs864000.py
python3 ./test.py -f functions/function_interp.py
#python3 ./test.py -f functions/queryTestCases.py
python3 ./test.py -f functions/function_stateWindow.py
python3 ./test.py -f functions/function_derivative.py
python3 ./test.py  -f functions/function_irate.py
python3 ./test.py  -f functions/function_ceil.py
python3 ./test.py  -f functions/function_floor.py
python3 ./test.py  -f functions/function_round.py
python3 ./test.py  -f functions/function_elapsed.py
python3 ./test.py -f functions/function_mavg.py
python3 ./test.py -f functions/function_csum.py
python3 ./test.py -f functions/function_percentile2.py
python3 ./test.py -f functions/variable_httpDbNameMandatory.py
######## system-test
#python3 ./test.py -f ../system-test/2-query/9-others/TD-11389.py # this case will run when this bug fix  TD-11389
#======================p5-end===============
