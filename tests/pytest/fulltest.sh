#!/bin/bash
ulimit -c unlimited
#======================p1-start===============

python3 ./test.py -f insert/basic.py
python3 ./test.py -f insert/int.py
python3 ./test.py -f insert/float.py
python3 ./test.py -f insert/bigint.py
python3 ./test.py -f insert/bool.py
python3 ./test.py -f insert/double.py
python3 ./test.py -f insert/smallint.py
python3 ./test.py -f insert/tinyint.py
python3 ./test.py -f insert/date.py
python3 ./test.py -f insert/binary.py
python3 ./test.py -f insert/nchar.py
#python3 ./test.py -f insert/nchar-boundary.py
python3 ./test.py -f insert/nchar-unicode.py
python3 ./test.py -f insert/multi.py
python3 ./test.py -f insert/randomNullCommit.py
python3 insert/retentionpolicy.py
python3 ./test.py -f insert/alterTableAndInsert.py
python3 ./test.py -f insert/insertIntoTwoTables.py
python3 ./test.py -f insert/before_1970.py
python3 ./test.py -f insert/special_character_show.py
python3 bug2265.py
python3 ./test.py -f insert/bug3654.py
python3 ./test.py -f insert/insertDynamicColBeforeVal.py
python3 ./test.py -f insert/in_function.py
python3 ./test.py -f insert/modify_column.py
python3 ./test.py -f insert/line_insert.py

# timezone 

python3 ./test.py -f TimeZone/TestCaseTimeZone.py

#table
python3 ./test.py -f table/alter_wal0.py
python3 ./test.py -f table/column_name.py
python3 ./test.py -f table/column_num.py
python3 ./test.py -f table/db_table.py
python3 ./test.py -f table/create_sensitive.py
python3 ./test.py -f table/tablename-boundary.py
python3 ./test.py  -f table/max_table_length.py
python3 ./test.py -f table/alter_column.py
python3 ./test.py -f table/boundary.py
python3 ./test.py -f table/create.py
python3 ./test.py -f table/del_stable.py

#stable
python3 ./test.py -f stable/insert.py
python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJsonStmt.py 

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

#python3 ./test.py -f dbmgmt/database-name-boundary.py
python3 test.py -f dbmgmt/nanoSecondCheck.py

python3 ./test.py -f import_merge/importBlock1HO.py
python3 ./test.py -f import_merge/importBlock1HPO.py
python3 ./test.py -f import_merge/importBlock1H.py
python3 ./test.py -f import_merge/importBlock1S.py
python3 ./test.py -f import_merge/importBlock1Sub.py
python3 ./test.py -f import_merge/importBlock1TO.py
python3 ./test.py -f import_merge/importBlock1TPO.py
python3 ./test.py -f import_merge/importBlock1T.py
python3 ./test.py -f import_merge/importBlock2HO.py
python3 ./test.py -f import_merge/importBlock2HPO.py
python3 ./test.py -f import_merge/importBlock2H.py
python3 ./test.py -f import_merge/importBlock2S.py
python3 ./test.py -f import_merge/importBlock2Sub.py
python3 ./test.py -f import_merge/importBlock2TO.py
python3 ./test.py -f import_merge/importBlock2TPO.py
python3 ./test.py -f import_merge/importBlock2T.py
python3 ./test.py -f import_merge/importBlockbetween.py
python3 ./test.py -f import_merge/importCacheFileHO.py
python3 ./test.py -f import_merge/importCacheFileHPO.py
python3 ./test.py -f import_merge/importCacheFileH.py
python3 ./test.py -f import_merge/importCacheFileS.py
python3 ./test.py -f import_merge/importCacheFileSub.py
python3 ./test.py -f import_merge/importCacheFileTO.py
python3 ./test.py -f import_merge/importCacheFileTPO.py
python3 ./test.py -f import_merge/importCacheFileT.py
python3 ./test.py -f import_merge/importDataH2.py
python3 ./test.py -f import_merge/importDataHO2.py
python3 ./test.py -f import_merge/importDataHO.py
python3 ./test.py -f import_merge/importDataHPO.py
python3 ./test.py -f import_merge/importDataLastHO.py
python3 ./test.py -f import_merge/importDataLastHPO.py
python3 ./test.py -f import_merge/importDataLastH.py
python3 ./test.py -f import_merge/importDataLastS.py
python3 ./test.py -f import_merge/importDataLastSub.py
python3 ./test.py -f import_merge/importDataLastTO.py
python3 ./test.py -f import_merge/importDataLastTPO.py
python3 ./test.py -f import_merge/importDataLastT.py
python3 ./test.py -f import_merge/importDataS.py
python3 ./test.py -f import_merge/importDataSub.py
python3 ./test.py -f import_merge/importDataTO.py
python3 ./test.py -f import_merge/importDataTPO.py
python3 ./test.py -f import_merge/importDataT.py
python3 ./test.py -f import_merge/importHeadOverlap.py
python3 ./test.py -f import_merge/importHeadPartOverlap.py
python3 ./test.py -f import_merge/importHead.py
python3 ./test.py -f import_merge/importHORestart.py
python3 ./test.py -f import_merge/importHPORestart.py
python3 ./test.py -f import_merge/importHRestart.py
python3 ./test.py -f import_merge/importLastHO.py
python3 ./test.py -f import_merge/importLastHPO.py
python3 ./test.py -f import_merge/importLastH.py
python3 ./test.py -f import_merge/importLastS.py
python3 ./test.py -f import_merge/importLastSub.py
python3 ./test.py -f import_merge/importLastTO.py
python3 ./test.py -f import_merge/importLastTPO.py
python3 ./test.py -f import_merge/importLastT.py
python3 ./test.py -f import_merge/importSpan.py
python3 ./test.py -f import_merge/importSRestart.py
python3 ./test.py -f import_merge/importSubRestart.py
python3 ./test.py -f import_merge/importTailOverlap.py
python3 ./test.py -f import_merge/importTailPartOverlap.py
python3 ./test.py -f import_merge/importTail.py
python3 ./test.py -f import_merge/importToCommit.py
python3 ./test.py -f import_merge/importTORestart.py
python3 ./test.py -f import_merge/importTPORestart.py
python3 ./test.py -f import_merge/importTRestart.py
python3 ./test.py -f import_merge/importInsertThenImport.py
python3 ./test.py -f import_merge/importCSV.py
python3 ./test.py -f import_merge/import_update_0.py
python3 ./test.py -f import_merge/import_update_1.py
python3 ./test.py -f import_merge/import_update_2.py
python3 ./test.py -f update/merge_commit_data.py
#======================p1-end===============
#======================p2-start===============
# tools
python3 test.py -f tools/taosdumpTest.py
python3 test.py -f tools/taosdumpTest2.py

python3 test.py -f tools/taosdemoTest.py
python3 test.py -f tools/taosdemoTestWithoutMetric.py
python3 test.py -f tools/taosdemoTestWithJson.py
python3 test.py -f tools/taosdemoTestLimitOffset.py
python3 test.py -f tools/taosdemoTestTblAlt.py
python3 test.py -f tools/taosdemoTestSampleData.py
python3 test.py -f tools/taosdemoTestInterlace.py
python3 test.py -f tools/taosdemoTestQuery.py

# restful test for python
python3 test.py -f restful/restful_bind_db1.py
python3 test.py -f restful/restful_bind_db2.py

# nano support
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanoInsert.py
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanoQuery.py
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestSupportNanosubscribe.py
python3 test.py -f tools/taosdemoAllTest/NanoTestCase/taosdemoTestInsertTime_step.py
python3 test.py -f tools/taosdumpTestNanoSupport.py

#
python3 ./test.py -f tsdb/tsdbComp.py 

# update
python3 ./test.py -f update/allow_update.py
python3 ./test.py -f update/allow_update-0.py
python3 ./test.py -f update/append_commit_data.py
python3 ./test.py -f update/append_commit_last-0.py
python3 ./test.py -f update/append_commit_last.py


python3 ./test.py -f update/merge_commit_data2.py
python3 ./test.py -f update/merge_commit_data2_update0.py
python3 ./test.py -f update/merge_commit_last-0.py
python3 ./test.py -f update/merge_commit_last.py
python3 ./test.py -f update/bug_td2279.py

#======================p2-end===============
#======================p3-start===============


# user
python3 ./test.py -f user/user_create.py
python3 ./test.py -f user/pass_len.py

# stable
python3 ./test.py -f stable/query_after_reset.py

# perfbenchmark
python3 ./test.py -f perfbenchmark/bug3433.py
#python3 ./test.py -f perfbenchmark/bug3589.py
python3 ./test.py -f perfbenchmark/taosdemoInsert.py

#taosdemo
python3 test.py -f tools/taosdemoAllTest/taosdemoTestInsertWithJson.py 
python3 test.py -f tools/taosdemoAllTest/taosdemoTestQueryWithJson.py

#query
python3 test.py -f query/distinctOneColTb.py 
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
python3 ./test.py -f query/nestquery_last_row.py
python3 ./test.py -f query/queryCnameDisplay.py
# python3 ./test.py -f query/operator_cost.py
# python3 ./test.py -f query/long_where_query.py
python3 test.py -f query/nestedQuery/queryWithSpread.py

#stream
python3 ./test.py -f stream/metric_1.py
python3 ./test.py -f stream/metric_n.py
python3 ./test.py -f stream/new.py
python3 ./test.py -f stream/stream1.py
python3 ./test.py -f stream/stream2.py
#python3 ./test.py -f stream/parser.py
python3 ./test.py -f stream/history.py
python3 ./test.py -f stream/sys.py
python3 ./test.py -f stream/table_1.py
python3 ./test.py -f stream/table_n.py
python3 ./test.py -f stream/showStreamExecTimeisNull.py
python3 ./test.py -f stream/cqSupportBefore1970.py

#alter table
python3 ./test.py -f alter/alter_table_crash.py
python3 ./test.py -f alter/alterTabAddTagWithNULL.py
python3 ./test.py -f alter/alterTimestampColDataProcess.py

# client
python3 ./test.py -f client/client.py
python3 ./test.py -f client/version.py
python3 ./test.py -f client/alterDatabase.py
python3 ./test.py -f client/noConnectionErrorTest.py
# python3 test.py -f client/change_time_1_1.py
# python3 test.py -f client/change_time_1_2.py

# Misc
python3 testCompress.py
python3 testNoCompress.py
python3 testMinTablesPerVnode.py
python3 queryCount.py
python3 ./test.py -f query/queryGroupbyWithInterval.py
python3 client/twoClients.py
python3 test.py -f query/queryInterval.py
python3 test.py -f query/queryFillTest.py
# subscribe
python3 test.py -f subscribe/singlemeter.py
#python3 test.py -f subscribe/stability.py  
python3 test.py -f subscribe/supertable.py
# topic
python3 ./test.py -f topic/topicQuery.py
#======================p3-end===============
#======================p4-start===============

python3 ./test.py -f update/merge_commit_data-0.py
# wal
python3 ./test.py -f wal/addOldWalTest.py
python3 ./test.py -f wal/sdbComp.py 

# function
python3 ./test.py -f functions/all_null_value.py
# functions
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
python3 ./test.py -f functions/function_twa.py -r 1
python3 ./test.py -f functions/function_twa_test2.py
python3 ./test.py -f functions/function_stddev_td2555.py
python3 ./test.py -f functions/showOfflineThresholdIs864000.py
python3 ./test.py -f functions/function_interp.py
python3 ./test.py -f insert/metadataUpdate.py
python3 ./test.py -f query/last_cache.py
python3 ./test.py -f query/last_row_cache.py
python3 ./test.py -f account/account_create.py
python3 ./test.py -f alter/alter_table.py
python3 ./test.py -f query/queryGroupbySort.py
python3 ./test.py -f functions/queryTestCases.py
python3 ./test.py -f functions/function_stateWindow.py
python3 ./test.py -f functions/function_derivative.py
python3 ./test.py  -f functions/function_irate.py

python3 ./test.py -f insert/unsignedInt.py
python3 ./test.py -f insert/unsignedBigint.py
python3 ./test.py -f insert/unsignedSmallint.py
python3 ./test.py -f insert/unsignedTinyint.py
python3 ./test.py -f insert/insertFromCSV.py
python3 ./test.py -f query/filterAllUnsignedIntTypes.py

python3 ./test.py -f tag_lite/unsignedInt.py
python3 ./test.py -f tag_lite/unsignedBigint.py
python3 ./test.py -f tag_lite/unsignedSmallint.py
python3 ./test.py -f tag_lite/unsignedTinyint.py

python3 ./test.py -f functions/function_percentile2.py
python3 ./test.py -f insert/boundary2.py
python3 ./test.py -f insert/insert_locking.py
python3 ./test.py -f alter/alter_debugFlag.py
python3 ./test.py -f query/queryBetweenAnd.py
python3 ./test.py -f tag_lite/alter_tag.py
python3 test.py -f tools/taosdemoAllTest/TD-4985/query-limit-offset.py
python3 test.py -f tools/taosdemoAllTest/TD-5213/insert4096columns_not_use_taosdemo.py
python3 test.py -f tools/taosdemoAllTest/TD-5213/insertSigcolumnsNum4096.py
python3 ./test.py -f tag_lite/drop_auto_create.py
python3 test.py -f insert/insert_before_use_db.py
python3 test.py -f alter/alter_keep.py
python3 test.py -f alter/alter_cacheLastRow.py
python3 ./test.py -f query/querySession.py 
python3 test.py -f  alter/alter_create_exception.py
python3 ./test.py -f insert/flushwhiledrop.py
python3 ./test.py -f insert/schemalessInsert.py
python3 ./test.py -f alter/alterColMultiTimes.py
python3 ./test.py -f query/queryWildcardLength.py
python3 ./test.py -f query/queryTbnameUpperLower.py
python3 ./test.py -f query/query.py
python3 ./test.py -f query/queryDiffColsOr.py
#======================p4-end===============









