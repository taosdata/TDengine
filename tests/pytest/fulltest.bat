#!\bin\bash
ulimit -c unlimited
#======================p1-start===============

python .\test.py -f insert\basic.py
python .\test.py -f insert\int.py
python .\test.py -f insert\float.py
python .\test.py -f insert\bigint.py
python .\test.py -f insert\bool.py
python .\test.py -f insert\double.py
python .\test.py -f insert\smallint.py
python .\test.py -f insert\tinyint.py
python .\test.py -f insert\date.py
python .\test.py -f insert\binary.py
python .\test.py -f insert\nchar.py
#python .\test.py -f insert\nchar-boundary.py
python .\test.py -f insert\nchar-unicode.py
python .\test.py -f insert\multi.py
python .\test.py -f insert\randomNullCommit.py
python insert\retentionpolicy.py
python .\test.py -f insert\alterTableAndInsert.py
python .\test.py -f insert\insertIntoTwoTables.py
python .\test.py -f insert\before_1970.py
python .\test.py -f insert\special_character_show.py
python bug2265.py
python .\test.py -f insert\bug3654.py
python .\test.py -f insert\insertDynamicColBeforeVal.py
python .\test.py -f insert\in_function.py
python .\test.py -f insert\modify_column.py
python .\test.py -f insert\line_insert.py

# timezone 

python .\test.py -f TimeZone\TestCaseTimeZone.py

#table
python .\test.py -f table\alter_wal0.py
python .\test.py -f table\column_name.py
python .\test.py -f table\column_num.py
python .\test.py -f table\db_table.py
python .\test.py -f table\create_sensitive.py
python .\test.py -f table\tablename-boundary.py
python .\test.py  -f table\max_table_length.py
python .\test.py -f table\alter_column.py
python .\test.py -f table\boundary.py
python .\test.py -f table\create.py
python .\test.py -f table\del_stable.py

#stable
python .\test.py -f stable\insert.py
python test.py -f tools\taosdemoAllTest\taosdemoTestInsertWithJsonStmt.py 

# tag
python .\test.py -f tag_lite\filter.py
python .\test.py -f tag_lite\create-tags-boundary.py
python .\test.py -f tag_lite\3.py
python .\test.py -f tag_lite\4.py
python .\test.py -f tag_lite\5.py
python .\test.py -f tag_lite\6.py
python .\test.py -f tag_lite\add.py
python .\test.py -f tag_lite\bigint.py
python .\test.py -f tag_lite\binary_binary.py
python .\test.py -f tag_lite\binary.py
python .\test.py -f tag_lite\bool_binary.py
python .\test.py -f tag_lite\bool_int.py
python .\test.py -f tag_lite\bool.py
python .\test.py -f tag_lite\change.py
python .\test.py -f tag_lite\column.py
python .\test.py -f tag_lite\commit.py
python .\test.py -f tag_lite\create.py
python .\test.py -f tag_lite\datatype.py
python .\test.py -f tag_lite\datatype-without-alter.py
python .\test.py -f tag_lite\delete.py
python .\test.py -f tag_lite\double.py
python .\test.py -f tag_lite\float.py
python .\test.py -f tag_lite\int_binary.py
python .\test.py -f tag_lite\int_float.py
python .\test.py -f tag_lite\int.py
python .\test.py -f tag_lite\set.py
python .\test.py -f tag_lite\smallint.py
python .\test.py -f tag_lite\tinyint.py
python .\test.py -f tag_lite\timestamp.py
python .\test.py -f tag_lite\TestModifyTag.py

#python .\test.py -f dbmgmt\database-name-boundary.py
python test.py -f dbmgmt\nanoSecondCheck.py

python .\test.py -f import_merge\importBlock1HO.py
python .\test.py -f import_merge\importBlock1HPO.py
python .\test.py -f import_merge\importBlock1H.py
python .\test.py -f import_merge\importBlock1S.py
python .\test.py -f import_merge\importBlock1Sub.py
python .\test.py -f import_merge\importBlock1TO.py
python .\test.py -f import_merge\importBlock1TPO.py
python .\test.py -f import_merge\importBlock1T.py
python .\test.py -f import_merge\importBlock2HO.py
python .\test.py -f import_merge\importBlock2HPO.py
python .\test.py -f import_merge\importBlock2H.py
python .\test.py -f import_merge\importBlock2S.py
python .\test.py -f import_merge\importBlock2Sub.py
python .\test.py -f import_merge\importBlock2TO.py
python .\test.py -f import_merge\importBlock2TPO.py
python .\test.py -f import_merge\importBlock2T.py
python .\test.py -f import_merge\importBlockbetween.py
python .\test.py -f import_merge\importCacheFileHO.py
python .\test.py -f import_merge\importCacheFileHPO.py
python .\test.py -f import_merge\importCacheFileH.py
python .\test.py -f import_merge\importCacheFileS.py
python .\test.py -f import_merge\importCacheFileSub.py
python .\test.py -f import_merge\importCacheFileTO.py
python .\test.py -f import_merge\importCacheFileTPO.py
python .\test.py -f import_merge\importCacheFileT.py
python .\test.py -f import_merge\importDataH2.py
python .\test.py -f import_merge\importDataHO2.py
python .\test.py -f import_merge\importDataHO.py
python .\test.py -f import_merge\importDataHPO.py
python .\test.py -f import_merge\importDataLastHO.py
python .\test.py -f import_merge\importDataLastHPO.py
python .\test.py -f import_merge\importDataLastH.py
python .\test.py -f import_merge\importDataLastS.py
python .\test.py -f import_merge\importDataLastSub.py
python .\test.py -f import_merge\importDataLastTO.py
python .\test.py -f import_merge\importDataLastTPO.py
python .\test.py -f import_merge\importDataLastT.py
python .\test.py -f import_merge\importDataS.py
python .\test.py -f import_merge\importDataSub.py
python .\test.py -f import_merge\importDataTO.py
python .\test.py -f import_merge\importDataTPO.py
python .\test.py -f import_merge\importDataT.py
python .\test.py -f import_merge\importHeadOverlap.py
python .\test.py -f import_merge\importHeadPartOverlap.py
python .\test.py -f import_merge\importHead.py
python .\test.py -f import_merge\importHORestart.py
python .\test.py -f import_merge\importHPORestart.py
python .\test.py -f import_merge\importHRestart.py
python .\test.py -f import_merge\importLastHO.py
python .\test.py -f import_merge\importLastHPO.py
python .\test.py -f import_merge\importLastH.py
python .\test.py -f import_merge\importLastS.py
python .\test.py -f import_merge\importLastSub.py
python .\test.py -f import_merge\importLastTO.py
python .\test.py -f import_merge\importLastTPO.py
python .\test.py -f import_merge\importLastT.py
python .\test.py -f import_merge\importSpan.py
python .\test.py -f import_merge\importSRestart.py
python .\test.py -f import_merge\importSubRestart.py
python .\test.py -f import_merge\importTailOverlap.py
python .\test.py -f import_merge\importTailPartOverlap.py
python .\test.py -f import_merge\importTail.py
python .\test.py -f import_merge\importToCommit.py
python .\test.py -f import_merge\importTORestart.py
python .\test.py -f import_merge\importTPORestart.py
python .\test.py -f import_merge\importTRestart.py
python .\test.py -f import_merge\importInsertThenImport.py
python .\test.py -f import_merge\importCSV.py
python .\test.py -f import_merge\import_update_0.py
python .\test.py -f import_merge\import_update_1.py
python .\test.py -f import_merge\import_update_2.py
python .\test.py -f update\merge_commit_data.py
#======================p1-end===============
#======================p2-start===============
# tools
python test.py -f tools\taosdumpTest.py
python test.py -f tools\taosdumpTest2.py

python test.py -f tools\taosdemoTest.py
python test.py -f tools\taosdemoTestWithoutMetric.py
python test.py -f tools\taosdemoTestWithJson.py
python test.py -f tools\taosdemoTestLimitOffset.py
python test.py -f tools\taosdemoTestTblAlt.py
python test.py -f tools\taosdemoTestSampleData.py
python test.py -f tools\taosdemoTestInterlace.py
python test.py -f tools\taosdemoTestQuery.py

# restful test for python
python test.py -f restful\restful_bind_db1.py
python test.py -f restful\restful_bind_db2.py

# nano support
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestSupportNanoInsert.py
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestSupportNanoQuery.py
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestSupportNanosubscribe.py
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestInsertTime_step.py
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdumpTestNanoSupport.py

# update
python .\test.py -f update\allow_update.py
python .\test.py -f update\allow_update-0.py
python .\test.py -f update\append_commit_data.py
python .\test.py -f update\append_commit_last-0.py
python .\test.py -f update\append_commit_last.py


python .\test.py -f update\merge_commit_data2.py
python .\test.py -f update\merge_commit_data2_update0.py
python .\test.py -f update\merge_commit_last-0.py
python .\test.py -f update\merge_commit_last.py
python .\test.py -f update\bug_td2279.py

#======================p2-end===============
#======================p3-start===============


# user
python .\test.py -f user\user_create.py
python .\test.py -f user\pass_len.py

# stable
python .\test.py -f stable\query_after_reset.py

# perfbenchmark
python .\test.py -f perfbenchmark\bug3433.py
#python .\test.py -f perfbenchmark\bug3589.py
python .\test.py -f perfbenchmark\taosdemoInsert.py

#taosdemo
python test.py -f tools\taosdemoAllTest\taosdemoTestInsertWithJson.py 
python test.py -f tools\taosdemoAllTest\taosdemoTestQueryWithJson.py

#query
python test.py -f query\distinctOneColTb.py 
python .\test.py -f query\filter.py
python .\test.py -f query\filterCombo.py
python .\test.py -f query\queryNormal.py
python .\test.py -f query\queryError.py
python .\test.py -f query\filterAllIntTypes.py
python .\test.py -f query\filterFloatAndDouble.py
python .\test.py -f query\filterOtherTypes.py
python .\test.py -f query\querySort.py
python .\test.py -f query\queryJoin.py
python .\test.py -f query\select_last_crash.py
python .\test.py -f query\queryNullValueTest.py
python .\test.py -f query\queryInsertValue.py
python .\test.py -f query\queryConnection.py
python .\test.py -f query\queryCountCSVData.py
python .\test.py -f query\natualInterval.py
python .\test.py -f query\bug1471.py
#python .\test.py -f query\dataLossTest.py
python .\test.py -f query\bug1874.py
python .\test.py -f query\bug1875.py
python .\test.py -f query\bug1876.py
python .\test.py -f query\bug2218.py
python .\test.py -f query\bug2117.py
python .\test.py -f query\bug2118.py
python .\test.py -f query\bug2143.py
python .\test.py -f query\sliding.py
python .\test.py -f query\unionAllTest.py
python .\test.py -f query\bug2281.py
python .\test.py -f query\bug2119.py
python .\test.py -f query\isNullTest.py
python .\test.py -f query\queryWithTaosdKilled.py
python .\test.py -f query\floatCompare.py
python .\test.py -f query\query1970YearsAf.py
python .\test.py -f query\bug3351.py
python .\test.py -f query\bug3375.py
python .\test.py -f query\queryJoin10tables.py
python .\test.py -f query\queryStddevWithGroupby.py
python .\test.py -f query\querySecondtscolumnTowherenow.py
python .\test.py -f query\queryFilterTswithDateUnit.py
python .\test.py -f query\queryTscomputWithNow.py
python .\test.py -f query\queryStableJoin.py
python .\test.py -f query\computeErrorinWhere.py
python .\test.py -f query\queryTsisNull.py
python .\test.py -f query\subqueryFilter.py
python .\test.py -f query\nestedQuery\queryInterval.py
python .\test.py -f query\queryStateWindow.py
# python .\test.py -f query\nestedQuery\queryWithOrderLimit.py
python .\test.py -f query\nestquery_last_row.py
python .\test.py -f query\queryCnameDisplay.py
python .\test.py -f query\operator_cost.py
# python .\test.py -f query\long_where_query.py
python test.py -f query\nestedQuery\queryWithSpread.py

#stream
python .\test.py -f stream\metric_1.py
python .\test.py -f stream\metric_n.py
python .\test.py -f stream\new.py
python .\test.py -f stream\stream1.py
python .\test.py -f stream\stream2.py
#python .\test.py -f stream\parser.py
python .\test.py -f stream\history.py
python .\test.py -f stream\sys.py
python .\test.py -f stream\table_1.py
python .\test.py -f stream\table_n.py
python .\test.py -f stream\showStreamExecTimeisNull.py
python .\test.py -f stream\cqSupportBefore1970.py

#alter table
python .\test.py -f alter\alter_table_crash.py
python .\test.py -f alter\alterTabAddTagWithNULL.py
python .\test.py -f alter\alterTimestampColDataProcess.py

# client
python .\test.py -f client\client.py
python .\test.py -f client\version.py
python .\test.py -f client\alterDatabase.py
python .\test.py -f client\noConnectionErrorTest.py
# python test.py -f client\change_time_1_1.py
# python test.py -f client\change_time_1_2.py

# Misc
python testCompress.py
python testNoCompress.py
python testMinTablesPerVnode.py
python queryCount.py
python .\test.py -f query\queryGroupbyWithInterval.py
python client\twoClients.py
python test.py -f query\queryInterval.py
python test.py -f query\queryFillTest.py
# subscribe
python test.py -f subscribe\singlemeter.py
#python test.py -f subscribe\stability.py  
python test.py -f subscribe\supertable.py
# topic
python .\test.py -f topic\topicQuery.py
#======================p3-end===============
#======================p4-start===============

python .\test.py -f update\merge_commit_data-0.py
# wal
python .\test.py -f wal\addOldWalTest.py
python .\test.py -f wal\sdbComp.py 

# function
python .\test.py -f functions\all_null_value.py
# functions
python .\test.py -f functions\function_avg.py -r 1
python .\test.py -f functions\function_bottom.py -r 1
python .\test.py -f functions\function_count.py -r 1
python .\test.py -f functions\function_count_last_stab.py
python .\test.py -f functions\function_diff.py -r 1
python .\test.py -f functions\function_first.py -r 1
python .\test.py -f functions\function_last.py -r 1
python .\test.py -f functions\function_last_row.py -r 1
python .\test.py -f functions\function_leastsquares.py -r 1
python .\test.py -f functions\function_max.py -r 1
python .\test.py -f functions\function_min.py -r 1
python .\test.py -f functions\function_operations.py -r 1 
python .\test.py -f functions\function_percentile.py -r 1
python .\test.py -f functions\function_spread.py -r 1
python .\test.py -f functions\function_stddev.py -r 1
python .\test.py -f functions\function_sum.py -r 1
python .\test.py -f functions\function_top.py -r 1
python .\test.py -f functions\function_twa.py -r 1
python .\test.py -f functions\function_twa_test2.py
python .\test.py -f functions\function_stddev_td2555.py
python .\test.py -f functions\showOfflineThresholdIs864000.py
python .\test.py -f functions\function_interp.py
python .\test.py -f insert\metadataUpdate.py
python .\test.py -f query\last_cache.py
python .\test.py -f query\last_row_cache.py
python .\test.py -f account\account_create.py
python .\test.py -f alter\alter_table.py
python .\test.py -f query\queryGroupbySort.py
python .\test.py -f functions\queryTestCases.py
python .\test.py -f functions\function_stateWindow.py
python .\test.py -f functions\function_derivative.py
python .\test.py  -f functions\function_irate.py

python .\test.py -f insert\unsignedInt.py
python .\test.py -f insert\unsignedBigint.py
python .\test.py -f insert\unsignedSmallint.py
python .\test.py -f insert\unsignedTinyint.py
python .\test.py -f insert\insertFromCSV.py
python .\test.py -f query\filterAllUnsignedIntTypes.py

python .\test.py -f tag_lite\unsignedInt.py
python .\test.py -f tag_lite\unsignedBigint.py
python .\test.py -f tag_lite\unsignedSmallint.py
python .\test.py -f tag_lite\unsignedTinyint.py

python .\test.py -f functions\function_percentile2.py
python .\test.py -f insert\boundary2.py
python .\test.py -f insert\insert_locking.py
python .\test.py -f alter\alter_debugFlag.py
python .\test.py -f query\queryBetweenAnd.py
python .\test.py -f tag_lite\alter_tag.py
python test.py -f tools\taosdemoAllTest\TD-4985\query-limit-offset.py
python test.py -f tools\taosdemoAllTest\TD-5213\insert4096columns_not_use_taosdemo.py
python test.py -f tools\taosdemoAllTest\TD-5213\insertSigcolumnsNum4096.py
python .\test.py -f tag_lite\drop_auto_create.py
python test.py -f insert\insert_before_use_db.py
python test.py -f alter\alter_keep.py
python test.py -f alter\alter_cacheLastRow.py
python .\test.py -f query\querySession.py 
python test.py -f  alter\alter_create_exception.py
python .\test.py -f insert\flushwhiledrop.py
python .\test.py -f insert\schemalessInsert.py
python .\test.py -f alter\alterColMultiTimes.py
python .\test.py -f query\queryWildcardLength.py
python .\test.py -f query\queryTbnameUpperLower.py
python .\test.py -f query\query.py
python .\test.py -f query\queryDiffColsOr.py
#======================p4-end===============









