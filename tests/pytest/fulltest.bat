#!\bin\bash
ulimit -c unlimited
#======================p1-start===============

python .\test.py -f insert\basic.py -w 1 -m u05
python .\test.py -f insert\int.py -w 1 -m u05
python .\test.py -f insert\float.py -w 1 -m u05
python .\test.py -f insert\bigint.py -w 1 -m u05
python .\test.py -f insert\bool.py -w 1 -m u05
python .\test.py -f insert\double.py -w 1 -m u05
python .\test.py -f insert\smallint.py -w 1 -m u05
python .\test.py -f insert\tinyint.py -w 1 -m u05
python .\test.py -f insert\date.py -w 1 -m u05
python .\test.py -f insert\binary.py -w 1 -m u05
python .\test.py -f insert\nchar.py -w 1 -m u05
#python .\test.py -f insert\nchar-boundary.py -w 1 -m u05
python .\test.py -f insert\nchar-unicode.py -w 1 -m u05
python .\test.py -f insert\multi.py -w 1 -m u05
python .\test.py -f insert\randomNullCommit.py -w 1 -m u05
python insert\retentionpolicy.py -w 1 -m u05
python .\test.py -f insert\alterTableAndInsert.py -w 1 -m u05
python .\test.py -f insert\insertIntoTwoTables.py -w 1 -m u05
python .\test.py -f insert\before_1970.py -w 1 -m u05
python .\test.py -f insert\special_character_show.py -w 1 -m u05
python bug2265.py -w 1 -m u05
python .\test.py -f insert\bug3654.py -w 1 -m u05
python .\test.py -f insert\insertDynamicColBeforeVal.py -w 1 -m u05
python .\test.py -f insert\in_function.py -w 1 -m u05
python .\test.py -f insert\modify_column.py -w 1 -m u05
python .\test.py -f insert\line_insert.py -w 1 -m u05

# timezone 

python .\test.py -f TimeZone\TestCaseTimeZone.py -w 1 -m u05

#table
python .\test.py -f table\alter_wal0.py -w 1 -m u05
python .\test.py -f table\column_name.py -w 1 -m u05
python .\test.py -f table\column_num.py -w 1 -m u05
python .\test.py -f table\db_table.py -w 1 -m u05
python .\test.py -f table\create_sensitive.py -w 1 -m u05
python .\test.py -f table\tablename-boundary.py -w 1 -m u05
python .\test.py  -f table\max_table_length.py -w 1 -m u05
python .\test.py -f table\alter_column.py -w 1 -m u05
python .\test.py -f table\boundary.py -w 1 -m u05
python .\test.py -f table\create.py -w 1 -m u05
python .\test.py -f table\del_stable.py -w 1 -m u05

#stable
python .\test.py -f stable\insert.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\taosdemoTestInsertWithJsonStmt.py 

# tag
python .\test.py -f tag_lite\filter.py -w 1 -m u05
python .\test.py -f tag_lite\create-tags-boundary.py -w 1 -m u05
python .\test.py -f tag_lite\3.py -w 1 -m u05
python .\test.py -f tag_lite\4.py -w 1 -m u05
python .\test.py -f tag_lite\5.py -w 1 -m u05
python .\test.py -f tag_lite\6.py -w 1 -m u05
python .\test.py -f tag_lite\add.py -w 1 -m u05
python .\test.py -f tag_lite\bigint.py -w 1 -m u05
python .\test.py -f tag_lite\binary_binary.py -w 1 -m u05
python .\test.py -f tag_lite\binary.py -w 1 -m u05
python .\test.py -f tag_lite\bool_binary.py -w 1 -m u05
python .\test.py -f tag_lite\bool_int.py -w 1 -m u05
python .\test.py -f tag_lite\bool.py -w 1 -m u05
python .\test.py -f tag_lite\change.py -w 1 -m u05
python .\test.py -f tag_lite\column.py -w 1 -m u05
python .\test.py -f tag_lite\commit.py -w 1 -m u05
python .\test.py -f tag_lite\create.py -w 1 -m u05
python .\test.py -f tag_lite\datatype.py -w 1 -m u05
python .\test.py -f tag_lite\datatype-without-alter.py -w 1 -m u05
python .\test.py -f tag_lite\delete.py -w 1 -m u05
python .\test.py -f tag_lite\double.py -w 1 -m u05
python .\test.py -f tag_lite\float.py -w 1 -m u05
python .\test.py -f tag_lite\int_binary.py -w 1 -m u05
python .\test.py -f tag_lite\int_float.py -w 1 -m u05
python .\test.py -f tag_lite\int.py -w 1 -m u05
python .\test.py -f tag_lite\set.py -w 1 -m u05
python .\test.py -f tag_lite\smallint.py -w 1 -m u05
python .\test.py -f tag_lite\tinyint.py -w 1 -m u05
python .\test.py -f tag_lite\timestamp.py -w 1 -m u05
python .\test.py -f tag_lite\TestModifyTag.py -w 1 -m u05

#python .\test.py -f dbmgmt\database-name-boundary.py -w 1 -m u05
python test.py -f dbmgmt\nanoSecondCheck.py -w 1 -m u05

python .\test.py -f import_merge\importBlock1HO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1HPO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1H.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1S.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1Sub.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1TO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1TPO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock1T.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2HO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2HPO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2H.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2S.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2Sub.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2TO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2TPO.py -w 1 -m u05
python .\test.py -f import_merge\importBlock2T.py -w 1 -m u05
python .\test.py -f import_merge\importBlockbetween.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileHO.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileHPO.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileH.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileS.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileSub.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileTO.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileTPO.py -w 1 -m u05
python .\test.py -f import_merge\importCacheFileT.py -w 1 -m u05
python .\test.py -f import_merge\importDataH2.py -w 1 -m u05
python .\test.py -f import_merge\importDataHO2.py -w 1 -m u05
python .\test.py -f import_merge\importDataHO.py -w 1 -m u05
python .\test.py -f import_merge\importDataHPO.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastHO.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastHPO.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastH.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastS.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastSub.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastTO.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastTPO.py -w 1 -m u05
python .\test.py -f import_merge\importDataLastT.py -w 1 -m u05
python .\test.py -f import_merge\importDataS.py -w 1 -m u05
python .\test.py -f import_merge\importDataSub.py -w 1 -m u05
python .\test.py -f import_merge\importDataTO.py -w 1 -m u05
python .\test.py -f import_merge\importDataTPO.py -w 1 -m u05
python .\test.py -f import_merge\importDataT.py -w 1 -m u05
python .\test.py -f import_merge\importHeadOverlap.py -w 1 -m u05
python .\test.py -f import_merge\importHeadPartOverlap.py -w 1 -m u05
python .\test.py -f import_merge\importHead.py -w 1 -m u05
python .\test.py -f import_merge\importHORestart.py -w 1 -m u05
python .\test.py -f import_merge\importHPORestart.py -w 1 -m u05
python .\test.py -f import_merge\importHRestart.py -w 1 -m u05
python .\test.py -f import_merge\importLastHO.py -w 1 -m u05
python .\test.py -f import_merge\importLastHPO.py -w 1 -m u05
python .\test.py -f import_merge\importLastH.py -w 1 -m u05
python .\test.py -f import_merge\importLastS.py -w 1 -m u05
python .\test.py -f import_merge\importLastSub.py -w 1 -m u05
python .\test.py -f import_merge\importLastTO.py -w 1 -m u05
python .\test.py -f import_merge\importLastTPO.py -w 1 -m u05
python .\test.py -f import_merge\importLastT.py -w 1 -m u05
python .\test.py -f import_merge\importSpan.py -w 1 -m u05
python .\test.py -f import_merge\importSRestart.py -w 1 -m u05
python .\test.py -f import_merge\importSubRestart.py -w 1 -m u05
python .\test.py -f import_merge\importTailOverlap.py -w 1 -m u05
python .\test.py -f import_merge\importTailPartOverlap.py -w 1 -m u05
python .\test.py -f import_merge\importTail.py -w 1 -m u05
python .\test.py -f import_merge\importToCommit.py -w 1 -m u05
python .\test.py -f import_merge\importTORestart.py -w 1 -m u05
python .\test.py -f import_merge\importTPORestart.py -w 1 -m u05
python .\test.py -f import_merge\importTRestart.py -w 1 -m u05
python .\test.py -f import_merge\importInsertThenImport.py -w 1 -m u05
python .\test.py -f import_merge\importCSV.py -w 1 -m u05
python .\test.py -f import_merge\import_update_0.py -w 1 -m u05
python .\test.py -f import_merge\import_update_1.py -w 1 -m u05
python .\test.py -f import_merge\import_update_2.py -w 1 -m u05
python .\test.py -f update\merge_commit_data.py -w 1 -m u05
#======================p1-end===============
#======================p2-start===============
# tools
python test.py -f tools\taosdumpTest.py -w 1 -m u05
python test.py -f tools\taosdumpTest2.py -w 1 -m u05

python test.py -f tools\taosdemoTest.py -w 1 -m u05
python test.py -f tools\taosdemoTestWithoutMetric.py -w 1 -m u05
python test.py -f tools\taosdemoTestWithJson.py -w 1 -m u05
python test.py -f tools\taosdemoTestLimitOffset.py -w 1 -m u05
python test.py -f tools\taosdemoTestTblAlt.py -w 1 -m u05
python test.py -f tools\taosdemoTestSampleData.py -w 1 -m u05
python test.py -f tools\taosdemoTestInterlace.py -w 1 -m u05
python test.py -f tools\taosdemoTestQuery.py -w 1 -m u05

# restful test for python
python test.py -f restful\restful_bind_db1.py -w 1 -m u05
python test.py -f restful\restful_bind_db2.py -w 1 -m u05

# nano support
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestSupportNanoInsert.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestSupportNanoQuery.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestSupportNanosubscribe.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdemoTestInsertTime_step.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\NanoTestCase\taosdumpTestNanoSupport.py -w 1 -m u05

# update
python .\test.py -f update\allow_update.py -w 1 -m u05
python .\test.py -f update\allow_update-0.py -w 1 -m u05
python .\test.py -f update\append_commit_data.py -w 1 -m u05
python .\test.py -f update\append_commit_last-0.py -w 1 -m u05
python .\test.py -f update\append_commit_last.py -w 1 -m u05


python .\test.py -f update\merge_commit_data2.py -w 1 -m u05
python .\test.py -f update\merge_commit_data2_update0.py -w 1 -m u05
python .\test.py -f update\merge_commit_last-0.py -w 1 -m u05
python .\test.py -f update\merge_commit_last.py -w 1 -m u05
python .\test.py -f update\bug_td2279.py -w 1 -m u05

#======================p2-end===============
#======================p3-start===============


# user
python .\test.py -f user\user_create.py -w 1 -m u05
python .\test.py -f user\pass_len.py -w 1 -m u05

# stable
python .\test.py -f stable\query_after_reset.py -w 1 -m u05

# perfbenchmark
python .\test.py -f perfbenchmark\bug3433.py -w 1 -m u05
#python .\test.py -f perfbenchmark\bug3589.py -w 1 -m u05
python .\test.py -f perfbenchmark\taosdemoInsert.py -w 1 -m u05

#taosdemo
python test.py -f tools\taosdemoAllTest\taosdemoTestInsertWithJson.py 
python test.py -f tools\taosdemoAllTest\taosdemoTestQueryWithJson.py -w 1 -m u05

#query
python test.py -f query\distinctOneColTb.py 
python .\test.py -f query\filter.py -w 1 -m u05
python .\test.py -f query\filterCombo.py -w 1 -m u05
python .\test.py -f query\queryNormal.py -w 1 -m u05
python .\test.py -f query\queryError.py -w 1 -m u05
python .\test.py -f query\filterAllIntTypes.py -w 1 -m u05
python .\test.py -f query\filterFloatAndDouble.py -w 1 -m u05
python .\test.py -f query\filterOtherTypes.py -w 1 -m u05
python .\test.py -f query\querySort.py -w 1 -m u05
python .\test.py -f query\queryJoin.py -w 1 -m u05
python .\test.py -f query\select_last_crash.py -w 1 -m u05
python .\test.py -f query\queryNullValueTest.py -w 1 -m u05
python .\test.py -f query\queryInsertValue.py -w 1 -m u05
python .\test.py -f query\queryConnection.py -w 1 -m u05
python .\test.py -f query\queryCountCSVData.py -w 1 -m u05
python .\test.py -f query\natualInterval.py -w 1 -m u05
python .\test.py -f query\bug1471.py -w 1 -m u05
#python .\test.py -f query\dataLossTest.py -w 1 -m u05
python .\test.py -f query\bug1874.py -w 1 -m u05
python .\test.py -f query\bug1875.py -w 1 -m u05
python .\test.py -f query\bug1876.py -w 1 -m u05
python .\test.py -f query\bug2218.py -w 1 -m u05
python .\test.py -f query\bug2117.py -w 1 -m u05
python .\test.py -f query\bug2118.py -w 1 -m u05
python .\test.py -f query\bug2143.py -w 1 -m u05
python .\test.py -f query\sliding.py -w 1 -m u05
python .\test.py -f query\unionAllTest.py -w 1 -m u05
python .\test.py -f query\bug2281.py -w 1 -m u05
python .\test.py -f query\bug2119.py -w 1 -m u05
python .\test.py -f query\isNullTest.py -w 1 -m u05
python .\test.py -f query\queryWithTaosdKilled.py -w 1 -m u05
python .\test.py -f query\floatCompare.py -w 1 -m u05
python .\test.py -f query\query1970YearsAf.py -w 1 -m u05
python .\test.py -f query\bug3351.py -w 1 -m u05
python .\test.py -f query\bug3375.py -w 1 -m u05
python .\test.py -f query\queryJoin10tables.py -w 1 -m u05
python .\test.py -f query\queryStddevWithGroupby.py -w 1 -m u05
python .\test.py -f query\querySecondtscolumnTowherenow.py -w 1 -m u05
python .\test.py -f query\queryFilterTswithDateUnit.py -w 1 -m u05
python .\test.py -f query\queryTscomputWithNow.py -w 1 -m u05
python .\test.py -f query\queryStableJoin.py -w 1 -m u05
python .\test.py -f query\computeErrorinWhere.py -w 1 -m u05
python .\test.py -f query\queryTsisNull.py -w 1 -m u05
python .\test.py -f query\subqueryFilter.py -w 1 -m u05
python .\test.py -f query\nestedQuery\queryInterval.py -w 1 -m u05
python .\test.py -f query\queryStateWindow.py -w 1 -m u05
# python .\test.py -f query\nestedQuery\queryWithOrderLimit.py -w 1 -m u05
python .\test.py -f query\nestquery_last_row.py -w 1 -m u05
python .\test.py -f query\queryCnameDisplay.py -w 1 -m u05
python .\test.py -f query\operator_cost.py -w 1 -m u05
# python .\test.py -f query\long_where_query.py -w 1 -m u05
python test.py -f query\nestedQuery\queryWithSpread.py -w 1 -m u05

#stream
python .\test.py -f stream\metric_1.py -w 1 -m u05
python .\test.py -f stream\metric_n.py -w 1 -m u05
python .\test.py -f stream\new.py -w 1 -m u05
python .\test.py -f stream\stream1.py -w 1 -m u05
python .\test.py -f stream\stream2.py -w 1 -m u05
#python .\test.py -f stream\parser.py -w 1 -m u05
python .\test.py -f stream\history.py -w 1 -m u05
python .\test.py -f stream\sys.py -w 1 -m u05
python .\test.py -f stream\table_1.py -w 1 -m u05
python .\test.py -f stream\table_n.py -w 1 -m u05
python .\test.py -f stream\showStreamExecTimeisNull.py -w 1 -m u05
python .\test.py -f stream\cqSupportBefore1970.py -w 1 -m u05

#alter table
python .\test.py -f alter\alter_table_crash.py -w 1 -m u05
python .\test.py -f alter\alterTabAddTagWithNULL.py -w 1 -m u05
python .\test.py -f alter\alterTimestampColDataProcess.py -w 1 -m u05

# client
python .\test.py -f client\client.py -w 1 -m u05
python .\test.py -f client\version.py -w 1 -m u05
python .\test.py -f client\alterDatabase.py -w 1 -m u05
python .\test.py -f client\noConnectionErrorTest.py -w 1 -m u05
# python test.py -f client\change_time_1_1.py -w 1 -m u05
# python test.py -f client\change_time_1_2.py -w 1 -m u05

# Misc
python testCompress.py -w 1 -m u05
python testNoCompress.py -w 1 -m u05
python testMinTablesPerVnode.py -w 1 -m u05
python queryCount.py -w 1 -m u05
python .\test.py -f query\queryGroupbyWithInterval.py -w 1 -m u05
python client\twoClients.py -w 1 -m u05
python test.py -f query\queryInterval.py -w 1 -m u05
python test.py -f query\queryFillTest.py -w 1 -m u05
# subscribe
python test.py -f subscribe\singlemeter.py -w 1 -m u05
#python test.py -f subscribe\stability.py  
python test.py -f subscribe\supertable.py -w 1 -m u05
# topic
python .\test.py -f topic\topicQuery.py -w 1 -m u05
#======================p3-end===============
#======================p4-start===============

python .\test.py -f update\merge_commit_data-0.py -w 1 -m u05
# wal
python .\test.py -f wal\addOldWalTest.py -w 1 -m u05
python .\test.py -f wal\sdbComp.py 

# function
python .\test.py -f functions\all_null_value.py -w 1 -m u05
# functions
python .\test.py -f functions\function_avg.py -r 1
python .\test.py -f functions\function_bottom.py -r 1
python .\test.py -f functions\function_count.py -r 1
python .\test.py -f functions\function_count_last_stab.py -w 1 -m u05
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
python .\test.py -f functions\function_twa_test2.py -w 1 -m u05
python .\test.py -f functions\function_stddev_td2555.py -w 1 -m u05
python .\test.py -f functions\showOfflineThresholdIs864000.py -w 1 -m u05
python .\test.py -f functions\function_interp.py -w 1 -m u05
python .\test.py -f insert\metadataUpdate.py -w 1 -m u05
python .\test.py -f query\last_cache.py -w 1 -m u05
python .\test.py -f query\last_row_cache.py -w 1 -m u05
python .\test.py -f account\account_create.py -w 1 -m u05
python .\test.py -f alter\alter_table.py -w 1 -m u05
python .\test.py -f query\queryGroupbySort.py -w 1 -m u05
python .\test.py -f functions\queryTestCases.py -w 1 -m u05
python .\test.py -f functions\function_stateWindow.py -w 1 -m u05
python .\test.py -f functions\function_derivative.py -w 1 -m u05
python .\test.py  -f functions\function_irate.py -w 1 -m u05

python .\test.py -f insert\unsignedInt.py -w 1 -m u05
python .\test.py -f insert\unsignedBigint.py -w 1 -m u05
python .\test.py -f insert\unsignedSmallint.py -w 1 -m u05
python .\test.py -f insert\unsignedTinyint.py -w 1 -m u05
python .\test.py -f insert\insertFromCSV.py -w 1 -m u05
python .\test.py -f query\filterAllUnsignedIntTypes.py -w 1 -m u05

python .\test.py -f tag_lite\unsignedInt.py -w 1 -m u05
python .\test.py -f tag_lite\unsignedBigint.py -w 1 -m u05
python .\test.py -f tag_lite\unsignedSmallint.py -w 1 -m u05
python .\test.py -f tag_lite\unsignedTinyint.py -w 1 -m u05

python .\test.py -f functions\function_percentile2.py -w 1 -m u05
python .\test.py -f insert\boundary2.py -w 1 -m u05
python .\test.py -f insert\insert_locking.py -w 1 -m u05
python .\test.py -f alter\alter_debugFlag.py -w 1 -m u05
python .\test.py -f query\queryBetweenAnd.py -w 1 -m u05
python .\test.py -f tag_lite\alter_tag.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\TD-4985\query-limit-offset.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\TD-5213\insert4096columns_not_use_taosdemo.py -w 1 -m u05
python test.py -f tools\taosdemoAllTest\TD-5213\insertSigcolumnsNum4096.py -w 1 -m u05
python .\test.py -f tag_lite\drop_auto_create.py -w 1 -m u05
python test.py -f insert\insert_before_use_db.py -w 1 -m u05
python test.py -f alter\alter_keep.py -w 1 -m u05
python test.py -f alter\alter_cacheLastRow.py -w 1 -m u05
python .\test.py -f query\querySession.py 
python test.py -f  alter\alter_create_exception.py -w 1 -m u05
python .\test.py -f insert\flushwhiledrop.py -w 1 -m u05
python .\test.py -f insert\schemalessInsert.py -w 1 -m u05
python .\test.py -f alter\alterColMultiTimes.py -w 1 -m u05
python .\test.py -f query\queryWildcardLength.py -w 1 -m u05
python .\test.py -f query\queryTbnameUpperLower.py -w 1 -m u05
python .\test.py -f query\query.py -w 1 -m u05
python .\test.py -f query\queryDiffColsOr.py -w 1 -m u05
#======================p4-end===============









