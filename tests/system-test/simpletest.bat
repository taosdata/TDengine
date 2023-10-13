
python3 .\test.py -f 0-others\taosShell.py
python3 .\test.py -f 0-others\taosShellError.py
python3 .\test.py -f 0-others\taosShellNetChk.py
python3 .\test.py -f 0-others\telemetry.py
python3 .\test.py -f 0-others\taosdMonitor.py
@REM python3 .\test.py -f 0-others\udfTest.py
@REM python3 .\test.py -f 0-others\udf_create.py
@REM python3 .\test.py -f 0-others\udf_restart_taosd.py
python3 .\test.py -f 0-others\cachemodel.py

@REM python3 .\test.py -f 0-others\user_control.py
@REM python3 .\test.py -f 0-others\fsync.py

python3 .\test.py -f 1-insert\influxdb_line_taosc_insert.py
@REM python3 .\test.py -f 1-insert\opentsdb_telnet_line_taosc_insert.py
@REM python3 .\test.py -f 1-insert\opentsdb_json_taosc_insert.py
@REM #python3 .\test.py -f 1-insert\test_stmt_muti_insert_query.py
@REM python3 .\test.py -f 1-insert\alter_stable.py
@REM python3 .\test.py -f 1-insert\alter_table.py
python3 .\test.py -f 2-query\between.py
@REM python3 .\test.py -f 2-query\distinct.py
@REM python3 .\test.py -f 2-query\varchar.py
@REM python3 .\test.py -f 2-query\ltrim.py
@REM python3 .\test.py -f 2-query\rtrim.py
@REM python3 .\test.py -f 2-query\length.py
@REM python3 .\test.py -f 2-query\char_length.py
@REM python3 .\test.py -f 2-query\upper.py
@REM python3 .\test.py -f 2-query\lower.py
@REM python3 .\test.py -f 2-query\join.py
@REM python3 .\test.py -f 2-query\join2.py
@REM python3 .\test.py -f 2-query\cast.py
@REM python3 .\test.py -f 2-query\union.py
@REM python3 .\test.py -f 2-query\union1.py
@REM python3 .\test.py -f 2-query\concat.py
@REM python3 .\test.py -f 2-query\concat2.py
@REM python3 .\test.py -f 2-query\concat_ws.py
@REM python3 .\test.py -f 2-query\concat_ws2.py
@REM python3 .\test.py -f 2-query\check_tsdb.py
@REM python3 .\test.py -f 2-query\spread.py
@REM python3 .\test.py -f 2-query\hyperloglog.py


@REM python3 .\test.py -f 2-query\timezone.py
@REM python3 .\test.py -f 2-query\Now.py
@REM python3 .\test.py -f 2-query\Today.py
@REM python3 .\test.py -f 2-query\max.py
@REM python3 .\test.py -f 2-query\min.py
@REM python3 .\test.py -f 2-query\count.py
@REM python3 .\test.py -f 2-query\last.py
@REM python3 .\test.py -f 2-query\first.py
@REM python3 .\test.py -f 2-query\To_iso8601.py
@REM python3 .\test.py -f 2-query\To_unixtimestamp.py
@REM python3 .\test.py -f 2-query\timetruncate.py
@REM python3 .\test.py -f 2-query\diff.py
@REM python3 .\test.py -f 2-query\Timediff.py

@REM python3 .\test.py -f 2-query\top.py
@REM python3 .\test.py -f 2-query\bottom.py
@REM python3 .\test.py -f 2-query\percentile.py
@REM python3 .\test.py -f 2-query\apercentile.py
@REM python3 .\test.py -f 2-query\abs.py
@REM python3 .\test.py -f 2-query\ceil.py
@REM python3 .\test.py -f 2-query\floor.py
@REM python3 .\test.py -f 2-query\round.py
@REM python3 .\test.py -f 2-query\log.py
@REM python3 .\test.py -f 2-query\pow.py
@REM python3 .\test.py -f 2-query\sqrt.py
@REM python3 .\test.py -f 2-query\sin.py
@REM python3 .\test.py -f 2-query\cos.py
@REM python3 .\test.py -f 2-query\tan.py
@REM python3 .\test.py -f 2-query\arcsin.py
@REM python3 .\test.py -f 2-query\arccos.py
@REM python3 .\test.py -f 2-query\arctan.py
python3 .\test.py -f 2-query\query_cols_tags_and_or.py
@REM # python3 .\test.py -f 2-query\nestedQuery.py
@REM # TD-15983 subquery output duplicate name column. 
@REM # Please Xiangyang Guo modify the following script
@REM # python3 .\test.py -f 2-query\nestedQuery_str.py

@REM python3 .\test.py -f 2-query\avg.py
@REM python3 .\test.py -f 2-query\elapsed.py
@REM python3 .\test.py -f 2-query\csum.py
@REM python3 .\test.py -f 2-query\mavg.py
@REM python3 .\test.py -f 2-query\diff.py
@REM python3 .\test.py -f 2-query\sample.py
@REM python3 .\test.py -f 2-query\function_diff.py
@REM python3 .\test.py -f 2-query\unique.py
@REM python3 .\test.py -f 2-query\stateduration.py
@REM python3 .\test.py -f 2-query\function_stateduration.py
@REM python3 .\test.py -f 2-query\statecount.py

@REM python3 .\test.py -f 7-tmq\basic5.py
@REM python3 .\test.py -f 7-tmq\subscribeDb.py
@REM python3 .\test.py -f 7-tmq\subscribeDb0.py
@REM python3 .\test.py -f 7-tmq\subscribeDb1.py
python3 .\test.py -f 7-tmq\subscribeStb.py
@REM python3 .\test.py -f 7-tmq\subscribeStb0.py
@REM python3 .\test.py -f 7-tmq\subscribeStb1.py
@REM python3 .\test.py -f 7-tmq\subscribeStb2.py
@REM python3 .\test.py -f 7-tmq\subscribeStb3.py
@REM python3 .\test.py -f 7-tmq\subscribeStb4.py
@REM python3 .\test.py -f 7-tmq\db.py
python3 .\test.py -f 6-cluster\5dnode3mnodeSep1VnodeStopDnodeModifyMeta.py  -N 6 -M 3