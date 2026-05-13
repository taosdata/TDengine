# 支持 blob 数据类型-Test Spec

## 1. 测试目标

测试 blob 数据类型在 taosx 数据同步任务中是否能够正确同步。

## 2. 参考文档

https://jira.taosdata.com:18080/browse/TS-5820

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-09-19 | 0.1 | 张贵川 | 文档撰写 |
|  |  |  |  |

## 4. 测试结论

已完成测试：
1. 有 blob 数据类型的数据源 mysql，oracle 数据源数据同步
2. 无 blob 数据类型的数据源：mssql, mangodb,kafka, mqtt 等，通过 mapping 字段配置进行映射
3. tmq2td 的数据同步测试
4. td2td 的带 blob 的数据同步测试
5. tmq2local 的数据导出测试 和  local2tmq 的数据导入测试：通过页面的 备份和恢复 功能测试
6. td2parquet 的数据导出测试
7. td2csv 的数据导出测试
8. 数据浏览页面是否正常显示 blob 数据 和 是否可以正常创建 blob 字段的表

## 5. 测试环境

- OS: Linux, ubuntu 24.04.1
- Browser: Chrome

## 6. 功能测试

**约束**：
1. 截止时间 20250925 版本， 目前 blob 写入功能最大不能超过 4M, 带 blob 的 sql 语句依然只能在 1M 以内。 当前测试基于此测试。

**测试：**
1. 有 blob 数据类型的数据源 mysql，oracle 数据源数据同步

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | Mysql 小数据blob是否能够同步 | 页面测试： ![](./images/img_X0BObqJlRou3UFxJkA8cZf0Pndb.png) ![](./images/img_Gknobp7vPoA2FXxKgArc5PsqnVd.png) | 1. 数据导入正常 |
| 2 | 通过 | Mysql 3M blob数据是否能够同步 | 7M数据超出 http payload 限制，这里使用命令行： ```bash {wrap} taosx run -v -f "mysql://root:taosdata@mysql:3306/zgc?subtable_fields=select distinct groupid, location from t2;&sql=SELECT * FROM t2 WHERE time >= \${start_no_tz} AND time < \${end_no_tz} and \${groupid} and \${location} ORDER BY time;&start=2025-09-22T00:00:00+08:00&end=2025-09-22T17:25:00+08:00&interval=1h&delay=0s&read_concurrency=0&batch_size=10000&health_check_window_in_second=0s&busy_threshold=100%&max_queue_length=1000&max_errors_in_window=10" -t "taos://root:taosdata@localhost:6030/db1" -p "@./parser.json" ``` | 1. 3M blob数据可以正常同步 |
| 4 | 通过 | Oracle 小数据量blob数据是否能够同步 | 原数据： ![](./images/img_N3c6bRe8UocE4txKvhQc6hqvnnd.png) 同步后： ![](./images/img_I6L1bx0iXozo99xsI11cwyxPnPc.png) 执行命令： ```bash {wrap} taosx run -vv -f "oracle://system:taosdata@172.18.0.3:1521/xe?batch_size=10000&busy_threshold=100%&delay=0s&end=2025-08-06T00:00:00+08:00&health_check_window_in_second=0s&interval=1d&max_errors_in_window=10&max_queue_length=1000&read_concurrency=0&sql=SELECT * FROM t1 WHERE time >= \${start_no_tz} AND time < \${end_no_tz} ORDER BY time&start=2025-08-04T00:00:00+08:00&subtable_fields=select distinct groupid, location from t1" -t "taos://root:taosdata@td1:6030/testdb" -p "@./oracle-parser.json ``` | 1. blob 数据正常同步 |

1. 无 blob 数据类型的数据源：mssql,kafka, mqtt 等，通过 mapping 字段配置进行映射
这里只用测试 kafka,mssql 即可,其他逻辑均一致:

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | mssql 的 varbinary 同步为 blob | mssql 数据的 varbinary 数据： ![](./images/img_MAlFbheGjoXOxmxxCDLcA2f4nKd.png) 同步到td的 blob 数据： ![](./images/img_HIFIbMJRLozGXBx4phDcZ7ASnNb.png) 命令： ```plaintext {wrap} taosx run -f "mssql://sa:Taos%40123%21@172.18.0.3:1433/mydb?batch_size=10000&busy_threshold=100%&dea7d812-3c76-40a5-bb8a-1048945f79cb=plain&delay=0s&encryption=Off&end=2025-08-19T00:00:00+08:00&health_check_window_in_second=0s&interval=1d&max_errors_in_window=10&max_queue_length=1000&read_concurrency=0&sql=SELECT * FROM t1 WHERE time >= \${start_no_tz} AND time < \${end_no_tz} and \${groupid} and \${location} ORDER BY time&start=2025-07-17T00:00:00+08:00&subtable_fields=select distinct groupid, location from t1&trust_cert=true" -t "taos://root:taosdata@td1:6030/testdb" -p "@/docker/dev/run_cmd/jira/ts5820/mssql-parser.json" ``` | 1. Blob 数据正常同步 |
| 2 | 通过 | 1. Kafka 数组数据同步为 blob 测试 1. 20k 的数组数据同步为 blob 测试 | 创建任务： ![](./images/img_Kcf3buHdNoYe1xxVxVvc5zehnZ0.png) 执行结果： ![](./images/img_E9vHbzkpkomonNx3tHVcbpPZnpb.png) | 1. 数组数据正常同步为blob 1. 20k 大的数据也正常同步 |
| 3 | 通过 | Kafka 字符串数据同步为 blob 测试 | 创建任务： ![](./images/img_QjwEbuPuPoL4fBxpw7qcR1FUnmd.png) 执行结果： ![](./images/img_FMRnb36H4oqWQKxStKwcOn5xnGc.png) | 1. 字符串数据正常同步为blob |


1. tmq2td 的数据同步测试

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | 500k blob数据是否能够同步 | 创建任务： ![](./images/img_TCiUbu1ERo9zCoxKMqZcLGgYn5m.png) 目标库： ![](./images/img_LLRPbnAR1opL7XxsGk1cY6kPnPf.png) ![](./images/img_EHb3blmlBol37Gx6xQ2cXn2onEf.png) | 1. 正常同步 |


1. td2td 的带 blob 的数据同步测试

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | 小数据blob是否能够同步 | 页面测试： ![](./images/img_JfPAbHoPtooF7IxltIncLOHOn3p.png) 正取同步: ![](./images/img_A64wbpsMUoicigxDlNqcT63dnVf.png) | 1. 数据导入正常 |
| 2 | 通过 | 3M blob数据是否能够同步 | 使用命令行： ```plaintext {wrap} /usr/local/taos/bin/taosx run -f "taos+ws://127.0.0.1:6041/db1?compression=false&end=2025-09-22T18:00:00+08:00&mode=history&schema=always&schema-polling-interval=5s&sparse=false&stables=mysql_st&start=2025-09-22T00:00:00+08:00&workers=0&write-concurrency=1" -t "taos://root:taosdata@172.18.0.2:6030/ts5820" -p "@./td2td-parser.json" ``` 成功同步： ![](./images/img_Dzi8bEtiWohVMvxBstlc5rw4nGb.png) | 1. 数据导入正常 |


1. 数据备份测试 tmq2local

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | 500k blob数据是否成功备份 | 创建备份计划： ![](./images/img_FbwYbQe8nom1pdxGhfEc9yRXnmf.png) 成功备份： ![](./images/img_FhyQbDiStoYxzPxPptwcFys2nng.png) | 1. 成功备份 |


1. 页面数据恢复测试 local2tmq 的数据同步测试

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 成功 | 使用备份文件恢复 | 备份恢复： ![](./images/img_BGgAbjA9JoHBnwxSltJcOtDVnPe.png) ![](./images/img_QVJ4bk7FzoSUTLxvlIRctg75nvb.png) 恢复前： ![](./images/img_HvCyb56gkoBwLGxOQrncE8fwnae.png) 恢复后： ![](./images/img_OT2qbuisRoJLZcx73yScUZGnnqb.png) | 1. 表创建 1. 数据正常同步 |


1. td2parquet 的数据导出测试

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | blob数据格式可以正常导出为 parquet 文件 | 执行sql: ```bash {wrap} /usr/local/taos/bin/taosx run -f "taos+ws://127.0.0.1:6041/db1?query=select tbname, * from meters" -t "parquet:./meters.parquet" ``` 结果： ![](./images/img_U0UpbIo57ojoiWxg3ascBM8FnqH.png) | 1. 数据正常导出 |


1. td2csv 的数据导出测试

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | blob数据格式可以正常导出为csv文件 | 执行sql: ```bash {wrap} /usr/local/taos/bin/taosx run -f "taos+ws://127.0.0.1:6041/db1?query=select tbname, * from meters" -t "csv:./meters.csv" ``` 结果： ![](./images/img_VlBCbkBu8ogcLtxdgG5c8WP1nyd.png) | 1. 数据正常导出 |


1. 数据浏览页面是否正常显示 blob 数据 和 是否可以正常创建 blob 字段的表：

| # | 测试结果 | 测试用例 | 测试描述 | 预期行为 |
| --- | --- | --- | --- | --- |
| 1 | 通过 | 数据浏览页面显示blob数据 | 正常显示： ![](./images/img_Nlr3bia0RojDCKxP0wTcHxrBnsh.png) | 1. 正常显示 |
| 2 | 通过 | 通过页面创建带 blob 的表 | 创建表： ![](./images/img_JWPkbzZtSoiw8wxbyyucDeuenTg.png) ![](./images/img_TIQbbBQ6houVmmxpsKdcefhCnjc.png) | 1. 正常创建 |



## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无

## 9. 性能测试

无

## 10. 安全测试

无

## 11. 兼容性测试

测试用例包括但不局限于：
- 兼容旧版本

## 12. 已知问题和限制（可选）

无
