# TSDB 3.4.1.0 中英文 Release Notes

## 中文

### 特性

1. 6702838952 特性：TDengine TSDB MCP 服务
2. 6550634959 特性：支持外部窗口查询，时间范围由子查询显式指定
3. 6542129231 特性：支持非相关标量子查询
4. 6659773695 特性：支持 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符
5. 6491345559 特性：支持批量修改子表标签值
6. 6490755304 特性：流计算新增按自然周、月、季、年维度的定时计算能力
7. 6572489317 特性：流计算新增源子表 / 虚拟子表无新数据写入超时通知功能
8. 6593807450 特性：数据订阅新增对虚拟表元数据变更的支持
9. 6659792966 特性：数据订阅支持 Token 登录
10. 6548007902 特性：添加身份鉴别函数
11. 6567926427 特性：新增 BLOB 类型对 CAST、SUBSTR 函数的支持
12. 6487609391 特性：支持 LAG、LEAD、FILL_FORWARD 函数
13. 6506145499 特性：INTERVAL、INTERP 的 FILL 子句支持插值时间范围
14. 6641525627 特性：支持不带 FROM 子句的标量子查询
15. 6510267752 特性：支持 IN 运算符中使用非相关子查询
16. 6469793274 特性：taosd 新增数据修复模式支持
17. 6793469667 特性：TDgpt 新增对多变量异常检测功能的支持
18. 6622713900 特性：taosX 增加可见性权限控制
19. 6658956251 特性：taosX 支持系统和对象权限管理
20. 6653327869 特性：taosX 新增力控 pSpace 数据源
21. 6751321432 特性：OPC-UA 联通下检测支持故障转移
22. 6718901244 特性：Kafka 数据采集支持通过过滤条件存储到多个超级表
23. 6723594269 特性：KingHistorian 数据源同步中点位自动更新
24. 6600045300 特性：KingHistorian 支持结束时间为空时持续迁移
25. 6735513765 特性：Go WebSocket 订阅支持 Token
26. 6641467177 特性：Go REST 连接器支持 TOKEN 认证
27. 6735261785 特性：Rust 连接器订阅支持 Token 认证
28. 6735116367 特性：C WebSocket 订阅支持 Token 认证
29. 6622691504 特性：taosdump 支持 DECIMAL 类型导入导出
30. 6835134117 特性：taosdump 支持 BLOB 导入导出
31. 6923365814 特性：taosgen 支持 Windows
32. 6751373446 特性：taosgen 支持配置文件设置日志目录
33. 6622579928 特性：taosKeeper 支持导出当前所有指标
34. 6835426458 特性：Explorer 支持密码登录获取 Token
35. 6506023136 特性：Explorer 支持 TOTP 认证
36. 6658975929 特性：Explorer 支持 Bearer TOKEN 认证
37. 6625571859 特性：支持 MQTT 配置多个 Broker

### 优化

1. 6646294817 优化：Windows 平台新增对 taosX 的支持
2. 6861933885 优化：Windows 平台新增对 TDgpt 的支持
3. 6857094454 优化：Windows 平台新增对流计算功能的支持
4. 6922189593 优化：优化虚拟超级表聚合查询结合 GROUP BY 的查询性能
5. 6862634689 优化：优化虚拟超级表时间、事件、会话窗口的查询性能
6. 6755544717 优化：优化虚拟表引用物理表列数较多时的查询性能
7. 6694539984 优化：优化虚拟超级表状态窗口的查询性能
8. 6548485194 优化：下推非相关子查询的时间过滤条件，提升虚拟表按批次查询性能
9. 6622781381 优化：虚拟表创建失败时返回明确错误信息，如具体列的类型匹配失败
10. 6589101088 优化：新增虚拟表与源表引用校验能力，包含引用关系存储、源表变更校验、引用关系查询及虚拟表可用性验证功能
11. 6643522153 优化：优化 Last_row 查询的性能
12. 6568211421 优化：支持动态调整 LRU 分片数量，提升 Last 查询的执行性能
13. 6930434043 优化：RESTORE DNODE 命令支持指定 VGROUP ID
14. 6554623504 优化：提升交错写入场景下的订阅速度
15. 6692120342 优化：流计算子查询场景下新增对 IN 运算符的支持
16. 6751417338 优化：流计算新增对 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符的支持
17. 6598056767 优化：流计算场景下新增对子查询的支持
18. 6491267649 优化：流计算虚拟超级表触发机制新增对「子表增删、子表 Tag 值修改、列映射关系调整」的支持
19. 6491136498 优化：流计算支持多分组批量计算
20. 6491072341 优化：流计算支持批量删除
21. 6490870739 优化：流计算新增 NODELAY_CREATE_SUBTABLE 选项，支持在无数据写入时提前创建结果表
22. 6617004723 优化：优化子查询作为主键过滤条件时的性能
23. 6506970855 优化：优化 interp 的 fill(prev/next/near/linear) 填充行为，支持填充前 / 后非 NULL 值
24. 6510828917 优化：join/window join 支持基于选择函数结果集运算
25. 6570714028 优化：SHOW QURIES 语句新增显示查询执行进度的能力
26. 6545510969 优化：升级 explain analyze 功能，修正算子执行时间统计偏差，新增多类算子指标，提升结果可读性
27. 6665488038 优化：优化慢日志的上报逻辑，提升上报的准确性与及时性
28. 6659988199 优化：为 SQL 语句提供更加明确的报错信息
29. 6668153717 优化：修正 NULL 值在 IN 运算符中的比较结果
30. 6670404791 优化：新增防 SQL 注入安全防护能力
31. 6670390631 优化：新增防拒绝服务攻击（DoS）防护能力
32. 6670169846 优化：新增防溢出防护能力
33. 6641346408 优化：新增敏感数据删除后的强制覆盖能力
34. 6641435300 优化：新增配置参数 AuditSaveInSelf，开启时审计信息无需经过 taoskeeper 记录
35. 6841578151 优化：新增 enableGrantLegacySyntax 参数支持，取值为 1 时 v3.4 授权语法兼容 v3.3 版本
36. 6827224903 优化：移除 forceReadConfig 参数
37. 6927834192 优化：优化系统时间变更后的定时器处理逻辑
38. 6490634781 优化：订阅场景下密码错误时，返回与 taos 连接一致的 "Authentication failure" 认证失败错误信息
39. 6835915394 优化：支持修改 ROOT 用户的 PASSWORD_LIFE_TIME、PASSWORD_REUSE_TIME、PASSWORD_REUSE_MAX、ALLOW_TOKEN_NUM 属性，并将密码最小有效期默认设置为 1 天
40. 6841566765 修复：新增配置项 enableAdvancedSecurity（默认值为 0），取值为 0 时关闭密码过期、强密码、密码轮换等策略（与 v3.3 保持一致）
41. 6638288147 优化：禁止社区版与企业版互连
42. 6484950091 优化：TDgpt 服务端采用 Gunicorn 替代 uWSGI 作为服务驱动
43. 6612673602 优化：taos shell 支持通过 token 登录
44. 6834892123 优化：taosdump 支持 STMT2 方式写入
45. 6928072333 优化：Explorer 会话支持自动续期
46. 6860882946 优化：Explorer 授权页面优化
47. 6829477393 优化：Explorer 优化数据源、Transform UI 交互
48. 6663139939 优化：Explorer 登录支持启用 CAPTCHA
49. 6622581453 优化：Explorer JSON 解析规则输入框支持缩放
50. 6923332287 优化：Explorer 激活码显示框可自动扩展
51. 6599885679 优化：C/Rust websocket 连接器安全优化
52. 6922990934 优化：JDBC taos-ws 文档始终设置 varcharAsString=true
53. 6921450121 优化：OPC 自定义标签属性值支持字符替换
54. 6865524240 优化：taosX 备份支持在获取 Offset 失败时正常生成备份点

### 修复

1. 6929465514 修复：taosX 大量 CSV 同时处理时 BreakPoint 锁竞争问题
2. 6929218255 修复：Kafka 数据接入数组类型写入格式统一为 JSON
3. 6929156886 修复：Transform 页面子表名配置自动移除首尾空格
4. 6755743701 修复：Parser 参数超长错误信息不明确
5. 6927636146 修复：OPCDA 偶发采集中断
6. 6924787424 修复：XNODE 数据复制任务无法进入运行状态
7. 6880875720 修复：导入任务时无法选择数据库
8. 6873490637 修复：OPCDA 无法枚举出点位的问题
9. 6871997016 修复：TDinsight taosX 面板任务信息显示错误
10. 6871981414 修复：停止 taosX 后任务没有进行迁移
11. 6870274784 修复：taosX 同步 VARBINARY/BLOB 类型报错
12. 6856696160 修复：SHOW XNODE TASKS 结果按照 ID 排序
13. 6929175237 修复：taosdump 恢复数据库时未包含 VGroups 信息的问题
14. 6593026343 修复：从系统表 ins_streams 中查询到的 SQL 语句内容不完整的问题
15. 6578680074 修复：查询虚拟超级表时触发 "Invalid value in client" 错误的问题
16. 6843756344 修复：查询虚拟子表的时间戳列和数据列时，若数据列未引用源表，查询会报告 Planner slot key not found 错误
17. 6842312971 修复：数据迁移任务在 taosd 不正常时任务状态不符合预期的问题
18. 6842208309 修复：流计算中 Agg 聚合结果的过滤条件不生效的问题
19. 6841998099 修复：虚拟表执行聚合查询且包含 partition by tag 子句时触发程序崩溃的问题
20. 6927860029 修复：虚拟子表列类型与映射源表列类型不一致（通常由增删同名列导致）引发查询异常的问题
21. 6788971938 修复：虚拟表参数绑定查询时的崩溃问题
22. 6872094106 修复：查询虚拟超级表时偶现的 crash 问题
23. 6835695315 修复：流计算 sliding (0s) 错误返回创建成功的问题
24. 6803294489 修复：SAMPLE 函数在数据量超出 int32 范围时可能触发 crash 的问题
25. 6789525493 修复：DROP TSMA 语句指定 IF EXISTS 选项不生效的问题
26. 6747082115 修复：explain 解析子查询时无返回结果的问题
27. 6617593607 修复：流计算使用 FILL_HISTORY 计算历史数据失败的问题
28. 6606723544 修复：修复流计算结果表已存在时的 schema 与 tag 同步失败问题
29. 6832148756 修复：执行 Last 查询读取 BLOB 类型数据时触发程序崩溃的问题
30. 6830111185 修复：在表达式或函数中调用 trim 函数时触发程序崩溃的问题
31. 6799007996 修复：流计算场景下包含 JOIN 操作的查询结果集为空时仍输出数据的问题
32. 6766024000 修复：流计算查询语句中使用常量作为起始时间过滤条件时返回结果错误的问题
33. 6643607743 修复：流计算场景下临时文件释放逻辑错误，导致运行期磁盘空间持续增大的问题
34. 6919719400 修复：两个包含聚合查询的语句通过 UNION ALL 合并时可能触发 crash 的问题
35. 6928829824 修复：虚拟超级表在会话窗口查询中结果不正确的问题
36. 6838000029 修复：比较两个标签列且使用索引时的 taosd 进程崩溃问题
37. 6925721805 修复：ssEnabled 动态修改范围配置错误的问题
38. 6925582013 修复：SS_CHUNKPAGES 参数默认值未生效的问题
39. 6841578238 修复：共享存储上传数据时执行 compact 操作报告 Operation not supported 错误的问题
40. 6841225129 修复：用户拥有删表权限时，执行 drop table if exists db.not_exist_table 语句仍报权限不足的问题
41. 6841185444 修复：拥有 sysinfo0 权限的用户查看无权访问字段时，与 v3.3.8 保持一致，均执行报错处理而非返回 NULL 值
42. 6921545652 修复：开启所有日志调试开关后，因日志快速大量生成导致 taosd CPU 占用率持续偏高的问题
43. 6593026468 修复：订阅 rawdata 数据失败问题
44. 6922112102 修复：订阅与流计算同时使用且并发度较高时，极低概率触发 crash 的问题
45. 6920983818 修复：订阅长期无数据返回时，部分 vnode 停止消费的问题
46. 6861895851 修复：Windows 平台下 dmp 文件生成机制异常的问题
47. 6918909234 修复：从 v3.3.8 升级至 v3.4.0.0-1 后再升级到 3.4.0.2 及后续版本，ROOT 用户执行部分操作时报权限不足的问题
48. 6780711826 修复：TDgpt 的 Docker 镜像中缺失 gcc 相关组件，导致 PyTorch 推理运行时执行失败的问题
49. 6735458295 修复：taos shell 中当子查询包含 LIMIT 子句时，仅显示前 100 行的行数控制逻辑失效的问题
50. 6928320924 修复：Explorer 写入配置错误时预览窗口不符合预期
51. 6856867748 修复：Explorer 删除拆分规则后报错
52. 6856202003 修复：Explorer 订阅权限留空时报错
53. 6796720783 修复：Explorer 任务过滤导致字段值丢失
54. 6916440530 修复：Explorer 密码特殊字符导致部分请求错误
55. 6919730792 修复：Explorer 调整 xor_allowed_duration_secs 参数不起作用
56. 6866632606 修复：Explorer 支持相对路径反向代理
57. 6924296368 修复：Explorer 数据复制任务报 Bad Request 错误
58. 6622823622 修复：Explorer SQL 注入问题
59. 6659862768 修复：taosKeeper/taosAdapter 安全增强

## English

### Features

1. 6702838952 feat: mcp server for TDengine TSDB
2. 6550634959 feat: support external window query with time range explicitly specified by subquery, suitable for complex analysis scenarios such as cross-event correlation, window reuse and hierarchical filtering
3. 6542129231 feat: support uncorrelated scalar subquery
4. 6659773695 feat: add support for the ANY/SOME/ALL/EXISTS/NOT EXISTS operators
5. 6491345559 feat: support batch modification of child table tag values
6. 6490755304 feat: stream adds support for scheduled calculation by natural week, month, quarter, and year dimensions
7. 6572489317 feat: add the timeout notification function for stream when no new data is written to source child tables/virtual child tables
8. 6593807450 feat: subscription function adds support for metadata changes of virtual tables
9. 6659792966 feat: data subscription supports token-based login
10. 6548007902 feat: add identity authentication functions
11. 6567926427 enh: add support for CAST and SUBSTR functions on BLOB type
12. 6487609391 feat: support the lag, lead, fill_forward function
13. 6506145499 feat: the fill clause of interval and interp supports surround time range
14. 6641525627 feat: support scalar subqueries without the FROM clause
15. 6510267752 feat: support non-correlated subqueries in IN operator
16. 6469793274 feat: taosd adds support for the data repair mode
17. 6793469667 feat: TDgpt adds support for the multivariate anomaly detection feature
18. 6622713900 feat: taosX add task owner privileges
19. 6658956251 feat: taosX support system & object level privileges
20. 6653327869 feat: taosX new data-in pSpace
21. 6751321432 feat: OPC-UA connection test support failover
22. 6718901244 feat: kafka transformer support filter to split messages to multiple schema
23. 6723594269 feat: KingHistorian dataset auto update
24. 6600045300 feat: KingHistorian support non-stop migration when end timestamp set empty
25. 6735513765 feat: go websocket subscription support token auth
26. 6641467177 feat: Go REST connector support TOKEN auth
27. 6735261785 feat: rust subscription support token auth
28. 6735116367 feat: c websocket subscription support token auth
29. 6622691504 feat: taosdump now support decimal data type
30. 6835134117 feat: taosdump now support BLOB data type
31. 6923365814 feat: taosgen support windows
32. 6751373446 feat: taosgen support setting log options in config file
33. 6622579928 feat: taosKeeper add endpoint for exporting all metrics
34. 6835426458 feat: Explorer support token response in login
35. 6506023136 feat: explorer support totp auth
36. 6658975929 feat: explorer support bearer token auth
37. 6625571859 feat: support multitple brokers in MQTT task

### Enhancements

1. 6646294817 enh: taosX now works on Windows
2. 6861933885 enh: add support for TDgpt functionality on the Windows platform
3. 6857094454 enh: add support for stream functionality on the Windows platform
4. 6922189593 enh: improve the query performance of aggregate queries with GROUP BY on virtual super tables
5. 6862634689 enh: optimize the query performance of interval, session, event windows for virtual super tables
6. 6755544717 enh: optimize query performance when virtual tables reference many columns of physical tables
7. 6694539984 enh: optimize the query performance of state windows for virtual super tables
8. 6548485194 enh: push down the time filter conditions of non-correlated subqueries to improve the batch query performance of virtual tables
9. 6622781381 enh: return clear error information when virtual table creation fails, such as type matching failure of a specific column
10. 6589101088 enh: add reference verification capabilities between virtual tables and source tables, including reference relationship storage, source table change verification, reference relationship query and virtual table availability verification functions
11. 6643522153 enh: improve the performance of last_row queries
12. 6568211421 enh: support dynamic adjustment of LRU shard count to improve the execution performance of Last queries
13. 6930434043 enh: RESTORE DNODE command supports specifying VGROUP ID
14. 6554623504 enh: improve the subscription speed in the scenario of interlace writing
15. 6692120342 enh: add support for the IN operator in subqueries of stream
16. 6751417338 enh: add support for ANY/SOME/ALL/EXISTS/NOT EXISTS operators in stream
17. 6598056767 enh: add support for subqueries in stream computing scenarios
18. 6491267649 enh: add support for child table addition/deletion, child table tag value modification, and column mapping relationship adjustment to the trigger mechanism of virtual super tables in stream
19. 6491136498 enh: stream supports multi-group batch computation
20. 6491072341 enh: support batch deletion of stream computing
21. 6490870739 enh: add NODELAY_CREATE_SUBTABLE option for stream to create result tables in advance when no data is written
22. 6617004723 enh: improve the performance when subqueries are used as primary key filter conditions
23. 6506970855 enh: optimize the fill(prev/next/near/linear) filling syntax of the interp function to support filling with non-NULL values before/after the target position
24. 6510828917 enh: join/window join supports operations based on selection function resultsets
25. 6570714028 enh: display query execution progress for the SHOW QURIES statement
26. 6545510969 enh: upgrade the explain analyze function, fix the deviation in operator execution time statistics, add various operator indicators, improve result readability
27. 6665488038 enh: optimize the reporting logic of slow logs to improve the accuracy and timeliness of reporting
28. 6659988199 enh: provide clearer error messages for SQL statements
29. 6668153717 enh: correct comparison results of NULL values in the IN operator
30. 6670404791 enh: add security protection capabilities against SQL injection
31. 6670390631 enh: add protection capabilities against Denial of Service (DoS) attacks
32. 6670169846 enh: add protection capabilities against overflow attacks
33. 6641346408 enh: add the capability of forced overwriting after sensitive data deletion
34. 6641435300 enh: add support for the configuration parameter AuditSaveInSelf; when enabled, audit information is recorded without going through taoskeeper
35. 6841578151 enh: add support for the enableGrantLegacySyntax parameter; when set to 1, the authorization syntax of v3.4 is compatible with v3.3
36. 6827224903 enh: remove the forceReadConfig parameter
37. 6927834192 enh: optimize the timer logic after system time changes
38. 6490634781 enh: when the password is incorrect in the subscription scenario, return the same "Authentication failure" error message as that of taos connection
39. 6835915394 enh: support modifying the PASSWORD_LIFE_TIME, PASSWORD_REUSE_TIME, PASSWORD_REUSE_MAX, and ALLOW_TOKEN_NUM attributes of the ROOT user, and set the minimum password validity period to 1 day by default
40. 6841566765 enh: add the configuration item enableAdvancedSecurity (default value is 0), which disables the password expiration, strong password and password rotation policy when set to 0 (this behavior is consistent with v3.3)
41. 6638288147 enh: prohibit connection between Community Edition and Enterprise Edition
42. 6484950091 enh: TDgpt server uses Gunicorn instead of uWSGI as the service driver
43. 6612673602 enh: taos shell supports logging in via token
44. 6834892123 enh: taosdump add support for stmt2
45. 6928072333 enh: auto review session in explorer
46. 6860882946 enh: Explorer Licence page refinement
47. 6829477393 enh: improve explorer UI for data-in tasks and transform
48. 6663139939 feat: explorer can enable CAPTCHA on login
49. 6622581453 enh: Explorer auto scale json parser input
50. 6923332287 enh: Explorer activation input auto scale
51. 6599885679 enh: C/Rust websocket connector security enhancement
52. 6922990934 enh: JDBC doc recommend use varcharAsString=true for taos-ws
53. 6921450121 enh: OPC custom tag support special chars replacing
54. 6865524240 enh: taosX backup allow backup checkout when offset checking failed

### Fixes

1. 6929465514 fix: taosX breakpoint db lock race condition issue with multiple csv
2. 6929218255 fix: kakfa data-in use full list json instead of json items
3. 6929156886 fix: automatically trim spaces around sub table name in transform page
4. 6755743701 fix: long parser error mislead root cause
5. 6927636146 fix: OPCDA dataset collection possibly interrupt
6. 6924787424 fix: XNODE replication task can't start
7. 6880875720 fix: unable to select target database in tsdb import task dialog
8. 6873490637 fix: opcda retrieves empty data points
9. 6871997016 fix: TDinsight taosX panel task info error
10. 6871981414 fix: task not restored in other xnodes when one stopped
11. 6870274784 fix: taosX sync error with varbinary/blob datatype
12. 6856696160 fix: SHOW XNODE TASKS should be ordered by id
13. 6929175237 fix: vgroups information is not included when taosdump restores the database
14. 6593026343 fix: the SQL statement content queried from the system table ins_streams is incomplete
15. 6578680074 fix: "Invalid value in client" error is triggered when querying virtual super tables
16. 6843756344 fix: the query reports a "Planner slot key not found" error when querying the timestamp and data column of a virtual child table if the data column does not reference the source table
17. 6842312971 fix: unexpected task status for query migration tasks
18. 6842208309 fix: the filter conditions for Agg aggregation results in stream do not take effect
19. 6841998099 fix: taosd crashes when a virtual table executes an aggregation query with the partition by tag clause
20. 6927860029 fix: query exception caused by inconsistent column types between virtual child table and mapped source table (usually due to adding or deleting columns with the same name)
21. 6788971938 fix: crash when querying with parameter binding on virtual tables
22. 6872094106 fix: the occasional crash issue when querying virtual super tables
23. 6835695315 fix: stream computing sliding(0s) incorrectly returns success
24. 6803294489 fix: potential crash when the SAMPLE function processes data exceeding the int32 range
25. 6789525493 fix: the IF EXISTS option does not take effect when using DROP TSMA
26. 6747082115 fix: no results returned when explain parses subqueries
27. 6617593607 fix: stream fails to calculate historical data when using FILL_HISTORY
28. 6606723544 fix: schema and tag synchronization issue when the stream result table already exists
29. 6832148756 fix: taosd crashes when executing a Last query to read BLOB type data
30. 6830111185 fix: taosc and taosd crashes when the trim function is called in an expression or function
31. 6799007996 fix: data is still output when the result set of queries containing JOIN operations is empty in stream
32. 6766024000 fix: query result is incorrect when a constant is used as the start time filter condition in a stream query statement
33. 6643607743 fix: incorrect logic for releasing temporary files in the stream leads to continuous increase of disk space during runtime
34. 6919719400 fix: the potential crash issue triggered when two statements with aggregate queries are combined via UNION ALL
35. 6928829824 fix: incorrect query result issue of session window on virtual super tables
36. 6917179306 fix: fix the potential crash issue when interval operator read SMA data instead of raw data
37. 6838000029 fix: taosd crash issue when comparing two tag columns with index usage
38. 6925721805 fix: incorrect configuration of the dynamic modification range for ssEnabled
39. 6925582013 fix: the default value of the SS_CHUNKPAGES parameter does not take effect
40. 6841578238 fix: the "Operation not supported" error is thrown when the compact operation is executed during data upload in the shared storage scenario
41. 6841225129 fix: the "Permission denied or target object not exist" error is reported when executing drop table if exists db.not_exist_table even though the user has the table drop permission
42. 6841185444 fix: when a user with sysinfo0 permission views fields without access rights, the behavior is consistent with version v3.3.8, where an error is thrown instead of returning a NULL value
43. 6921545652 fix: taosd CPU usage remains high due to rapid generation of a large number of logs after enabling all log debug switches
44. 6593026468 fix: resolve the issue of failed rawdata data subscription
45. 6922112102 fix: the extremely low-probability crash issue that occurs when subscriptions and stream computing are used simultaneously with high concurrency
46. 6920983818 fix: some vnodes stop consuming when subscriptions return no data for a prolonged period
47. 6861895851 fix: fix the dmp file generation mechanism on Windows platform
48. 6918909234 fix: the ROOT user reports insufficient permissions when performing some operations after upgrading from v3.3.8 to v3.4.0.0-1 and then to 3.4.0.2 and later versions
49. 6780711826 fix: PyTorch inference runtime execution fails due to missing gcc-related components in the TDgpt Docker image
50. 6735458295 fix: the logic for limiting display to the first 100 rows fails in taos shell when a subquery contains the LIMIT clause
51. 6928320924 fix: Explorer preview table unexpected when parsing error
52. 6856867748 fix: explorer extraction rules cause error when removed
53. 6856202003 fix: subscription empty cause user permission error in explorer
54. 6796720783 fix: data-in filter cause parameter lost in explorer
55. 6916440530 fix: passoword with special chars cause ws request error
56. 6919730792 fix: Explorer changing xor_allowed_duration_secs in toml not work
57. 6866632606 fix: explorer support relative-path in reverse-proxy
58. 6924296368 fix: replication task cause bad request error
59. 6622823622 fix: explorer sql injection vulnerability
60. 6659862768 fix: taosKeeper/taosAdapter security enhancement

## 行为变更

### 用户行为变更

1. 6927058167 未授权时，授权运行模块不再检查机器码变更，授权状态会一直保持在 ungranted 状态。
2. 6672169603 计算测点时，排除 log、审计数据库中已知的系统超级表对应的子表测点（暂无普通表，后续也不新增普通表）；为防止更新不及时，额外冗余 1000 测点，仅用于判断测点数是否超出授权测点时使用（仅影响建表或者添加列），show grants 均展示实际的测点值；虚拟表不计入测点统计（现有行为）。补充说明：如果 log 库 / 审计库不存在，直接使用新版本即可；如果 log 库 / 审计库已经存在，直接升级新版本不会触发测点重算，测点数仍然不正确。如果需要修正，请手工将 log 库 / 审计库对应的所有 vnode 的 vnode.json 中的 vndStats.timeseries 值重置为 0，重启后即可触发测点重算。vndStats.timeseries 为 vnode 中子表的测点数，vndStats.ntimeseries 为 vnode 中普通表的测点数。如果要修正测点数，只需要将 vndStats.timeseries 重置为 0，vndStats.ntimeseries 不需要改动。
3. 6841566765 增加配置参数 enableAdvancedSecurity，设置为 1 时启用安全功能，设置为 0 时关闭安全功能。增加了编译选项 ADVANCED_SECURITY，默认为 false。设置为 true 时，enableAdvancedSecurity 的默认值为 1，否则为 0。注：关闭安全功能时，不影响强密码策略的状态，但允许使用默认密码，已经可以保证默认行为与 3.3 一致。
4. 6641435300 增加配置参数 AuditSaveInSelf，开启后审计信息不通过 taoskeeper。创建审计库时同时创建 operations 超级表。
5. 6593807450 新增配置参数 tmqWriteRefDB、tmqWriteCheckRef
6. 6827224903 删除配置参数 forceReadConfig
7. 6755544717 虚拟表可以引用的原始表数量上限从 2000 调整为 unlimited
8. 6694539984 移除 WIN_OPTIMIZE_BATCH 和 WIN_OPTIMIZE_SINGLE 两个 HINT
9. 6668153717 NULL 值参与比较运算时结果为 NULL，IN/NOT IN 中 NULL 值比较规则有变化
10. 6659773695 新增关键字 ANY、SOME
11. 6550634959 新增关键字 EXTERNAL_WINDOW
12. 6638288147 禁止社区版与企业版互连
13. 6568211421 新增数据库参数且支持修改 CACHESHARDBITS，新增关键字 CACHESHARDBITS
14. 6659862768 taosAdapter 和 taosKeeper 添加 readHeaderTimeout 10 秒以防御 Slowloris 攻击
15. 6929175237 taosdump 恢复时数据库 VGROUPS 数据有变化
16. 6925721805 3.3.7.0 - 3.4.0.14 之间的版本应当避免通过 alter dnodes 修改配置参数 ssEnabled。
17. 6925582013 3.3.8.20 - 3.3.8.22 和 3.4.0.10 - 3.4.0.14 之间的版本，新建的 DB 默认建库参数为 SS_CHUNKPAGES。如果使用 S3，上传的文件切分大小不为默认的 512 MB，而是 16 MB，导致 S3 上传频率较高，且 S3 上的文件偏大。从其他低版本升级上来的已创建 DB 不受影响，只有新建 DB 默认参数有影响。如果在使用这几个版本且要新建库，如果未来有可能使用 S3，临时先指定参数建库，例如：create database d1 ss_chunkpages 131072；
18. 6841185444 拥有 sysinfo0 权限的用户查看无权访问字段时，与 v3.3.8 保持一致，均执行报错处理而非返回 NULL 值
19. 6835915394 支持修改 ROOT 用户属性：PASSWORD_LIFE_TIME、PASSWORD_REUSE_TIME、PASSWORD_REUSE_MAX、ALLOW_TOKEN_NUM，将密码最小有效期设置为 1 天
