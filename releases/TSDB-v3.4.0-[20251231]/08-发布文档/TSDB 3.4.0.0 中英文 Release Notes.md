# TSDB 3.4.0.0 中英文 Release Notes

### 1. 中文

```markdown

### 2. 特性

   - TD-38445 特性：Adapter 支持连接器类型版本信息
   - TD-38555 特性：C WebSocket 支持 TLS 扩展参数
   - TS-5665  特性：添加 taosAdapter 实例注册和查询
   - TS-6020  特性：支持解析 JSON 格式入库
   - TS-6142  特性：添加 Rust 连接器性能测试工具
   - TS-6299  特性：taosX 高可用，支持 Kafka 负载均衡
   - TS-7198  特性：TDgpt 支持 dtw、dtw_path、tlcc 等行相关性分析函数
   - TS-7229  特性：数据传输支持 SASL 机制与资源管控 [企业版]
   - TS-7230  特性：数据存储支持全量透明加密 [企业版]
   - TS-7231  特性：支持多因素认证及多种登录安全策略 [企业版]
   - TS-7232  特性：支持基于 RBAC 的权限架构 [企业版]
   - TS-7233  特性：支持分级审计与权限管控 [企业版]
   - TS-7235  特性：支持 SM4_ENCRYPT、SM4_DECRYPT、AES_ENCRYPT、AES_DECRYPT、MD5、SHA、SHA1、SHA2、MASK_FULL、MASK_PARTIAL、MASK_NONE、FROM_BASE64、TO_BASE64 函数
   - TS-7276  特性：流计算事件窗口触发支持子事件窗口
   - TS-7354  特性：Explorer 支持 OAuth 2.0/OIDC SSO
   - TS-7422  特性：taosAdapter 自定义 SQL 拦截规则
   - 6488942152 特性：添加身份鉴别函数

### 3. 优化

   - TS-6665  优化：虚拟表支持的最大列数提升至 32767 列
   - TD-29642 优化：禁止在非聚合查询中对聚合函数进行排序
   - TD-37942 优化：状态窗口支持通过 zeroth_state 指定“零状态”，处于该状态的窗口将跳过计算与输出
   - TD-38063 优化：优化因日志文件目录无写权限导致 taosc 初始化失败时的错误信息
   - TD-38139 优化：流计算支持 interp 和 percentile 函数
   - TD-38148 优化：降低流计算触发数据与计算数据读取的资源消耗
   - TD-38456 优化：禁止在超级表的 state_window、count_window 与 event_window 中使用重复时间戳
   - TD-38533 优化：改进 taosmqtt 的退出处理逻辑，实现更优雅的停机和资源释放
   - TS-5925  优化：设定全局时区以降低并发查询与写入对时间转换相关的锁竞争
   - TS-6102  优化：窗口查询无需强制包含聚合函数，支持仅包含 _wstart、tbname 等伪列
   - TS-6472  优化：Linux 的 tar 安装包支持非 root 用户安装 TDengine TSDB，并可自定义安装路径
   - TS-6562  优化：修改标签列后，支持通过执行 RELOAD TOPIC 命令使数据订阅无需重建即生效
   - TS-6863  优化：支持在审计日志中记录查询、删除等操作
   - TS-6919  优化：show connections 命令新增客户端版本号字段
   - TS-7018  优化：TDgpt 的 ins_anodes_full 表新增状态及说明字段，实时反馈模型和算法状态
   - TS-7132  优化：增加 WIN_OPTIMIZE_BATCH 和 WIN_OPTIMIZE_SINGLE 两个 Hints 参数，用于提升虚拟表状态窗口的查询性能
   - TS-7270  优化：支持加密算法管理 [企业版]
   - TS-7346  优化：新增集群间通信的时钟检查机制
   - TS-7348  优化：新增 stmt2 对虚拟表查询的支持
   - TS-7540  优化：compact 命令支持 force 选项
   - TS-7591  优化：提升虚拟超级表聚合函数与选择函数的查询性能
   - TS-6865  优化：新增用户登录失败策略的支持能力
   - TS-7693  优化：优化 OPC-DA 采集和重连
   - TS-5982  优化：执行计划支持显示标签索引
   - 6487586808 优化：show vgroups 命令新增 is_ready 列
   - 6570501686 优化: stmt2 写入时新增布尔类型校验
   - 6551611200 优化：调整 stmt2 的日志，便于问题定位
   - 6506048792 优化：流计算的事件窗口仅在满足 true_for 条件时生成窗口开启通知
   - 6497313576 优化：优化 RPC 通信过程中读写锁的使用逻辑
   - 6593858156 优化：标签值无变化时更新子表标签操作立刻返回，避免频繁更新数据订阅的子表集合

### 4. 修复

   - TD-37246 修复：流计算触发数据重算时未使用最新的 WAL 版本号，导致计算结果错误的问题
   - TD-38284 修复: order by abs (col) 中 col 出现在结果集而非表中时报错的问题
   - TD-38640 修复: TAG 列类型为 JSON 时，show create table 及 SELECT distinct tbname, json_tag_name 查询结果不符合预期的问题
   - TD-37606 修复：数据订阅与 tdb 修改操作同时执行引发的并发问题
   - TD-38514 修复：OPCUA 修改任务时，下载点位数据失效
   - TS-7721  修复：创建流语句解析输出表 tag 名时，误将反引号 (`) 纳入列名，与手动创建的表列名不匹配，导致报 “Out table tag type mismatch” 错误的问题
   - TS-7676  修复：嵌套查询的子查询在逆序排序、无排序、非时间主键排序时查询结果错误
   - 6494377671 修复：Grafana 12.x 支持导入告警面板
   - 6639635535 修复：系统表 ins_vgroups 的 cache_load 字段超过 int32 最大值时显示为负值的错误
   - 6624752477 修复：修复查询内存管控时潜在的 crash 隐患
   - 6617593607 修复：流计算使用 FILL_HISTORY 计算历史数据失败的问题
   - 6616813301 修复：流计算读取数据报错导致后续计算任务持续等待的问题
   - 6613241466 修复: JOIN 语法中子表时间主键为常量时无需再排序
   - 6612628859 修复：Kafka 提取或拆分配置中 depth 错误
   - 6606723544 修复：修复流计算结果表已存在时的 schema 与 tag 同步失败问题
   - 6593026468 修复：订阅 rawdata 数据失败问题
   - 6591440017 修复：Explorer 默认上报监控信息
   - 6578738895 修复: STMT2 写入自动建表时，表名过长导致 crash 的问题
   - 6578680074 修复: 查询虚拟超级表时触发 "Invalid value in client" 错误的问题
   - 6555444586 修复：KingHistorian 实时订阅不能订阅到数据
   - 6554558952 修复：使用 insert into stable语法写入数据时 tbname 处理逻辑存在错误的问题
   - 6496202256 修复：流计算通过 WebSocket 发送事件通知时存在概率性失败的问题
   - 6495965587 修复：部分 req_id 在其他组件中找不到的问题
   - 6494389715 修复：node.js 连接器默认不设置 timezone
   - 6492331838 修复：Explorer 登录错误
   - 6492220991 修复：修改全局变量时事务无法结束的问题
   - 6490695158 修复：OPC-UA 点位数多时数据写入延迟明显
   - 6487976526 修复：多个虚拟表的列使用同一原始表列导致的逻辑错误
   - 6483553723 修复: stmt 查询返回后回调函数未被调用导致 taosAdapter 崩溃的问题
   - 6581610795 修复：taosX 单条消息超过 100k 可能导致 SQL 超长
   - 6570627479 修复: insert into select 语句写入数据时，数据被写入错误的 dnode 且触发 “Vnode is closed” 错误的问题
   - 6570600210 修复: 流计算子查询中的关联查询包含 tag==%% n 条件会导致 coredump 的问题
   - 6527280584 修复：列拆分结果显示与预期不符
   - 6507005892 修复：C WebSocket taos_stmt2_get_fields 接口类型错误
   - 6506119206 修复：流计算中使用异常检测功能时调用了错误数据源
   - 6506118573 修复：通过 stmt2 执行查询语句时，若 dbname 和 tbname 均为`？`，导致查询进程卡住的问题
   - 6504067588 修复：InluxDB 无 tag 数据无法正常入库
   - 6491188721 修复: schemaless 写入场景下，并发修改内部表结构产生错误
   - 6487579264 修复: state window 结合 partition by 查询时返回结果错误的问题
   - 6482045823 修复：嵌套查询中外层 cols 函数执行出错的问题
   - 6596598754 修复：AVEVA Historian 数据写入任务配置页面异常
   - 6589381451 修复：虚拟表引用子表同名但不同类型数据列时查询会 crash 的问题
   - 6587537091 修复: 从 3.3.6.32-3.3.6.36 升级至 3.3.8.6-3.3.8.10 时出现的数据加载异常问题
   - 6585525351 修复: 开启缓存时，值为 NULL 的 decimal 类型数据落盘过程中偶发 crash 的问题
   - 6583546898 修复：客户端无法感知其他客户端对表结构的修改问题
   - 6574277804 修复：PI/PI Backfill 数据源包含句号时失败问题
   - 6568881982 修复：PI 创建任务报错 Not found model in DSN
   - 6517545856 修复：taosgen 导入特殊 csv 格式异常退出
   - 6499196890 修复: 流计算触发建表时因未携带写入数据导致 crash 的问题
```

### 5. English

```markdown

### 6. Features

   - TD-38445 feat: adapter add connector info attribute in ws connection
   - TD-38555 feat: C websocket support ssl/tls connection params
   - TS-5665  feat: support taosAdapter instances register and query
   - TS-6020  feat: parse and write any JSON data to database
   - TS-6142  feat: add rust connector benchmark tool
   - TS-6299  feat: support taosX high-availability and Kafka task load balancing
   - TS-7198  feat: TDgpt supports row correlation analysis functions such as dtw, dtw_path and tlcc
   - TS-7229  feat: data transmission supports SASL mechanism and resource control [Enterprise Edition]
   - TS-7230  feat: data storage supports full transparent encryption [Enterprise Edition]
   - TS-7231  feat: support multi-factor authentication (MFA) and multiple login security policies [Enterprise Edition]
   - TS-7232  feat: support RBAC-based permission architecture [Enterprise Edition]
   - TS-7233  feat: support hierarchical auditing and permission control [Enterprise Edition]
   - TS-7235  feat: Support SM4_ENCRYPT, SM4_DECRYPT, AES_ENCRYPT, AES_DECRYPT, MD5, SHA, SHA1, SHA2, MASK_FULL, MASK_PARTIAL, MASK_NONE, FROM_BASE64 and TO_BASE64 functions
   - TS-7276  feat: event window triggering of stream supports sub-event windows
   - TS-7354  feat: Explorer add support for OAuth 2.0 and OIDC SSO
   - TS-7422  feat: taosAdapter support customized SQL rejection rules
   - 6488942152 feat: add identity authentication functions

### 7. Enhancements

   - TS-6665  enh: increase the maximum number of columns supported by virtual tables to 32767
   - TD-29642 enh: prohibit sorting by aggregate functions in non-aggregate queries
   - TD-37942 enh: state windows now support specifying a "zero state" via zeroth_state, skipping computation and output for windows in this state.
   - TD-38063 enh: optimize the error message when taosc initialization fails due to insufficient write permissions for the log file directory
   - TD-38139 enh: stream now supports interp and percentile functions
   - TD-38148 enh: reduce resource consumption of trigger data and computation data reading in stream computing
   - TD-38456 enh: prohibit the use of duplicate timestamps in state_window, count_window and event_window on super tables
   - TD-38533 enh: improved the exit handling logic of taosmqtt to achieve a more graceful shutdown and resource release
   - TS-5925  enh: set the global timezone to reduce lock contention related to time conversion during concurrent queries and writes
   - TS-6102  enh: window query do not require mandatory aggregate functions, support including only pseudo-columns like _wstart and tbname
   - TS-6472  enh: Linux tar package supports non-root installation of TDengine TSDB with custom install path
   - TS-6562  enh: after modifying tag columns, support data subscription to take effect without reconstruction by executing the RELOAD TOPIC command
   - TS-6863  enh: support recording operations such as query and deletion in audit logs
   - TS-6919  enh: add the client version number field to the show connections command.
   - TS-7018  enh: add status and note fields to the ins_anodes_full table for real-time feedback on model and algorithm status
   - TS-7132  enh: add two Hints parameters (WIN_OPTIMIZE_BATCH and WIN_OPTIMIZE_SINGLE) to improve the state window query performance of virtual tables
   - TS-7270  enh: support encryption algorithm management [Enterprise Edition]
   - TS-7346  enh: add clock check mechanism for communication between clusters
   - TS-7348  enh: add support for virtual table queries in stmt2
   - TS-7540  enh: compact command now support the force option
   - TS-7591  enh: improve the query performance of aggregate and selection functions for virtual super tables
   - TS-6865  enh: support for user login failure policies
   - TS-7693  enh: optimize OPC-DA collection and auto-reconnection
   - TS-5982  enh: execution plan supports displaying tag indexes
   - 6487586808 enh: add the 'is_ready' column to the show vgroups command
   - 6570501686 enh: add bool type validation during stmt2 writing
   - 6551611200 enh: adjust the logs of stmt2 to facilitate problem troubleshooting
   - 6506048792 enh: event window of stream generates window opening notifications only when the true_for condition is met
   - 6497313576 enh: optimize the usage logic of read-write locks in RPC communication
   - 6593858156 enh: return immediately when updating sub-table tags if the tag value remains unchanged, avoiding frequent updates to the sub-table set of data subscriptions

### 8. Fixes

   - TD-37246 fix: the latest WAL version number was not used when stream triggered data recalculation, resulting in incorrect calculation results
   - TD-38284 fix: query error when col in order by abs(col) exists in result set instead of table
   - TD-38640 fix: the results of show create table and SELECT distinct tbname, json_tag_name are not as expected when the TAG column is of JSON type
   - TD-37606 tix: concurrency issue caused by modifying tdb while data subscription is in progress
   - TD-38514 fix: OPCUA download data points not work when edit
   - TS-7721  fix: when parsing the output table tag name in the stream creation statement, the backtick (`) was mistakenly taken as part of the column name, which did not match the column name of the table created manually via SQL, resulting in the "Ou
   - TS-7676  fix: resolve incorrect query results when subqueries in nested queries use descending sorting, no sorting, or sorting by non-time primary key
   - 6494377671 fix: support import alert dashboard for Grafana 12.x
   - 6639635535 fix: the cache_load field of ins_vgroups is displayed as a negative value when it exceeds the maximum value of int32
   - 6624752477 fix: potential crash risks during query memory control
   - 6617593607 fix: stream fails to calculate historical data when using FILL_HISTORY
   - 6616813301 fx: stream errors when reading data cause subsequent computing tasks to wait
   - 6613241466 fix: no longer require sorting when the time primary key of sub-tables in JOIN syntax is a constant
   - 6612628859 fix: Kafka extract/split with depth error
   - 6606723544 fix: schema and tag synchronization issue when the stream result table already exists
   - 6593026468 fix: resolve the issue of failed rawdata data subscription
   - 6591440017 fix: Explorer upload monitor to taoskeeper by default
   - 6578738895 fix: crash caused by excessively long table names during STMT2 automatic table creation on write
   - 6578680074 fix: "Invalid value in client" error is triggered when querying virtual super tables
   - 6555444586 fix: KingHistorian subscription not work
   - 6554558952 fix: incorrect tbname processing logic when writing data via the insert into stable syntax
   - 6496202256 fix: the issue of probabilistic failure when stream sends event notifications via WebSocket
   - 6495965587 fix: taosX req_id can't find in other components
   - 6494389715 fix: node.js support no timezone on window
   - 6492331838 fix: Explorer login error without subpath
   - 6492220991 fix: transactions cannot end when modifying global variables
   - 6490695158 fix: fix latency issue in opc-ua
   - 6487976526 fix: logic error caused by columns of multiple virtual tables using the same original table column
   - 6483553723 fix: taosAdapter crash caused by callback function not being called after stmt query returns
   - 6581610795 fix: taosX my cause sql too long with 100k message per row
   - 6570627479 fix: data is written to an incorrect dnode and triggers the "Vnode is closed" error when executing insert into select statements
   - 6570600210 fix: coredump occurs when the join query in stream subquery contains the tag==%%n condition
   - 6527280584 fix: unexpected split result in transform
   - 6507005892 fix: field_type error in c ws taos_stmt2_get_fields
   - 6506119206 fix: incorrect data source was invoked when using anomaly detection in stream
   - 6506118573 fix: the issue that query processes get stuck when both dbname and tbname are set to `?` in query statements executed via stmt2
   - 6504067588 fix: InfluxDB no tag measurements data in error
   - 6491188721 fix: errors occurring when modifying the internal table structure concurrently during schemaless writing
   - 6487579264 fix: incorrect results when querying with state window and partition by
   - 6482045823 fix: errors in the outer cols function of nested queries
   - 6596598754 fix: AVEVA Historian data in configration error
   - 6589381451 fix: resolve the crash issue when querying virtual tables that reference data columns of the same name but different types in sub-tables
   - 6587537091 fix: abnormal data loading issue when upgrading from version 3.3.6.32-3.3.6.36 to 3.3.8.6-3.3.8.10
   - 6585525351 fix: crash occurs occasionally during disk flushing for decimal-type data with NULL values when caching is enabled
   - 6583546898 fix: client cannot perceive table schema modifications made by other clients
   - 6574277804 fix: PI/PI-Backfill ingestion error with period sign
   - 6568881982 fix: fix not found model in dsn error while creating pi tasks
   - 6517545856 fix: fix taosgen coredump in case of inccorect csv format
   - 6499196890 fix: crash occurs when stream triggers table creation without carrying written data
```

### 9. 行为变更

```markdown

### 10. 行为变更

   - taosx 不兼容旧版本，无法升级，需要全新配置，升级前请联系研发部评估影响 
   - taosx-agent 无法兼容旧版本，需要 Agent 的暂缓升级
   - 禁止社区版与企业版互连。
   - SHOW TABLE DISTRIBUTE 不支持虚拟超级表。
   - 虚拟表支持的最大列数提升至 32767 列。
   - 禁止在超级表的 state_window、count_window 与 event_window 中使用重复时间戳。

### 11. 新增关键字

   - ALGR_NAME
   - ALGR_TYPE
   - DB_KEY
   - ENCRYPT_STATUS
   - ENCRYPT_ALGORITHMS
   - ENCRYPT_ALGR
   - IS_AUDIT
   - OSSL_ALGR_NAME
   - RELOAD
   - ROLE
   - ROLES
   - SVR_KEY
   - TOKEN
   - TOKENS
   - DRAIN
   - REBALANCE
   - XNODE
   - XNODES

### 12. 配置参数变更

   - 新增 auditHttps
   - 新增 auditLevel
   - 新增 authReq
   - 新增 authReqInterval
   - 新增 authReqUrl
   - 新增 authServer
   - 新增 auditUseToken
   - 新增 compareAsStrInGreatest
   - 新增 enableTLS
   - 新增 enableSasl
   - 新增 encryptExtDir
   - 新增 maxSQLLength
   - 新增 maxTsmaCalcDelay
   - 新增 multiResultFunctionStarReturnTags
   - 新增 rejectQuerySqlRegex 
   - 新增 rpcRecvLogThreshold
   - 新增 sessionMaxCallVnodeNum
   - 新增 sessionMaxConcurrency
   - 新增 sessionConnIdleTime
   - 新增 sessionConnTime
   - 新增 sessionControl
   - 新增 sessionPerUser
   - 新增 showFullCreateTableColumn
   - 新增 tsmaDataDeleteMark
   - 新增 timestampDeltaLimit
   - 移除 database-url 

```
