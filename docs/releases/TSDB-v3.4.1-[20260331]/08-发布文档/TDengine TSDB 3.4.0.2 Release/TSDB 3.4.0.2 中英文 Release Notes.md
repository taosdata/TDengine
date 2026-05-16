# TSDB 3.4.0.2 中英文 Release Notes

### 1. 中文

```plaintext

### 2. 特性

   - 特性：TSDB 初始化时新建 xnode
   - 特性：Explorer 许可证页面展示机器码便于复制
   - 特性：taosX Agent 适配 XNODE 高可用
   - 特性：taosX 支持导入 Parquet 文件
   - 特性：adapter 增加 STMT 的 SQL 输出
   - 特性：Rust 原生连接支持 STMT2
   - 特性：流计算和批查询的事件、状态窗口 true_for 判断支持持续时间与持续条数双条件

### 3. 优化

   - 优化：XNODE TASK 添加 created_by/labels 字段
   - 优化：STMT 查询支持 interval 参数绑定
   - 优化：为 SQL 语句提供更加明确的报错信息
   - 优化：优化登录校验逻辑，强制使用默认密码登录的 root 用户修改密码
   - 优化：完善数据加密对 bse 等文件的处理机制
   - 优化：增加用户 token 相关的通知机制
   - 优化：支持从旧版本数据加密机制集群升级至新版本
   - 优化：支持使用 create totp_secret 语法创建 TOTP 密钥
   - 优化：同时支持 IPv4 & IPv6 协议栈
   - 优化：支持更新数据加密密钥的过期时间和过期策略
   - 优化：MQTT Topic 支持任意字符
   - 优化：重命名 lag 函数为 fill_forward，后续重新开发兼容 MySQL 的 Lag 函数
   - 优化：提升虚拟表仅含单个原始表时 cols + last 查询的性能
   - 优化：支持用户修改权限控制
   - 优化：完善视图和审计相关的权限控制
   - 优化：处理 3.3 至 3.4 版本的权限兼容性
   - 优化：数据加密生效后禁止用户篡改配置文件
   - 优化：优化流计算定时触发的通知内容，新增触发开始时间与结束时间字段（沿用 windowStart 和 windowEnd 字段名）
   - 优化：虚拟表引用原始表的数目上限从 1000 调整为 2000
   - 优化：降低流计算查询语句包含虚拟表标签过滤条件时计算延迟
   - 优化：优化流计算查询语句的语法，支持 _twstart 等占位符在 ORDER BY 子句中使用

### 4. 修复

   - 修复：虚拟表执行 show create table 语句时返回结果错误
   - 修复：InfluxDB 数据迁移特殊情况下空指针错误
   - 修复：流计算在管理节点切主后因心跳消息处理不当导致的程序崩溃问题
   - 修复：UDF 函数初始化失败时的内存泄漏问题
   - 修复：taosX 迁移 influxdb 报错误语法存在 "\" 错误
   - 修复：流计算调用 concat 函数返回结果错误的问题
   - 修复：taoskeeper 不支持特殊字符密码的问题
   - 修复：多级存储迁移任务定时执行时间不准确的问题
   - 修复：AVEVA Historian 数据源迁移方式采集异常
   - 修复：状态窗口处理多分组数据时，个别分组窗口内数据全为 NULL 值导致计算结果错误的问题
   - 修复：删除流计算结果子表并重建后，计算结果出现异常的问题
   - 修复：STMT 写入并发较大量时客户端可能发生崩溃的问题
   - 修复：密码重用限制的默认时间配置错误问题
   - 修复：不带 FROM 子句的子查询执行 JOIN 操作时返回错误的问题
   - 修复：MySQL 任务在数据很少的情况下可能陷入阻塞
   - 修复：移除重复的报错信息
   - 修复：流计算中 true_for 选项在乱序数据写入场景下触发逻辑错误的问题
   - 修复：taosX 原生接口 TMQ 错误兼容性问题
   - 修复：xnoded 关闭逻辑导致 stop taosd 偶发卡住
   - 修复：删除用户后其已创建的订阅无法取消的问题
   - 修复：group_concat 函数与 count_window 联合使用时的 crash 问题
   - 修复：创建 OPC 的数据源任务报错
   - 修复：v3.4.0 版本 taosExplorer 设置用户权限失败的问题
   - 优化：最外层关联查询带 limit 但未指定 order by 时 limit 不生效的问题
```

### 5. English

```plaintext

### 6. Features

   - feat: Automatically create xnode during TSDB initialization
   - feat: Explorer License page shows machine code
   - feat: taosX-agent support xnode ha
   - feat: taosX support parquet files import
   - feat: add SQL recording for STMT in adapter
   - feat: Rust native connector support STMT2
   - feat: support dual conditions of duration and row count for true_for judgment in event and state windows of stream and batch query

### 7. Enhancements

   - enh: add column created_by/labels for XNODE TASK
   - enh: STMT query supports the binding of the interval parameter
   - enh: provide clearer error messages for SQL statements
   - enh: optimize the login verification logic to force root users who log in with the default password to change their passwords
   - enh: improve the processing mechanism of data encryption for bse and other files
   - enh: add notification mechanism related to user tokens
   - enh: support upgrading clusters with old-version data encryption mechanism to new-version one
   - enh: support creating TOTP secrets using the create totp_secret syntax
   - enh: support both IPv4 & IPv6 protocol stacks
   - enh: support updating expiration time and policy for data encryption keys
   - enh: allow any chars in MQTT topic
   - enh: rename lag function to fill_forward and redevelop Lag function compatible with MySQL in follow-up
   - enh: improve performance of cols + last queries on virtual tables with only one original table
   - enh: support modifying permission control for users
   - enh: improve permission control for views and audits
   - enh: handle permission compatibility from version 3.3 to 3.4
   - enh: prohibit users from tampering with configuration files after data encryption takes effect
   - enh: optimize the notification content of stream period triggers by adding the trigger start time and end time fields (retaining the windowStart and windowEnd field names)
   - enh: adjust the upper limit of the original tables referenced by virtual tables from 1000 to 2000
   - enh: reduce the computing latency when stream query statements contain virtual table tag filter conditions
   - enh: optimize the syntax of stream statements to support the use of placeholders such as _twstart in the ORDER BY clause

### 8. Fixes

   - fix: incorrect results when executing the show create table statement on virtual tables
   - fix: null pointer error when InfluxDB schema changes
   - fix: crash in stream caused by improper handling of heartbeat messages after mnode leader switchover
   - fix: memory leak that occurs when UDF function initialization fails
   - fix: influxdb error with backslash syntax in taosX
   - fix: incorrect results when invoking the concat function in stream
   - fix: special characters not supported in taoskeeper password  
   - fix: inaccurate scheduled execution time for multi-level storage migration tasks
   - fix: migration stuck in AVEVA Historian data-in
   - fix: incorrect results when the data in individual group windows is all NULL values during multi-group processing in state windows
   - fix: abnormal calculation results after deleting and rebuilding the result subtable of stream
   - fix: the client may crash when the concurrent write volume of STMT is high
   - fix: incorrect default time configuration issue for password reuse restrictions
   - fix: error while performing JOIN operations with subqueries without the FROM clause
   - fix: MySQL DataIn task may block when few data rows
   - fix: remove duplicate error details
   - fix: trigger logic error of the true_for option in stream when out-of-order data is written
   - fix: taosX native connection tmq error message
   - fix: possibly stuck on systemctl stop taosd caused by xnoded
   - fix: subscriptions created by a user cannot be canceled after the user is deleted
   - fix: crash while combined use of group_concat function and count_window
   - fix: create task error for opc
   - fix: failed to set user permissions in taosExplorer of v3.4.0 version
   - enh: optimize the execution logic of outer join queries to resolve the issue where limit does not take effect when order by is not specified

```

### 9. 行为变更

```markdown {wrap}

### 10. 行为变更

   - 6728731962 重命名 lag 函数为 fill_forward, 参数和行为不变

### 11. 新增关键字

### 12. 配置参数变更

### 13. 用户行为变更

   - 6641469804 新增配置项 allowDefaultPassword，仅在服务端首次启动时起作用；值为 1 时，默认用户可以使用默认密码，否则，默认用户登录后必须修改密码。
```
