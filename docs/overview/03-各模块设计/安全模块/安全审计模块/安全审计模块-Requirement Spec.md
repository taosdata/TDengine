# 安全审计模块-Requirement Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-09-11 | 2025-10-15 | 1.0 | 关胜亮 | 发布 |
| 2025-12-08 | 2025-12-11 | 1.1 | 程洪泽 | 修订及完善内容 |

## 2. 引言

### 2.1 术语与缩写名词

| **术语/缩写** | **全称/解释** |
| --- | --- |
| **DDL** | Data Definition Language，数据定义语言 |
| **DML** | Data Manipulation Language，数据操作语言 |
| **SQL** | Structured Query Language，结构化查询语言 |
| **WAL** | Write-Ahead Logging，预写式日志 |
| **SYSAUDITOR** | 系统审计员角色，具有审计相关权限 |
| **taosKeeper** | TDengine的守护进程，负责监控和审计功能 |

### 2.2 相关文档资料

JIRA [TS-7233](https://jira.taosdata.com:18080/browse/TS-7233?src=confmacro)

### 2.3 优先级要求

高

### 2.4 版本要求

1. 企业版支持
2. 社区版不支持

## 3. 需求目标

启用全面的数据库审计功能，记录并保护审计日志。
1. 补充更多操作日志记录信息，用户登录、权限变更、数据操作（INSERT/UPDATE/DELETE）等关键事件，审计记录应保留12个月以上
2. 记录 原始SQL 、执行时间、影响行数
3. 审计库的防暴力篡改
4. 审计信息传输的防篡改
5. 审计库的非法修改和查看
6. 审计库中对用户密码、Key 的保护
7. 审计信息可明文下载

## 4. 功能需求

### 4.1 审计分级

审计功能主要分为五个级别，增加配置参数，控制审计记录级别
1. 系统级：数据库系统本身，例如 数据库启动、关闭、参数修改、版本升级等
2. 集群级：DDL、DML 的关键操作，指子表操作之外的操作
3. 数据库级：数据库对象上的关键操作，指 DELETE 等发生几率较小的操作
4. 子表级：发生频率较大的操作，指子表创建、删除、修改
5. 数据级：发生频率较大的操作，指数据写入

### 4.2 审计项列表

1. 在已有 [审计文档](https://taosdata.feishu.cn/wiki/SiEdwCMGNiUiofkFON7cYB7Lnhh) 的基础上，重新梳理审计项目，并归类到 1-5 的分级中，在 FS 文档中详细说明
2. 必须包含用户登录、权限变更、数据操作等关键事件

### 4.3 审计权限管理

在 [访问控制需求文档](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg) 中，增加了 SYSAUDITOR 角色，以及与审计相关的权限控制。确保这些控制项是生效的，包括：
1. GRANT SYSAUDITOR PRIVILEGE
2. REVOKE SYSAUDITOR PRIVILEGE
3. ALTER AUDIT VARIABLE
4. SHOW AUDIT VARIABLE
5. CREATE AUDIT DATABASE
6. DROP AUDIT DATABASE
7. ALTER AUDIT DATABASE
8. READ AUDIT DATABASE
9. WRITE AUDIT DATABASE
10. READ INFORMATION_SCHEMA AUDIT

### 4.4 审计数据库

1. 支持审计类型数据库
2. 审计类型数据库需由具备 `CREATE AUDIT DATABASE` 权限的用户创建
3. 系统内此类数据库仅可存在一个

### 4.5 审计用户

1. 系统初始化后，将自动创建 SYSAUDITOR 角色
2. SYSAUDITIOR 会创建一个专用于记录审计日志的用户，该用户仅拥有写入审计信息的权限，并为其分配专属 token

### 4.6 审计信息传输

审计功能依赖 taosKeeper
1. taosd 和 taosKeeper 采用安全通信机制，防止审计信息传输篡改
2. taosKeeper 记录审计日志时，将 4.4 和 4.5 节提供的“审计数据库”、“审计用户” 一并提供给 taosKeeper

### 4.7 审计安全管理

1. 创建审计类型数据库时，强制审计记录的保存时间大于 5 年，且不可修改为更短时间
2. 审计类型的数据库 WAL 日志级别应为每次写入到落盘，保证安全性
3. 审计类型数据库必须是加密数据库库，防暴力篡改和非法查看
4. 审计日志需记录 原始 SQL 、执行时间、影响行数
5. 审计日志中不应记录敏感信息，如密码、TOKEN、KEY 等信息需要进行脱敏处理

## 5. 性能要求

可能带来一定的性能开销。在测试过程中，开启 3 级 审计时，对如下指标进行测试，如果性能不达预期，需优化代码。
1. **写入性能**：
   - 写入延迟增加不得超过 100%。
2. **查询性能**：
   - 查询延迟增加不得超过 100%。

## 6. 安全需求

见 4.5 审计安全管理 一节

## 7. 兼容性需求

1. 3.3.8 版本之前的审计库因不符合加密要求，需重新创建新的审计数据库
2. 使用新版本后，不能退回到旧版本
