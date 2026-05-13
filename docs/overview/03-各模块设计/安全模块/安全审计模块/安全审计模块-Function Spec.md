# 安全审计模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-31 | 2025-11-28 | 1.0 | 陈东明 | 新建 |
| 2025-12-08 | 2025-12-15 | 1.1 | 程洪泽 | 修订及完善内容 |

## 2. 背景

随着数据安全法规的日益严格和合规性要求的不断提高，数据库系统需要提供全面、可靠的安全审计功能。当前系统缺乏系统化的审计机制，无法满足等保 2.0、ISO27001 等安全标准对数据库审计的要求。为提升系统的安全性和合规性，需要建立完整的数据库操作审计体系，记录所有关键操作，实现操作可追溯、行为可监控、安全可保障。

## 3. 定义

| **术语/缩写** | **全称/解释** |
| --- | --- |
| **DDL** | Data Definition Language，数据定义语言 |
| **DML** | Data Manipulation Language，数据操作语言 |
| **SQL** | Structured Query Language，结构化查询语言 |
| **WAL** | Write-Ahead Logging，预写式日志 |
| **SYSAUDITOR** | 系统审计员角色，具有审计相关权限 |
| **taosKeeper** | TDengine的守护进程，负责监控和审计功能 |
| **审计数据库** | 专门用于存储审计日志的数据库类型 |
| **审计级别** | 审计操作的粒度级别，分为1-5级 |

## 4. 行为说明

### 4.1 分级配置参数

#### 4.1.1 审计级别配置参数

新增审计相关配置参数如下：
1. **auditLevel**：审计级别控制参数
  - `AUDIT_LEVEL_SYSTEM = 1`：系统级别，审计数据库系统本身的关键操作
  - `AUDIT_LEVEL_CLUSTER = 2`：集群级别，审计集群管理相关操作
  - `AUDIT_LEVEL_DATABASE = 3`：库级别，审计数据库对象上的关键操作
  - `AUDIT_LEVEL_CHILTABLE = 4`：子表级别，审计子表管理操作
  - `AUDIT_LEVEL_DATA = 5`：数据级别，审计数据写入和查询操作
1. **enableAuditSelect**：是否审计 select 操作
  - 类型：布尔值
  - 默认值：true
  - 生效条件：仅当 `auditLevel = AUDIT_LEVEL_DATA` 时生效
1. **enableAuditInsert**：是否审计 insert 操作
  - 类型：布尔值
  - 默认值：true
  - 生效条件：仅当 `auditLevel = AUDIT_LEVEL_DATA` 时生效
1. **enableAuditDelete**：是否审计 delete 操作
  - 类型：布尔值
  - 默认值：true
  - 生效条件：仅当 `auditLevel = AUDIT_LEVEL_DATA` 时生效
<quote-container>
注：原参数 `auditCreateTable`功能已由 `AUDIT_LEVEL_CHILTABLE`审计级别覆盖，故予以废弃。
</quote-container>

#### 4.1.2 动态修改配置参数

```sql
-- 设置审计级别为数据级别（5级）
ALTER ALL DNODES 'auditLevel' '5';

-- 启用SELECT操作审计
ALTER ALL DNODES 'enableAuditSelect' 'true';

-- 启用INSERT操作审计
ALTER ALL DNODES 'enableAuditInsert' 'true';

-- 启用DELETE操作审计
ALTER ALL DNODES 'enableAuditDelete' 'true';
```

### 4.2 审计操作及其级别

#### 4.2.1 系统级审计操作（级别 1）

系统级审计记录数据库系统本身的关键操作，包括：

| 操作 | ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- | --- |
| create dnode | 时间戳 | user1 | createDnode | n/a |  | 完整 SQL 语句 |
| drop dnode | 时间戳 | user1 | dropDnode |  | dnodeid1 | 完整 SQL 语句 |
| alter dnode | 时间戳 | user1 | alterDnode |  | dnodeId | 完整 SQL 语句 |
| create mnode | 时间戳 | user1 | createMnode |  | dnodeid1 | 完整 SQL 语句 |
| drop mnode | 时间戳 | user1 | dropMnode |  | dnodeid1 | 完整 SQL 语句 |
| create qnode | 时间戳 | user1 | createQnode |  | nodeid | 完整 SQL 语句 |
| drop qnode | 时间戳 | user1 | dropQnode |  | dnodeId | 完整 SQL 语句 |
| restore dnode/mnode/vnode/qnode | 时间戳 | user1 | restoreDnode |  | nodeid | 完整 SQL 语句 |

**审计记录字段说明**：
- `ts`：操作时间戳，精确到微秒
- `User`：执行操作的用户名
- `Operation`：操作类型
- `db`：操作的数据库（如适用）
- `resource`：操作的资源标识
- `Detail`：完整的SQL语句或操作详情

#### 4.2.2 集群级审计操作（级别 2）

集群级审计记录集群管理相关操作：

| 操作 | ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- | --- |
| alter cluster | 时间戳 | user1 | alterCluster | n/a | n/a | 完整 SQL 语句 |
| balance vgroup leader | 时间戳 | user1 | balanceVgroupLead | n/a | n/a | 完整 SQL 语句 |
| redistribute vgroup | 时间戳 | user1 | redistributeVgroup |  | vgId | 完整 SQL 语句 |
| balance vgroup | 时间戳 | user1 | balanceVgroup | n/a | n/a | 完整 SQL 语句 |
| assign leader | 时间戳 | user1 | assignLeader | n/a | n/a | 完整 SQL 语句 |
| grant privileges | 时间戳 | user1 | grantPrivileges |  | targetUserName1 | 完整 SQL 语句 |
| revoke privileges | 时间戳 | user1 | revokePrivileges |  | targetUserName1 | 完整 SQL 语句 |
| login | 时间戳 | user1 | login |  | ip:port | 客户端应用名称 |
| alter user | 时间戳 | operationUserName1 | alterUser | n/a | 被修改的参数和新值 | 参数变更详情 |
| create user | 时间戳 | operationUserName1 | createUser | n/a | 其它参数及其值 | 用户创建详情 |
| import user | 时间戳 | operationUserName1 | importUser | n/a | 其它参数及其值 | 用户导入详情 |
| drop user | 时间戳 | operationUserName1 | dropUser |  | user | 完整 SQL 语句 |
| grant privileges | 时间戳 | operationUserName1 | grantPrivileges | Db name | user | 完整 SQL 语句 |
| revoke privileges | 时间戳 | operationUserName1 | revokePrivileges | Db name | user | 完整 SQL 语句 |
| create mount | 时间戳 | operationUserName1 | createMount | mountName |  | 完整 SQL 语句 |
| drop mount | 时间戳 | operationUserName1 | dropMount | mountName |  | 完整 SQL 语句 |
| kill retention | 时间戳 | operationUserName1 | killRetention | Db name | id | 完整 SQL 语句 |
| auto trimdb | 时间戳 |  | autoTrimDB | Db name |  | 自动清理操作 |

说明：
- `objName`：修改DB、table 权限时，`objName` 为 DBname，修改 topic 权限时，`objName` 为 topicName
- 敏感信息脱敏：密码字段替换为 "***"，TOKEN显示前/后几位，中间用"*"代替

#### 4.2.3 库级别审计操作（级别 3）

库级别审计记录数据库对象上的关键操作：

| 操作 | ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- | --- |
| create database | 时间戳 | user1 | createDB | dbname1 | n/a | 完整 SQL 语句 |
| alter database | 时间戳 | user1 | alterDB | dbName1 | n/a | 完整 SQL 语句 |
| drop database | 时间戳 | user1 | dropDB | dbName1 | n/a | 完整 SQL 语句 |
| kill compact | 时间戳 | user1 | compact | dbName |  | 完整 SQL 语句 |
| compact | 时间戳 | user1 | compact | dbName |  | 完整 SQL 语句 |
| alter stable | 时间戳 | user1 | alterStb | dbName1 | stable name | 完整 SQL 语句 |
| create stable | 时间戳 | user1 | createStb | dbName1 | stable name | 完整 SQL 语句 |
| dropStb | 时间戳 | user1 | dropStb | dbName1 | stable name | 完整 SQL 语句 |
| create stream | 时间戳 | user1 | createStream |  | stream name | 完整 SQL 语句 |
| drop stream | 时间戳 | user1 | dropStream |  | stream name | 完整 SQL 语句 |
| recalc stream | 时间戳 | user1 | recalcStream | streamName | recalc name | 完整 SQL 语句 |
| create topic | 时间戳 | user1 | createTopic | dbName1 | topic name | 完整 SQL 语句 |
| drop topic | 时间戳 | user1 | dropTopic |  | topic name | 完整 SQL 语句 |
| drop rsma | 时间戳 | user1 | dropRsma | rsmaName |  | 完整 SQL 语句 |
| create rsma | 时间戳 | user1 | createRsma | rsmaName |  | 完整 SQL 语句 |
| alter rsma | 时间戳 | user1 | alterRsma | rsmaName | table name | 完整 SQL 语句 |
| create view | 时间戳 | user1 | createView | dbName | view name | 完整 SQL 语句 |
| drop view | 时间戳 | user1 | dropView | dbName | view name | 完整 SQL 语句 |

#### 4.2.4 子表级别审计操作（级别 4）

子表级别审计记录子表管理操作：

| 操作 | ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- | --- |
| create table | 时间戳 | user1 | createTable | db name | table name | 完整 SQL 语句 |
| drop table | 时间戳 | user1 | dropTable | dDb name | table name | 完整 SQL 语句 |

#### 4.2.5 数据级别审计操作（级别 5）

数据级别审计记录数据写入和查询操作：

| 操作 | ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- | --- |
| delete | 时间戳 | user1 | delete | db name | table name | 完整 SQL 语句 |
| insert | 时间戳 | user1 | insert | db name | table name | 完整 SQL 语句 |
| select | 时间戳 | user1 | select | db name | table name | 完整 SQL 语句 |

### 4.3 审计权限控制

#### 4.3.1 SYSAUDITOR 角色

系统自动创建 SYSAUDITOR 角色，具有审计相关权限：
```sql
-- 授予用户SYSAUDITOR角色
GRANT SYSAUDITOR PRIVILEGE TO 'audit_admin';

-- 撤销用户SYSAUDITOR角色
REVOKE SYSAUDITOR PRIVILEGE FROM 'audit_admin';
```

#### 4.3.2 审计相关权限

SYSAUDITOR 角色具有以下权限：
1. ALTER AUDIT VARIABLE：修改审计相关配置参数
2. SHOW AUDIT VARIABLE：查看审计相关配置参数
3. CREATE AUDIT DATABASE：创建审计类型数据库
4. DROP AUDIT DATABASE：删除审计类型数据库
5. ALTER AUDIT DATABASE：修改审计数据库属性
6. READ AUDIT DATABASE：读取审计数据库内容（需特殊授权）
7. WRITE AUDIT DATABASE：向审计数据库写入记录（仅审计用户）
8. READ INFORMATION_SCHEMA AUDIT：查询审计相关的系统表信息

#### 4.3.3 审计用户管理

系统自动创建审计专用用户：
1. 用户名：`_audit_user`（系统保留，用户不可见）
2. 权限：仅拥有 WRITE AUDIT DATABASE 权限
3. 身份认证：使用专属 token 进行身份验证
4. 自动管理：由系统自动创建、维护和销毁

### 4.4 审计信息传输

#### 4.4.1 传输协议

1. **审计信息传输机制**：审计信息将统一发送至 `taoskeeper`组件进行收集与处理。
2. **传输协议**：系统新增参数 `AuditHttps`用于控制传输协议
   - 设置为 `true`时，使用 HTTPS 协议传输，保障通信安全。
   - 设置为 `false`时，使用 HTTP 协议传输。
   - 该参数默认值为 `false`。
3. 实现依赖
当启用 HTTPS 时，系统将通过 CURL 的 C 语言库 发起请求。该库已存在于当前代码库中，此次实现为本地调用，不引入新的第三方依赖。

#### 4.4.2 传输接口

Http api 的地址为：
1. **单个传输**：/audit_v2?db=test&token=xxxxxxxx
2. **批量传输**：/audit-batch?db=test&token=xxxxxxxx
接收数据的格式为 json，字段为：
```bash
{
    "ts": timestamp,
    "cluster_id": string,
    "user": string,
    "operation": string,
    "db": string,
    "resource": string,
    "client_add": string,
    "details": string
    "affected_rows": Integer
    "duration":Double
}
```

**字段说明**：
- `ts`：操作时间戳，ISO 8601 格式
- `cluster_id`：集群标识符
- `user`：执行操作的用户
- `operation`：操作类型
- `db`：数据库名称
- `resource`：资源标识
- `client_add`：客户端地址（IP:Port）
- `details`：操作详情（完整 SQL 语句）
- `affected_rows`：影响行数
- `duration`：操作耗时（秒）

#### 4.4.3 传输安全机制

1. **TLS加密通信**：taosd 和 taosKeeper 之间采用 TLS 加密通信
2. **数字签名**：传输的审计信息包含数字签名，接收方可验证数据完整性
3. **断点续传**：网络中断时，审计信息可缓存并在恢复后继续传输
4. **重试机制**：传输失败时自动重试，最大重试次数可配置

### 4.5 审计库保存时间

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] audit 1;

database_options:
    database_option ...

database_option: {
  DURATION value
}
```

Audit 为1 时，duration 默认 为 1825d， 如果用户指定 duration，要求大于 1825d。

### 4.6 强制落盘策略

```plaintext
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] audit 1;

database_options:
    database_option ...

database_option: {
  WAL_LEVEL value
}
```

Audit 为 1 时，WAL_LEVEL 默认 为 2， 如果用户不能更改。

### 4.7 强制加密策略

```plaintext
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] audit 1;

database_options:
    database_option ...

database_option: {
  ENCRYPT_ALGORITHM value
}
```

Audit 为1 时，ENCRYPT_ALGORITHM 用户不能指定为 None，可以选择任意一种 CBC 模式的对称加密算法。

## 5. 性能

开启 3 级 审计时，性能指标如下：
1. **写入性能**：
   - 写入延迟增加不得超过 10%。
2. **查询性能**：
   - 查询延迟增加不得超过 10%。
开启 4 级 审计时，性能指标如下：
1. **创建子表性能**：写入延迟增加不得超过 100%。

## 6. 安全

1. 审计库的防暴力篡改 
2. 审计信息传输的防篡改 
3. 审计库的非法修改和查看
4. 强制审计记录的保存时间大于 5 年，且不可修改为更短时间 
5. 强制 WAL 日志级别应为每次写入到落盘

## 7. 兼容性

### 7.1 版本兼容性

1. **向前兼容**：
  - 3.3.8 版本之前的审计库因不符合加密要求，需重新创建新的审计数据库
  - 提供数据迁移工具，支持旧审计数据导入新格式
1. **向后兼容**：
  - 使用新版本后，不能退回到旧版本
  - 新版本审计格式不兼容旧版本

### 7.2 系统兼容性

1. **操作系统**：支持主流 Linux 发行版（CentOS 7+, Ubuntu 18.04+, RedHat 7+）
2. **硬件架构**：支持 x86_64 和 ARM64 架构
3. **网络环境**：支持 IPv4 和 IPv6 网络环境

### 7.3 客户端兼容性

1. **客户端版本**：支持所有版本客户端连接
2. **协议兼容**：审计信息不因客户端版本差异而丢失
3. **接口兼容**：保持现有接口的向后兼容性

## 8. 运维

### 8.1 日常运维

1. 监控审计系统状态：
  - 监控审计日志写入速率
  - 监控审计存储空间使用情况
  - 监控审计传输失败率
1. 定期检查：
  - 定期检查审计日志完整性
  - 定期验证审计数据可读性
  - 定期备份审计数据库

### 8.2 故障处理

1. 审计传输失败：
  - 检查taosKeeper服务状态
  - 检查网络连通性
  - 检查防火墙配置
1. 存储空间不足：
  - 监控磁盘使用情况
  - 设置存储空间告警阈值
  - 定期清理过期审计数据（按保留策略）
1. 性能问题：
  - 调整审计级别降低审计粒度
  - 优化审计配置参数
  - 扩容系统资源

### 8.3 备份恢复

1. 备份策略：
  - 定期全量备份审计数据库
  - 实时增量备份审计日志
  - 备份文件加密存储
1. 恢复流程：
  - 验证备份文件完整性
  - 恢复审计数据库
  - 验证恢复后审计功能正常

## 9. 使用场景

审计功能支持多种使用场景，下表列出了主要的使用场景及其配置建议和重点关注内容：

| 使用场景 | 场景描述 | 配置建议 | 重点关注内容 |
| --- | --- | --- | --- |
| 合规性审计 | 满足等保2.0、ISO27001等安全标准对数据库审计的要求。 | • 审计级别：3级（库级别）或以上 • 启用所有操作类型审计 • 设置合适的审计保留时间（≥5年） | • 所有用户登录/注销操作 • 所有权限变更操作 • 所有数据库对象创建/修改/删除操作 • 所有数据删除操作 |
| 安全监控 | 实时监控数据库操作，及时发现异常行为和潜在威胁。 | • 审计级别：5级（数据级别） • 启用SELECT操作审计（监控敏感数据访问） • 设置异常操作告警规则 | • 非工作时间的数据访问 • 高频度的失败登录尝试 • 异常的数据导出操作 • 权限提升尝试 |
| 故障排查 | 数据库出现异常时，通过审计日志追溯问题原因。 | • 审计级别：4级（子表级别）或以上 • 记录完整的SQL语句和执行结果 • 记录操作耗时和影响行数 | • 定位数据不一致的原因 • 分析性能问题的根源 • 追踪数据变更历史 |
| 责任追溯 | 发生安全事件时，追溯操作责任。 | • 审计级别：5级（数据级别） • 记录客户端IP地址和应用信息 • 确保审计日志的完整性和不可篡改性 | • 操作时间、用户、客户端信息 • 完整的操作语句 • 操作结果和影响 |

## 10. 约束和限制

### 10.1 功能约束

1. 审计数据库唯一性：系统内只能存在一个审计数据库
2. 审计级别限制：数据级别审计（5 级）仅在企业版中支持
3. 自定义审计项：不支持用户自定义审计项，仅支持预定义的审计操作

### 10.2 性能限制

1. 审计性能影响：开启高级别审计会对系统性能产生一定影响
2. 存储空间需求：审计日志需要额外的存储空间
3. 网络带宽需求：审计信息传输需要网络带宽

### 10.3 使用限制

1. 权限要求：审计相关操作需要 SYSAUDITOR 角色权限
2. 配置限制：审计数据库的安全属性有强制限制
3. 兼容性限制：新旧版本审计格式不兼容

### 10.4 技术限制

1. 加密算法限制：审计数据库仅支持 CBC 模式的对称加密算法
2. 时间精度限制：审计记录时间戳精度为微秒级
3. 存储格式限制：审计日志采用特定的存储格式

## 11. 常见错误和排查

下表列出了审计功能使用过程中可能遇到的常见问题及其排查方法：

| 问题类型 | 问题名称/现象 | 问题描述 | 可能原因 | 排查步骤 |
| --- | --- | --- | --- | --- |
| 常见错误 | Failed to send out audit record | 发送审计记录失败 | taosKeeper服务未启动、网络不通、防火墙阻挡 | 1. 检查 taosKeeper 服务状态：`systemctl status taoskeeper` 2. 检查网络连通性：`ping <taoskeeper_host>` 3. 检查端口访问：`telnet <taoskeeper_host> 6043` 4. 检查防火墙配置：`firewall-cmd --list-ports` |
| 常见错误 | Audit database creation failed | 创建审计数据库失败 | 权限不足、已存在审计数据库、参数错误 | 1. 检查用户权限：`SHOW GRANTS` 2. 检查是否已存在审计数据库：`SHOW DATABASES` 3. 检查创建语句语法 4. 查看错误日志：`tail -f /var/log/taos/taosdlog.0` |
| 常见错误 | Audit level configuration failed | 审计级别配置失败 | 参数值无效、权限不足、服务异常 | 1. 检查参数值是否在有效范围内（1-5） 2. 检查用户是否具有 ALTER AUDIT VARIABLE 权限 3. 检查 taosd 服务状态 4. 查看配置日志 |
| 性能问题 | 审计导致性能下降明显 | 开启审计后系统性能显著下降 | 审计级别设置过高、审计传输积压、系统资源不足 | 1. 检查审计级别设置，适当降低审计粒度 2. 检查审计传输是否正常，避免积压 3. 监控系统资源使用情况（CPU、内存、磁盘 I/O） 4. 优化审计配置参数 |
| 性能问题 | 审计日志积压 | 审计日志写入速度跟不上产生速度 | 存储性能不足、网络传输异常、审计级别过高 | 1. 检查存储性能，确保磁盘 I/O 正常 2. 检查网络传输是否正常 3. 考虑降低审计级别或减少审计项 4. 扩容系统资源 |
| 数据完整性问题 | 审计记录丢失 | 部分操作未记录到审计日志 | 审计级别设置不当、审计服务异常、网络传输故障 | 1. 检查审计级别设置，确认操作在审计范围内 2. 检查审计服务是否正常运行 3. 检查网络传输是否正常 4. 查看审计服务日志 |
| 数据完整性问题 | 审计记录不完整 | 审计记录字段缺失或错误 | 审计配置错误、客户端版本不兼容、审计服务版本问题 | 1. 检查审计配置是否正确 2. 检查客户端版本是否兼容 3. 检查审计服务版本 4. 查看详细错误日志 |

## 12. 可观测性

发送 audit 信息的过程如果遇到失败，须在日志中详细记录失败的原因。

## 13. 安装和卸载

### 13.1 安装要求

1. 软件版本：
  - TDengine Server 版本：3.3.8+
  - taosKeeper 版本：与 TDengine Server 版本匹配
  - OpenSSL 版本：1.1.1+（HTTPS 传输需要）
1. 硬件要求：
  - 额外存储空间：根据审计保留策略和审计级别估算
  - 额外内存：审计服务需要额外内存缓冲区
  - 网络带宽：审计信息传输需要额外网络带宽
1. 权限要求：
  - 安装用户需要 root 或 sudo 权限
  - 运行用户需要访问审计目录的权限

## 14. 文档

在企业版文档中添加所有审计操作的说明，包括每种审计操作包含的详细信息。

## 15. 参考文档

[安全审计 RS](https://taosdata.feishu.cn/wiki/X7cDws2RwiEn3CkQQCGcUt8unke)

## 16. 附录

无
