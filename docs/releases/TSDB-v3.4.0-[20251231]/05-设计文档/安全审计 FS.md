# 安全审计 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-31 | - | 0.1 | 陈东明 | 初始化 |
| 2025-11-28 | 2025-11-28 | 1.0 | 关胜亮 | 修订及发布 |

## 2. 背景

启用全面的数据库审计功能。

## 3. 定义

不涉及

## 4. 行为说明

### 4.1 分级配置参数

新增审计相关配置参数如下
1. auditLevel
   - AUDIT_LEVEL_SYSTTEM = 1, 表示系统级别
   - AUDIT_LEVEL_CLUSTER = 2, 表示集群级别
   - AUDIT_LEVEL_DATABASE = 3, 表示库级别
   - AUDIT_LEVEL_CHILTABLE = 4, 表示子表级别
   - AUDIT_LEVEL_DATA = 5, 表示数据级别
2. enableAuditSelect：是否审计 select 操作
3. enableAuditInsert：是否审计 insert 操作
参数生效说明：
1. `enableAuditSelect`、`enableAuditInsert`、`enableAuditDelete`仅在 `auditLevel = AUDIT_LEVEL_DATA`（数据级别）时生效。
2. 原参数 `auditCreateTable`功能已由 `AUDIT_LEVEL_CHILTABLE`审计级别覆盖，故予以废弃。

### 4.2 审计操作及其级别

#### 4.2.1 系统级

1. create dnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createDnode |  | n/a | sql |

1. drop dnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropDnode |  | dnodeid1 | sql |

1. alter dnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterDnode |  | dnodeId | sql |

1. create mnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createMnode |  | dnodeid1 | sql |

1. drop mnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropMnode |  | dnodeid1 | sql |

1. create qnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createQnode |  | nodeid | sql |

1. drop qnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropQnode |  | dnodeId | sql |

1. restore dnode/mnode/vnode/qnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | restoreDnode |  | nodeid | sql |

#### 4.2.2 集群级

1. Alter cluster

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterCluster | n/a | n/a | sql |

1. balance vgroup leader

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | balanceVgroupLead | n/a | n/a | sql |

1. REDISTRIBUTE VGROUP

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | RedistributeVgroup |  | vgId | sql |

1. BALANCE VGROUP

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | BalanceVgroup | n/a | n/a | sql |

1. Assign Leader

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | assignLeader | n/a | n/a | sql |

1. GRANT privileges

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | GrantPrivileges |  | targetUserName1 | sql |

objName：修改DB、table 权限时，objName 为 DBname，修改 topic 权限时，objName 为 topicName
1. REVOKE privileges

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | RevokePrivileges |  | targetUserName1 | sql |

objName：修改DB、table 权限时，objName 为 DBname，修改 topic 权限时，objName 为 topicName
1. login

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | login |  | ip:port | appName |

1. alter user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | alterUser |  | n/a | 被修改的参数和新值（password 不记新值） 例如： sysinfo '1' password xxx |

1. create user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | createUser |  | n/a | 其它参数及其值 |

1. import user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | importUser |  | n/a | 其它参数及其值 |

1. drop user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | dropUser |  | user | sql |

1. GrantPrivileges

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | GrantPrivileges | Db name | user | sql |

1. RevokePrivileges

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | RevokePrivileges | Db name | user | sql |

1. Create Mount

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | createMount | mountName |  | sql |

1. Drop Mount

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | dropMount | mountName |  | sql |

1. kill Retention

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | killRetention | Db name | id | sql |

1. auto TrimDB

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  |  | autoTrimDB | Db name |  | sql |

1. createEncryptAlgr

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  |  | createEncryptAlgr |  | algorithmId | sql |

1. dropEncryptAlgr

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  |  | dropEncryptAlgr |  | algorithmId | sql |

#### 4.2.3 库级别

1. create database 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createDB | dbname1 | n/a | sql |

1. alter database

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterDB | dbname1 | n/a | sql |

1. drop database

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropDB | dbname1 | n/a | sql |

1. Kill compact 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | compact | dbName |  | sql |

1. compact 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | compact | dbName |  | sql |

1. alterStb

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterStb | dbname1 | stablename1 | sql |

1. create stable

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createStb | dbname1 | stablename1 | sql |

1. dropStb

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropStb | dbname1 | stablename1 | sql |

1. create stream

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createStream |  | streamName | sql |

1. drop stream

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropStream |  | streamName | sql |

1. recalcStream

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | recalcStream | streamName | recalcName | sql |

1. create topic

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createTopic | DbName1 | topicName1 | sql |

1. drop topic

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropTopic |  | topicName1 | sql |

1. reload topic

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | reloadTopic |  | topicName1 | sql |

1. drop Rsma 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropRsma | Rsma name |  | sql |

1. create Rsma 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createRsma | Rsma name |  | sql |

1. alterRsma 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterRsma | Rsma name | Table name | sql |

1. createView 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createView | Db name | view name | sql |

1. dropView 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropView | Db name | view name | sql |

#### 4.2.4 子表级别

1. createTable 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createTable | Db name | table name | sql |

1. dropTable 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropTable | Db name | table name | sql |

#### 4.2.5 数据级别

1. delete 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | delete | Db name | table name | sql |

1. insert 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | insert | Db name | table name | sql |

1. select 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | select | Db name | table name | sql |

### 4.3 审计权限控制

RS 中 4.2、4.3、4.4 三个部分，在 [安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f) 中实现

### 4.4 审计信息传输

#### 4.4.1 传输协议

1. 审计信息传输机制：审计信息将统一发送至 `taoskeeper`组件进行收集与处理。
2. 传输协议：系统新增参数 `AuditHttps`用于控制传输协议
   - 设置为 `true`时，使用 HTTPS 协议传输，保障通信安全。
   - 设置为 `false`时，使用 HTTP 协议传输。
   - 该参数默认值为 `false`。
3. 实现依赖
当启用 HTTPS 时，系统将通过 cURL 的 C 语言库 发起请求。该库已存在于当前代码库中，此次实现为本地调用，不引入新的第三方依赖。

#### 4.4.2 传输接口

Http api 的地址为：
1. 单个传输：/audit_v2?db=test&token=xxxxxxxx
2. 批量传输：/audit-batch?db=test&token=xxxxxxxx
新增 db 和 token 2个url参数。
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
    "affected_rows": Integer （新增）
    "duration":Double （新增）
}
```

新增 affected_rows，duration 2个字段。

### 4.5 审计库保存时间

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] IS_AUDIT 1;

database_options:
    database_option ...

database_option: {
  DURATION value
}
```

Audit 为1 时，keep 默认 为 1825d， 如果用户指定keep，要求大于1825d。

### 4.6 强制落盘策略

```plaintext
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] IS_AUDIT 1;

database_options:
    database_option ...

database_option: {
  WAL_LEVEL value
}
```

Audit 为1 时，WAL_LEVEL 默认 为 2， 用户不能更改。

### 4.7 强制加密策略

```plaintext
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] audit 1;

database_options:
    database_option ...

database_option: {
  ENCRYPT_ALGORITHM value
}
```

Audit 为1 时，ENCRYPT_ALGORITHM 用户不能指定为None，可以选择任意一种CBC模式的对称加密算法。

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

1. 3.3.8 版本之前的审计库因不符合加密要求，需重新创建新的审计数据库 
2. 使用新版本后，不能退回到旧版本

## 8. 运维

无

## 9. 使用场景

### 9.1 记录数据库的各种操作

列表见 4.2

### 9.2 查询数据库的各种操作

## 10. 约束和限制

无

## 11. 常见错误和排查

| Failed to send out audit record | 发送 audit 记录失败，确认taoskeeper 的地址是否正确 |
| --- | --- |

## 12. 可观测性

发送 audit 信息的过程如果遇到失败，须在日志中详细记录失败的原因。

## 13. 安装和卸载

无

## 14. 文档

在企业版文档中添加所有审计操作的说明，包括每种审计操作包含的详细信息。

## 15. 参考文档

[安全审计 RS](https://taosdata.feishu.cn/wiki/X7cDws2RwiEn3CkQQCGcUt8unke)

## 16. 附录
