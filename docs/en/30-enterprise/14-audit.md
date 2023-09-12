---
toc_max_heading_level: 4
title: "审计日志"
sidebar_label: "审计日志"
---

## Introduction

从 TDengine 3.1.1.0 开始，TDengine 企业版提供审计日志的功能，用于帮助系统管理员记录在整个数据库系统中曾经发生的重要的元数据变更操作。 TDengine 审计日志功能需要由 taosd, taosKeeper, taosAdapter 等几个组件协同才能生效。具体的数据流是 TDengine 集群中的 mnode 对涉及元数据变更的操作会将相应的审计日志发送到 taosKeeper ，taosKeeper 再通过 taosAdapter 将审计日志写入到相同或不同的 TDengine 集群中。

## 配置
1. taosd 的配置 (taos.cfg)
- audit（boolean）：是否打开审计日志功能，类型为 bool，默认值为 false，其为 3.1.1.0 版本中新增的配置参数
- monitorFqdn（string）：taosKeeper 所在的 （现有参数）
- monitorPort（int32）：taoskeeper 所监听的端口（现有参数）
- monitorCompaction (bool）：是否压缩上报数据  （现有参数）

2. taosKeeper 的配置
- auditDB: 用于存放审计日志的数据库的名字，默认值为 "audit" ，taosKeeper 在收到上报的审计日志后会判断该数据库是否存在，如果不存在会自动创建它

## 数据格式

上报的审计日志格式如下

```json
{
    "ts": timestamp,
    "cluster_id": string,
    "user": string,
    "operation": string,
    "target1": string,
    "target2": string
    "details": string
}
```

## 表结构

taosKeeper 会依据上报的审计数据在相应的数据库中自动建立超级表用于存储数据。该超级表的定义如下

```sql
CREATE STABLE operations(ts timestamp, details VARCHAR(1000)， User VARCHAR(25), Operation VARCHAR(20)) TAGS (target1 VARCHAR(100)，target2 VARCHAR(300), clusterID VARCHAR(64) );
```

The parameters are:
1. target1 和 target2 为标签，用于表示所操作的对象。比如 target1 = "db1", target2 = "stableA"。在不同场景下 target1 和 target2 所代表的含义不同，也可能 target2 为空。
2. User 和 Operation 为数据列，表示哪个用户在该对象上进行了什么操作
3. timestamp 为时间戳列，表示操作发生时的时间
4. details 为该操作的一些补充细节

## 操作列表

目前审计日志中所记录的操作列表以及每个操作中各字段的含义如下表（注：因为每个操作的实加者即 user 字段和时间戳字段在所有操作中的含义相同，下表不包含）

| 操作        | Operation | Target1 | Target 2 | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | 建库参数 |
| alter database  | alterDB   | db name  | NULL     | 被修改的参数和新值 |
| drop database   | dropDB    | db name  | NULL     | NULL |
| create stable   | createStb | db name  | stable name | 超级表 schema |
| alter stable    | alterStb  | db name  | stable name | SQL 语句 |
| drop stable     | dropStb   | db name  | stable name | NULL |
| create user     | createUser | 被创建的用户名 |  NULL | 用户属性参数 |
| alter user      | alterUser | 被修改的用户名 | NULL | 被修改的参数和新值 (password) 除外 |
| drop user       | dropUser | 被删除的用户名 | NULL | NULL |
| create topic    | createTopic | 创建的 topic 名字 | topic 所在 DB | 创建 topic 的参数 |
| drop topic      | cropTopic | 删除的 topic 名字 | NULL | NULL |
| create dnode    | createDnode | IP:Port 或 FQDN:Port | NULL | NULL |
| drop dnode      | dropDnode | IP:Port 或 FQDN:Port | dnodeId | NULL |
| alter dnode     | alterDnode | IP:Port 或 FQDN:Port | dnodeId | 修改的参数和新值 |
| create mnode    | createMnode | IP:Port 或 FQDN:Port | dnodeId | NULL |
| drop mnode      | dropMnode | IP:Port 或 FQDN:Port | dnodeId | NULL |
| create qnode    | createQnode | IP:Port 或 FQDN:Port | NULL | NULL |
| drop qnode      | dropQnode | IP:Port 或 FQDN:Port | NULL | NULL |
| login           | login  | appName | 客户端所在 ip:port | NULL |
| create stream   | createStream | 所创建的 strem 名 | NULL | 创建 stream 时的参数 |
| drop stream     | dropStream | 所删除的 stream 名 | NULL | NULL |
| grant privileges| grantPrivileges | 所授予的用户 | 所授权的对象 | 授权的权限 | 
| remove privileges | revokePrivileges | 被收回权限的用户 | 被收回权限的对象 | 所收回的权限 | 
| compact database| compact | database name  | NULL | time range (如果指定了的话) |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | NULL |
| restore dnode | restoreDnode | dnodeId | NULL | 具体参数 |
| restribute vgroup | restributeVgroup | vgroupId | NULL | NULL |
| balance vgroup | balanceVgroup | vgroupId | NULL | NULL |



