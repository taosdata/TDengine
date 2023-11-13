---
toc_max_heading_level: 4
title: "审计日志"
sidebar_label: "审计日志"
---

## 简介

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
    "db": string,
    "resource": string,
    "client_add": string,
    "details": string
}
```

## 表结构

taosKeeper 会依据上报的审计数据在相应的数据库中自动建立超级表用于存储数据。该超级表的定义如下

```sql
CREATE STABLE operations(ts timestamp, details VARCHAR(64000)， User VARCHAR(25), Operation VARCHAR(20), db VARCHAR(65), resource VARCHAR(193), client_add(25)) TAGS (clusterID VARCHAR(64) );
```

其中：
1. db为操作涉及的database，resource为操作涉及的资源。
2. User 和 Operation 为数据列，表示哪个用户在该对象上进行了什么操作
3. timestamp 为时间戳列，表示操作发生时的时间
4. details 为该操作的一些补充细节，在大多数操作下是所执行的操作的SQL语句。
5. client_add为客户端地址，包括ip和端口

## 操作列表

目前审计日志中所记录的操作列表以及每个操作中各字段的含义如下表（注：因为每个操作的实加者即 user 字段、时间戳字段和client_add在所有操作中的含义相同，下表不包含）

| 操作        | Operation | DB | Resource | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | SQL |
| alter database  | alterDB   | db name  | NULL     | SQL |
| drop database   | dropDB    | db name  | NULL     | SQL |
| create stable   | createStb | db name  | stable name | SQL |
| alter stable    | alterStb  | db name  | stable name | SQL |
| drop stable     | dropStb   | db name  | stable name | SQL |
| create user     | createUser | NULL |  被创建的用户名 | 用户属性参数,  (password除外) |
| alter user      | alterUser | NULL | 被修改的用户名 | 修改密码操作记录的是被修改的参数和新值 (password除外) ；其他操作记录SQL |
| drop user       | dropUser | NULL | 被删除的用户名 | SQL |
| create topic    | createTopic | topic 所在 DB | 创建的 topic 名字 | SQL |
| drop topic      | cropTopic | topic 所在 DB | 删除的 topic 名字 | SQL |
| create dnode    | createDnode | NULL | IP:Port 或 FQDN:Port | SQL |
| drop dnode      | dropDnode | NULL | dnodeId | SQL |
| alter dnode     | alterDnode | NULL | dnodeId | SQL |
| create mnode    | createMnode | NULL | dnodeId | SQL |
| drop mnode      | dropMnode | NULL | dnodeId | SQL |
| create qnode    | createQnode | NULL | dnodeId | SQL |
| drop qnode      | dropQnode | NULL | dnodeId | SQL |
| login           | login  | NULL | NULL | appName |
| create stream   | createStream | NULL | 所创建的 strem 名 | SQL |
| drop stream     | dropStream | NULL | 所删除的 stream 名 | SQL |
| grant privileges| grantPrivileges | NULL | 所授予的用户 | SQL | 
| remove privileges | revokePrivileges | NULL | 被收回权限的用户 | SQL | 
| compact database| compact | database name  | NULL | SQL |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | SQL |
| restore dnode | restoreDnode | NULL | dnodeId | SQL |
| restribute vgroup | restributeVgroup | NULL | vgroupId | SQL |
| balance vgroup | balanceVgroup | NULL | vgroupId | SQL |
| create table | createTable | db name | NULL | table name |
| drop table | dropTable | db name | NULL | table name |



