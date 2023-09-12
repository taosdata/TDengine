---
toc_max_heading_level: 4
title: "Audit Log"
sidebar_label: "Audit Log"
---

## Introduction

From TDengine 3.1.1.0, TDengine Enterprise provides the functionality of audit log to help system administrator to record the imporment metadata change operations in the database system. This functionality can be activated with combining taosd, taosKeeper, taosAdapter components. The data stream is that `mnode` in TDengine cluster sends audit log to taosKeeper for the operations that involve with metadata change, taosKeeper wirtes the audit log to the same or another TDengine cluster through taosAdapter. 

## Configuration
1. taosd configuration (taos.cfg file)
- audit（boolean）：Enable audit log or not, the type is bool, the default value is false.  It's available after 3.1.0.0 (including)
- monitorFqdn（string）：The FQDN where taosKeeper is running
- monitorPort（int32）：The Port taoskeeper is listening to
- monitorCompaction (bool): Compact the data sent to toasKeeper or not

2. taosKeeper configuration
- auditDB: the database name for stroing audit log data. The default value is "audit", taosKeeper will create it if it doesn't exist. 

## Data Format

The format of audit log is as follows:

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

## Table Schema

taosKeeper will automatically create super table in the specified database to store the audit log data, the schema of the super table is as follows: \

```sql
CREATE STABLE operations(ts timestamp, details VARCHAR(1000)， User VARCHAR(25), Operation VARCHAR(20)) TAGS (target1 VARCHAR(100)，target2 VARCHAR(300), clusterID VARCHAR(64) );
```

The parameters are:
1. target1 and target2 are tags, to identify the object being operated. For example target1 = "db1", target2 = "stableA". In diffrent operations, the meaning of target1 and target2 may be different from the above example, and target2 may be NULL in some cases.。
2. User and Operation are data columns, to identify who did what on the object. 
3. timestamp is the primary key, identfies the time when the operation is performed.
4. details provides some additional details.

## User Operations

The table below summarizes the operations that are supported in audit log, and the meaning of all tags and columns for these operations. (Note: user and timestamp are skipped since their meaning are exactly same in all operations.)

| User Operation  | Operation | Target1 | Target 2 | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | parameters for creating databases |
| alter database  | alterDB   | db name  | NULL     | parameters to be modified and their new values |
| drop database   | dropDB    | db name  | NULL     | NULL |
| create stable   | createStb | db name  | stable name | super table schema |
| alter stable    | alterStb  | db name  | stable name | SQL statement |
| drop stable     | dropStb   | db name  | stable name | NULL |
| create user     | createUser | user name being created |  NULL | user attributes |
| alter user      | alterUser | user name being altered | NULL | attributes being altered and their new value (except for password) |
| drop user       | dropUser | user name being deleted | NULL | NULL |
| create topic    | createTopic | topic being created | the database the topic belongs to | topic parameters |
| drop topic      | cropTopic | topic being deleted | NULL | NULL |
| create dnode    | createDnode | IP:Port or FQDN:Port | NULL | NULL |
| drop dnode      | dropDnode | IP:Port or FQDN:Port | dnodeId | NULL |
| alter dnode     | alterDnode | IP:Port or FQDN:Port | dnodeId | parameters being altered and their new values |
| create mnode    | createMnode | IP:Port or FQDN:Port | dnodeId | NULL |
| drop mnode      | dropMnode | IP:Port or FQDN:Port | dnodeId | NULL |
| create qnode    | createQnode | IP:Port or FQDN:Port | NULL | NULL |
| drop qnode      | dropQnode | IP:Port or FQDN:Port | NULL | NULL |
| login           | login  | appName | the ip:port of client | NULL |
| create stream   | createStream | stream name being created | NULL | stream parameters |
| drop stream     | dropStream | stream being deleted | NULL | NULL |
| grant privileges| grantPrivileges | the user being granted with privileges | the object being granted | the privileges being granted | 
| remove privileges | revokePrivileges | the user whose privileges being revoked | the affected object | the revoked privileges | 
| compact database| compact | database name  | NULL | time range (if specified) |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | NULL |
| restore dnode | restoreDnode | dnodeId | NULL | parameters |
| restribute vgroup | restributeVgroup | vgroupId | NULL | NULL |
| balance vgroup | balanceVgroup | vgroupId | NULL | NULL |



