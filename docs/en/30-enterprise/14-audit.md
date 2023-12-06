---
toc_max_heading_level: 4
title: Auditing
sidebar_label: Auditing
---

## Introduction

You can enable user behavior auditing in TDengine to record all significant changes to TDengine metadata. Auditing requires the taosKeeper and taosAdapter components in addition to the TDengine Server. The mnode in your TDengine cluster generates logs for operations involving metadata and sends these logs to taosKeeper. taosKeeper then stores the audit logs in another TDengine cluster via taosAdapter.

## Configuration
1. TDengine Server configuration
- audit (boolean): Specify whether to enable auditing. The default value is false.
- monitorFqdn (string): Specify the FQDN of your taosKeeper instance.
- monitorPort (int32): Specify the port of your taosKeeper instance.
- monitorCompaction (boolean): Specify whether to compress audit log data.

2. taosKeeper configuration
- auditDB: Specify the database to contain audit log data. The default value is `audit`. taosKeeper will create the specified database if it does not exist.

## Data Format

Audit log data is in the following format:

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

## Table Schema

taosKeeper automatically creates a supertable in the specified database to store audit log data. This supertable is created as follows:

```sql
CREATE STABLE operations(ts timestamp, details VARCHAR(64000), User VARCHAR(25), Operation VARCHAR(20), db VARCHAR(65)，resource VARCHAR(193), client_add(25)) TAGS (clusterID VARCHAR(64) );
```

The parameters are:
1. The `db` records the database name, the `resource` records the resouce which is operated.
2. The `User` and `Operation` columns record the user who performed the operation and the operation that was performed, respectively.
3. The `timestamp` column records the time at which the operation was performed.
4. The `details` column records additional information about the operation.
5. The `client_add` records the client ip and port.

## Operation List

The operations that are recorded in TDengine audit logs are described in the following table. Note that the `user` , `timestamp` and `client_add` columns are not shown in this example.

| Statement        | Operation | Target1 | Target 2 | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | SQL |
| alter database  | alterDB   | db name  | NULL     | SQL |
| drop database   | dropDB    | db name  | NULL     | SQL |
| create stable   | createStb | db name  | stable name | SQL |
| alter stable    | alterStb  | db name  | stable name | SQL |
| drop stable     | dropStb   | db name  | stable name | SQL |
| create user     | createUser | NULL |  User name | User parameters (except the user's password) |
| alter user      | alterUser | NULL | User name | Any parameters modified or added (except the user's password) if operation is password, else SQL|
| drop user       | dropUser | NULL | User name | SQL |
| create topic    | createTopic | Database in which the topic was created | Topic name | SQL |
| drop topic      | cropTopic | Database in which the topic was created | Topic name | Topic name | SQL |
| create dnode    | createDnode | NULL | IP:Port or FQDN:Port | SQL |
| drop dnode      | dropDnode | NULL | dnodeId | SQL |
| alter dnode     | alterDnode | NULL | dnodeId | SQL |
| create mnode    | createMnode | NULL | dnodeId | SQL |
| drop mnode      | dropMnode | NULL | dnodeId | SQL |
| create qnode    | createQnode | NULL | IP:Port or FQDN:Port | SQL |
| drop qnode      | dropQnode | NULL | dnodeId | SQL |
| login           | login  | NULL | NULL | appName |
| create stream   | createStream | NULL | Stream name | SQL |
| drop stream     | dropStream | NULL | Stream name | SQL |
| grant privileges| grantPrivileges | NULL | User name | SQL | 
| remove privileges | revokePrivileges | NULL | User name | SQL | 
| compact database| compact | database name  | NULL | SQL |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | SQL |
| restore dnode | restoreDnode | NULL | dnodeId | SQL |
| restribute vgroup | restributeVgroup | NULL | vgroupId | SQL |
| balance vgroup | balanceVgroup | NULL | vgroupId | SQL |



