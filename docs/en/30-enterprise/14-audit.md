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
    "target1": string,
    "target2": string
    "details": string
}
```

## Table Schema

taosKeeper automatically creates a supertable in the specified database to store audit log data. This supertable is created as follows:

```sql
CREATE STABLE operations(ts timestamp, details VARCHAR(1000)， User VARCHAR(25), Operation VARCHAR(20)) TAGS (target1 VARCHAR(100)，target2 VARCHAR(300), clusterID VARCHAR(64) );
```

The parameters are:
1. The `target1` and `target2` tags record the object on which the operation was performed. For example, `target1` = `db1`, `target2` = `stableA`. In some cases, these values may represent different objects. The value of `target2` may be empty.
2. The `User` and `Operation` columns record the user who performed the operation and the operation that was performed, respectively.
3. The `timestamp` column records the time at which the operation was performed.
4. The `details` column records additional information about the operation.

## Operation List

The operations that are recorded in TDengine audit logs are described in the following table. Note that the `user` and `timestamp` columns are not shown in this example.

| Statement        | Operation | Target1 | Target 2 | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | Configuration of the database |
| alter database  | alterDB   | db name  | NULL     | Any parameters modified or added |
| drop database   | dropDB    | db name  | NULL     | NULL |
| create stable   | createStb | db name  | stable name | Schema of the supertable |
| alter stable    | alterStb  | db name  | stable name | SQL statement executed |
| drop stable     | dropStb   | db name  | stable name | NULL |
| create user     | createUser | User name |  NULL | User parameters |
| alter user      | alterUser | User name | NULL | Any parameters modified or added (except the user's password) |
| drop user       | dropUser | User name | NULL | NULL |
| create topic    | createTopic | Topic name | Database in which the topic was created | Topic parameters |
| drop topic      | cropTopic | Topic name | NULL | NULL |
| create dnode    | createDnode | IP:Port or FQDN:Port | NULL | NULL |
| drop dnode      | dropDnode | IP:Port or FQDN:Port | dnodeId | NULL |
| alter dnode     | alterDnode | IP:Port or FQDN:Port | dnodeId | Any parameters modified or added |
| create mnode    | createMnode | IP:Port or FQDN:Port | dnodeId | NULL |
| drop mnode      | dropMnode | IP:Port or FQDN:Port | dnodeId | NULL |
| create qnode    | createQnode | IP:Port or FQDN:Port | NULL | NULL |
| drop qnode      | dropQnode | IP:Port or FQDN:Port | NULL | NULL |
| login           | login  | appName | IP address and port of the client | NULL |
| create stream   | createStream | Stream name | NULL | Stream parameters |
| drop stream     | dropStream | Stream name | NULL | NULL |
| grant privileges| grantPrivileges | User name | Object to which privileges were granted | Privileges granted | 
| remove privileges | revokePrivileges | User name | Object from which privileges were revoked | Revoked privileges | 
| compact database| compact | database name  | NULL | time range (if specified) |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | NULL |
| restore dnode | restoreDnode | dnodeId | NULL | Parameters |
| restribute vgroup | restributeVgroup | vgroupId | NULL | NULL |
| balance vgroup | balanceVgroup | vgroupId | NULL | NULL |



