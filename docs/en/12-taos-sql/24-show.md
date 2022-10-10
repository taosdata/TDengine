---
sidebar_label: SHOW Statement
title: SHOW Statement for Metadata
---

`SHOW` command can be used to get brief system information. To get details about metatadata, information, and status in the system, please use `select` to query the tables in database `INFORMATION_SCHEMA`. 

## SHOW APPS

```sql
SHOW APPS;
```

Shows all clients (such as applications) that connect to the cluster.

## SHOW CLUSTER

```sql
SHOW CLUSTER;
```

Shows information about the current cluster.

## SHOW CONNECTIONS

```sql
SHOW CONNECTIONS;
```

Shows information about connections to the system.

## SHOW CONSUMERS

```sql
SHOW CONSUMERS;
```

Shows information about all active consumers in the system.

## SHOW CREATE DATABASE

```sql
SHOW CREATE DATABASE db_name;
```

Shows the SQL statement used to create the specified database.

## SHOW CREATE STABLE

```sql
SHOW CREATE STABLE [db_name.]stb_name;
```

Shows the SQL statement used to create the specified supertable.

## SHOW CREATE TABLE

```sql
SHOW CREATE TABLE [db_name.]tb_name
```

Shows the SQL statement used to create the specified table. This statement can be used on supertables, standard tables, and subtables.

## SHOW DATABASES

```sql
SHOW DATABASES;
```

Shows all user-created databases.

## SHOW DNODES

```sql
SHOW DNODES;
```

Shows all dnodes in the system.

## SHOW FUNCTIONS

```sql
SHOW FUNCTIONS;
```

Shows all user-defined functions in the system.

## SHOW LICENSE

```sql
SHOW LICENSE;
SHOW GRANTS;
```

Shows information about the TDengine Enterprise Edition license.

Note: TDengine Enterprise Edition only.

## SHOW INDEXES

```sql
SHOW INDEXES FROM tbl_name [FROM db_name];
```

Shows indices that have been created.

## SHOW LOCAL VARIABLES

```sql
SHOW LOCAL VARIABLES;
```

Shows the working configuration of the client.

## SHOW MNODES

```sql
SHOW MNODES;
```

Shows information about mnodes in the system.

## SHOW QNODES

```sql
SHOW QNODES;
```

Shows information about qnodes in the system.

## SHOW SCORES

```sql
SHOW SCORES;
```

Shows information about the storage space allowed by the license.

Note: TDengine Enterprise Edition only.

## SHOW STABLES

```sql
SHOW [db_name.]STABLES [LIKE 'pattern'];
```

Shows all supertables in the current database. You can use LIKE for fuzzy matching.

## SHOW STREAMS

```sql
SHOW STREAMS;
```

Shows information about streams in the system.

## SHOW SUBSCRIPTIONS

```sql
SHOW SUBSCRIPTIONS;
```

Shows all subscriptions in the system.

## SHOW TABLES

```sql
SHOW [db_name.]TABLES [LIKE 'pattern'];
```

Shows all standard tables and subtables in the current database. You can use LIKE for fuzzy matching.

## SHOW TABLE DISTRIBUTED

```sql
SHOW TABLE DISTRIBUTED table_name;
```

Shows how table data is distributed.

## SHOW TAGS

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
```

Shows all tag information in a subtable.

## SHOW TOPICS

```sql
SHOW TOPICS;
```

Shows all topics in the current database.

## SHOW TRANSACTIONS

```sql
SHOW TRANSACTIONS;
```

Shows all running transactions in the system.

## SHOW USERS

```sql
SHOW USERS;
```

Shows information about users on the system. This includes user-created users and system-defined users.

## SHOW VARIABLES

```sql
SHOW VARIABLES;
SHOW DNODE dnode_id VARIABLES;
```

Shows the working configuration of the parameters that must be the same on each node. You can also specify a dnode to show the working configuration for that node. 

## SHOW VGROUPS

```sql
SHOW [db_name.]VGROUPS;
```

Shows information about all vgroups in the system or about the vgroups for a specified database.

## SHOW VNODES

```sql
SHOW VNODES [dnode_name];
```

Shows information about all vnodes in the system or about the vnodes for a specified dnode.
