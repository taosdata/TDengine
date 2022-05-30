---
sidebar_label: Database
title: Database
description: "create and drop database, show or change database parameters"
---

## Create Database

```
CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep] [DAYS days] [UPDATE 1];
```

:::info

1. KEEP specifies the number of days for which the data in the database will be retained. The default value is 3650 days, i.e. 10 years. The data will be deleted automatically once its age exceeds this threshold.
2. UPDATE specifies whether the data can be updated and how the data can be updated.
   1. UPDATE set to 0 means update operation is not allowed. The update for data with an existing timestamp will be discarded silently and the original record in the database will be preserved as is.
   2. UPDATE set to 1 means the whole row will be updated. The columns for which no value is specified will be set to NULL.
   3. UPDATE set to 2 means updating a subset of columns for a row is allowed. The columns for which no value is specified will be kept unchanged.
3. The maximum length of database name is 33 bytes.
4. The maximum length of a SQL statement is 65,480 bytes.
5. Below are the parameters that can be used when creating a database
   - cache: [Description](/reference/config/#cache)
   - blocks: [Description](/reference/config/#blocks)
   - days: [Description](/reference/config/#days)
   - keep: [Description](/reference/config/#keep)
   - minRows: [Description](/reference/config/#minrows)
   - maxRows: [Description](/reference/config/#maxrows)
   - wal: [Description](/reference/config/#wallevel)
   - fsync: [Description](/reference/config/#fsync)
   - update: [Description](/reference/config/#update)
   - cacheLast: [Description](/reference/config/#cachelast)
   - replica: [Description](/reference/config/#replica)
   - quorum: [Description](/reference/config/#quorum)
   - maxVgroupsPerDb: [Description](/reference/config/#maxvgroupsperdb)
   - comp: [Description](/reference/config/#comp)
   - precision: [Description](/reference/config/#precision)
6. Please note that all of the parameters mentioned in this section are configured in configuration file `taos.cfg` on the TDengine server. If not specified in the `create database` statement, the values from taos.cfg are used by default. To override default parameters, they must be specified in the `create database` statement.
   
:::

## Show Current Configuration

```
SHOW VARIABLES;
```

## Specify The Database In Use

```
USE db_name;
```

:::note
This way is not applicable when using a REST connection. In a REST connection the database name must be specified before a table or stable name. For e.g. to query the stable "meters" in database "test" the query would be "SELECT count(*) from test.meters"

:::

## Drop Database

```
DROP DATABASE [IF EXISTS] db_name;
```

:::note
All data in the database will be deleted too. This command must be used with extreme caution. Please follow your organization's data integrity, data backup, data security or any other applicable SOPs before using this command.

:::

## Change Database Configuration

Some examples are shown below to demonstrate how to change the configuration of a database. Please note that some configuration parameters can be changed after the database is created, but some cannot. For details of the configuration parameters of database please refer to [Configuration Parameters](/reference/config/).

```
ALTER DATABASE db_name COMP 2;
```

COMP parameter specifies whether the data is compressed and how the data is compressed.

```
ALTER DATABASE db_name REPLICA 2;
```

REPLICA parameter specifies the number of replicas of the database.

```
ALTER DATABASE db_name KEEP 365;
```

KEEP parameter specifies the number of days for which the data will be kept.

```
ALTER DATABASE db_name QUORUM 2;
```

QUORUM parameter specifies the necessary number of confirmations to determine whether the data is written successfully.

```
ALTER DATABASE db_name BLOCKS 100;
```

BLOCKS parameter specifies the number of memory blocks used by each VNODE.

```
ALTER DATABASE db_name CACHELAST 0;
```

CACHELAST parameter specifies whether and how the latest data of a sub table is cached.

:::tip
The above parameters can be changed using `ALTER DATABASE` command without restarting. For more details of all configuration parameters please refer to [Configuration Parameters](/reference/config/).

:::

## Show All Databases

```
SHOW DATABASES;
```

## Show The Create Statement of A Database

```
SHOW CREATE DATABASE db_name;
```

This command is useful when migrating the data from one TDengine cluster to another. This command can be used to get the CREATE statement, which can be used in another TDengine instance to create the exact same database.
