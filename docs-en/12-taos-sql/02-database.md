---
sidebar_label: Database
title: Database
description: "create and drop database, show or change database parameters"
---

## Create Datable

```
CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep] [DAYS days] [UPDATE 1];
```

:::info

1. KEEP specifies the number of days for which the data in the database to be created will be kept, the default value is 3650 days, i.e. 10 years. The data will be deleted automatically once its age exceeds this threshold.
2. UPDATE specifies whether the data can be updated and how the data can be updated.
   1. UPDATE set to 0 means update operation is not allowed, the data with an existing timestamp will be dropped silently.
   2. UPDATE set to 1 means the whole row will be updated, the columns for which no value is specified will be set to NULL
   3. UPDATE set to 2 means updating a part of columns for a row is allowed, the columns for which no value is specified will be kept as no change
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
6. Please note that all of the parameters mentioned in this section can be configured in configuration file `taosd.cfg` at server side and used by default, the default parameters can be overriden if they are specified in `create database` statement.
   
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
This way is not applicable when using a REST connection

:::

## Drop Database

```
DROP DATABASE [IF EXISTS] db_name;
```

:::note
All data in the database will be deleted too. This command must be used with caution.

:::

## Change Database Configuration

Some examples are shown below to demonstrate how to change the configuration of a database. Please note that some configuration parameters can be changed after the database is created, but some others can't, for details of the configuration parameters of database please refer to [Configuration Parameters](/reference/config/).

```
ALTER DATABASE db_name COMP 2;
```

COMP parameter specifies whether the data is compressed and how the data is compressed.

```
ALTER DATABASE db_name REPLICA 2;
```

REPLICA parameter specifies the number of replications of the database.

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

This command is useful when migrating the data from one TDengine cluster to another one. This command can be used to get the CREATE statement, which can be used in another TDengine to create the exact same database.
