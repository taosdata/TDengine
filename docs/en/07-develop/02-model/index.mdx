---
title: Data Model
---

The data model employed by TDengine is similar to that of a relational database. You have to create databases and tables. You must design the data model based on your own business and application requirements. You should design the STable (an abbreviation for super table) schema to fit your data. This chapter will explain the big picture without getting into syntactical details.

## Create Database

The [characteristics of time-series data](https://www.taosdata.com/blog/2019/07/09/86.html) from different data collection points may be different. Characteristics include collection frequency, retention policy and others which determine how you create and configure the database. For e.g. days to keep, number of replicas, data block size, whether data updates are allowed and other configurable parameters would be determined by the characteristics of your data and your business requirements. For TDengine to operate with the best performance, we strongly recommend that you create and configure different databases for data with different characteristics. This allows you, for example, to set up different storage and retention policies. When creating a database, there are a lot of parameters that can be configured such as, the days to keep data, the number of replicas, the number of memory blocks, time precision, the minimum and maximum number of rows in each data block, whether compression is enabled, the time range of the data in single data file and so on. Below is an example of the SQL statement to create a database.

```sql
CREATE DATABASE power KEEP 365 DAYS 10 BLOCKS 6 UPDATE 1;
```

In the above SQL statement:
- a database named "power" will be created
- the data in it will be kept for 365 days, which means that data older than 365 days will be deleted automatically
- a new data file will be created every 10 days
- the number of memory blocks is 6
- data is allowed to be updated

For more details please refer to [Database](/taos-sql/database).

After creating a database, the current database in use can be switched using SQL command `USE`. For example the SQL statement below switches the current database to `power`. Without the current database specified, table name must be preceded with the corresponding database name.

```sql
USE power;
```

:::note

- Any table or STable must belong to a database. To create a table or STable, the database it belongs to must be ready.
- JOIN operations can't be performed on tables from two different databases.
- Timestamp needs to be specified when inserting rows or querying historical rows.

:::

## Create STable

In a time-series application, there may be multiple kinds of data collection points. For example, in the electrical power system there are meters, transformers, bus bars, switches, etc. For easy and efficient aggregation of multiple tables, one STable needs to be created for each kind of data collection point. For example, for the meters in [table 1](/tdinternal/arch#model_table1), the SQL statement below can be used to create the super table.

```sql
CREATE STable meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);
```

:::note
If you are using versions prior to 2.0.15, the `STable` keyword needs to be replaced with `TABLE`.

:::

Similar to creating a regular table, when creating a STable, the name and schema need to be provided. In the STable schema, the first column must always be a timestamp (like ts in the example), and the other columns (like current, voltage and phase in the example) are the data collected. The remaining columns can [contain data of type](/taos-sql/data-type/) integer, float, double, string etc. In addition, the schema for tags, like location and groupId in the example, must be provided. The tag type can be integer, float, string, etc. Tags are essentially the static properties of a data collection point. For example, properties like the location, device type, device group ID, manager ID are tags. Tags in the schema can be added, removed or updated. Please refer to [STable](/taos-sql/stable) for more details.

For each kind of data collection point, a corresponding STable must be created. There may be many STables in an application. For electrical power system, we need to create a STable respectively for meters, transformers, busbars, switches. There may be multiple kinds of data collection points on a single device, for example there may be one data collection point for electrical data like current and voltage and another data collection point for environmental data like temperature, humidity and wind direction. Multiple STables are required for these kinds of devices.

At most 4096 (or 1024 prior to version 2.1.7.0) columns are allowed in a STable. If there are more than 4096 of metrics to be collected for a data collection point, multiple STables are required. There can be multiple databases in a system, while one or more STables can exist in a database.

## Create Table

A specific table needs to be created for each data collection point. Similar to RDBMS, table name and schema are required to create a table. Additionally, one or more tags can be created for each table. To create a table, a STable needs to be used as template and the values need to be specified for the tags. For example, for the meters in [Table 1](/tdinternal/arch#model_table1), the table can be created using below SQL statement.

```sql
CREATE TABLE d1001 USING meters TAGS ("California.SanFrancisco", 2);
```

In the above SQL statement, "d1001" is the table name, "meters" is the STable name, followed by the value of tag "Location" and the value of tag "groupId", which are "California.SanFrancisco" and "2" respectively in the example. The tag values can be updated after the table is created. Please refer to [Tables](/taos-sql/table) for details.

In the TDengine system, it's recommended to create a table for a data collection point via STable. A table created via STable is called subtable in some parts of the TDengine documentation. All SQL commands applied on regular tables can be applied on subtables.

:::warning
It's not recommended to create a table in a database while using a STable from another database as template.

:::tip
It's suggested to use the globally unique ID of a data collection point as the table name. For example the device serial number could be used as a unique ID. If a unique ID doesn't exist, multiple IDs that are not globally unique can be combined to form a globally unique ID. It's not recommended to use a globally unique ID as tag value.

## Create Table Automatically

In some circumstances, it's unknown whether the table already exists when inserting rows. The table can be created automatically using the SQL statement below, and nothing will happen if the table already exists.

```sql
INSERT INTO d1001 USING meters TAGS ("California.SanFrancisco", 2) VALUES (now, 10.2, 219, 0.32);
```

In the above SQL statement, a row with value `(now, 10.2, 219, 0.32)` will be inserted into table "d1001". If table "d1001" doesn't exist, it will be created automatically using STable "meters" as template with tag value `"California.SanFrancisco", 2`.

For more details please refer to [Create Table Automatically](/taos-sql/insert#automatically-create-table-when-inserting).

## Single Column vs Multiple Column

A multiple columns data model is supported in TDengine. As long as multiple metrics are collected by the same data collection point at the same time, i.e. the timestamps are identical, these metrics can be put in a single STable as columns. 

However, there is another kind of design, i.e. single column data model in which a table is created for each metric. This means that a STable is required for each kind of metric. For example in a single column model, 3 STables would be required for current, voltage and phase.

It's recommended to use a multiple column data model as much as possible because insert and query performance is higher. In some cases, however, the collected metrics may vary frequently and so the corresponding STable schema needs to be changed frequently too. In such cases, it's more convenient to use single column data model.
