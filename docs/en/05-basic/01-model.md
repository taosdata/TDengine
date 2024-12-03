---
sidebar_label: Data Model
title: Understand the TDengine Data Model
slug: /basic-features/data-model
---

import Image from '@theme/IdealImage';
import dataModel from '../assets/data-model-01.png';

This document describes the data model and provides definitions of terms and concepts used in TDengine.

The TDengine data model is illustrated in the following figure.

<figure>
<Image img={dataModel} alt="Data Model Diagram"/>
<figcaption>Figure 1. The TDengine data model</figcaption>
</figure>

## Terminology

### Metric

A metric is a measurement obtained from a data collection point. With smart meters, for example, current, voltage, and phase are typical metrics.

### Tag

A tag is a static attribute associated with a data collection point and that does not typically change over time, such as device model, color, or location. With smart meters, for example, location and group ID are typical tags.

### Data Collection Point

A data collection point (DCP) is a hardware or software device responsible for collecting metrics at a predetermined time interval or upon a specific event trigger. A DCP can collect one or more metrics simultaneously, but all metrics from each DCP share the same timestamp.

Complex devices often have multiple DCPs, each with its own collection cycle, operating independently of each other. For example, in a car, one DCP may collect GPS data while a second monitors the engine status and a third monitors the interior environment.

### Table

A table in TDengine consists of rows of data and columns defining the type of data, like in the traditional relational database model. However, TDengine stores the data from each DCP in a separate table. This is known as the "one table per DCP" model and is a unique feature of TDengine.

Note that for complex devices like cars, which have multiple DCPs, this means that multiple tables are created for a single device.

Typically, the name of a DCP is stored as the name of the table, not as a separate tag. The `tbname` pseudocolumn is used for filtering by table name. Each metric collected by and tag associated with the DCP is represented as a column in the table, and each column has a defined data type. The first column in the table must be the timestamp, which is used to build an index.

TDengine includes two types of tables: subtables, which are created within a supertable, and basic tables, which are independent of supertables.

### Basic Table

A basic table cannot contain tags and does not belong to any supertable. The functionality of a basic table is similar to that of a table in a relational database management system.

### Supertable

A supertable is a data structure that groups together a specific type of DCP into a logical unified table. The tables created within a supertable are known as subtables. All subtables within a supertable have the same schema. The supertable is a unique concept in TDengine that simplifies table management and facilitates aggregation across DCPs.

Each supertable contains at least one timestamp column, at least one metric column, and at least one tag column. Tags in a supertable can be added, modified, or deleted at any time without creating new time series. Note that data is not stored within a supertable, but within the subtables in that supertable.

With smart meters, for example, one supertable would be created for all smart meters. Within that supertable, one subtable is created for each smart meter. For more information, see [TDengine Concepts: Supertable](https://tdengine.com/tdengine-concepts-supertable/).

### Subtable

A subtable is created within a supertable and inherits the schema of the supertable. The schema of a subtable cannot be modified; any modifications to the supertable schema affect all subtables that it contains. 

### Database

A database defines storage policies for its supertables and tables. Each database can contain one or more supertables, but each supertable or table belongs to only one database.

A single TDengine deployment can contain multiple databases with different policies. You can create multiple databases to achieve finer-grained data management and optimization.

### Timestamp

Timestamps are a complex but essential part of time-series data management. TDengine stores timestamps in Unix time, which represents the number of milliseconds elapsed since the Unix epoch of January 1, 1970 at 00:00 UTC. However, when an application queries data in TDengine, the TDengine client automatically converts the timestamp to the local time zone of the application.

- When TDengine ingests a timestamp in RFC 3339 format, for example `2018-10-03T14:38:05.000+08:00`, the time zone specified in the timestamp is used to convert the timestamp to Unix time.
- When TDengine ingests a timestamp that does not contain time zone information, the local time zone of the application is used to convert the timestamp to Unix time.

## Sample Data

In this documentation, a smart meters scenario is used as sample data. The smart meters in this scenario collect three metrics, current, voltage, and phase; and two tags, location and group ID. The device ID of each smart meter is used as its table name.

An example of data collected by these smart meters is shown in the following table.

| Device ID |   Timestamp   | Current | Voltage | Phase |        Location         | Group ID |
| :-------: | :-----------: | :-----: | :-----: | :---: | :---------------------: | :------: |
|   d1001   | 1538548685000 |  10.3   |   219   | 0.31  | California.SanFrancisco |    2     |
|   d1002   | 1538548684000 |  10.2   |   220   | 0.23  | California.SanFrancisco |    3     |
|   d1003   | 1538548686500 |  11.5   |   221   | 0.35  |  California.LosAngeles  |    3     |
|   d1004   | 1538548685500 |  13.4   |   223   | 0.29  |  California.LosAngeles  |    2     |
|   d1001   | 1538548695000 |  12.6   |   218   | 0.33  | California.SanFrancisco |    2     |
|   d1004   | 1538548696600 |  11.8   |   221   | 0.28  |  California.LosAngeles  |    2     |
|   d1002   | 1538548696650 |  10.3   |   218   | 0.25  | California.SanFrancisco |    3     |
|   d1001   | 1538548696800 |  12.3   |   221   | 0.31  | California.SanFrancisco |    2     |

## Data Management

This section describes how to create databases, supertables, and tables to store your data in TDengine.

### Create a Database

You use the `CREATE DATABASE` statement to create a database:

```sql
CREATE DATABASE power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;
```

The name of the database created is `power` and its parameters are explained as follows:

- `PRECISION 'ms'`: The time-series data in this database uses millisecond-precision timestamps.
- `KEEP 3650`: The data in this database is retained for 3650 days. Any data older than 3650 days is automatically deleted.
- `DURATION 10`: Each data file contains 10 days of data.
- `BUFFER 16`: A 16 MB memory buffer is used for data ingestion.

For a list of all database parameters, see [Manage Databases](../../tdengine-reference/sql-manual/manage-databases/).

You use the `USE` statement to set a current database:

```sql
USE power;
```

This SQL statement switches the current database to `power`, meaning that subsequent statements are performed within the `power` database.

### Create a Supertable

You use the `CREATE STABLE` statement to create a supertable.

```sql
CREATE STABLE meters (
    ts TIMESTAMP, 
    current FLOAT, 
    voltage INT, 
    phase FLOAT
) TAGS (
    location VARCHAR(64), 
    group_id INT
);
```

The name of the supertable created is `meters` and the parameters following the name define the columns in the supertable. Each column is defined as a name and a data type. The first group of columns are metrics and the second group, following the `TAGS` keyword, are tags.

:::note

- The first metric column must be of type `TIMESTAMP`.
- Metric columns and tag columns cannot have the same name.

:::

### Create a Subtable

You use the `CREATE TABLE` statement with the `USING` keyword to create a subtable:

```sql
CREATE TABLE d1001 
USING meters (
    location,
    group_id
) TAGS (
    "California.SanFrancisco", 
    2
);
```

The name of the subtable created is `d1001` and it is created within the `meters` supertable. The `location` and `group_id` tag columns are used in this subtable, and their values are set to `California.SanFrancisco` and `2`, respectively.

Note that when creating a subtable, you can specify values for all or a subset of tag columns in the target supertable. However, these tag columns must already exist within the supertable.

### Create a Basic Table

You use the `CREATE TABLE` statement to create a basic table.

```sql
CREATE TABLE d1003(
    ts TIMESTAMP,
    current FLOAT, 
    voltage INT, 
    phase FLOAT,
    location VARCHAR(64),
    group_id INT
);
```

The name of the basic table is `d1003` and it includes the columns `ts`, `current`, `voltage`, `phase`, `location`, and `group_id`. Note that this table is not associated with any supertable and its metric and tag columns are not separate.

## Multi-Column Model vs. Single-Column Model

Typically, each supertable in TDengine contains multiple columns, one for each metric and one for each tag. However, in certain scenarios, it can be preferable to create supertables that contain only one column.

For example, when the types of metrics collected by a DCP frequently change, the standard multi-column model would require frequent modifications to the schema of the supertable. In this situation, creating one supertable per metric may offer improved performance.
