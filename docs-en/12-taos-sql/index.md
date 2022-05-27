---
title: TDengine SQL
description: "The syntax supported by TDengine SQL "
---

This section explains the syntax of SQL to perform operations on databases, tables and STables, insert data, select data and use functions. We also provide some tips that can be used in TDengine SQL. If you have previous experience with SQL this section will be fairly easy to understand. If you do not have previous experience with SQL, you'll come to appreciate the simplicity and power of SQL.

TDengine SQL is the major interface for users to write data into or query from TDengine. For ease of use, the syntax is similar to that of standard SQL. However, please note that TDengine SQL is not standard SQL. For instance, TDengine doesn't provide a delete function for time series data and so corresponding statements are not provided in TDengine SQL.

TDengine SQL doesn't support abbreviation for keywords. For example "SELECT" cannot be abbreviated as "SEL".

Syntax Specifications used in this chapter:

- The content inside <\> needs to be input by the user, excluding <\> itself.
- \[ \] means optional input, excluding [] itself.
- | means one of a few options, excluding | itself.
- … means the item prior to it can be repeated multiple times.

To better demonstrate the syntax, usage and rules of TAOS SQL, hereinafter it's assumed that there is a data set of data from electric meters. Each meter collects 3 data measurements: current, voltage, phase. The data model is shown below:

```sql
taos> DESCRIBE meters;
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```

The data set includes the data collected by 4 meters, the corresponding table name is d1001, d1002, d1003 and d1004 based on the data model of TDengine.
