---
title: TAOS SQL
description: "The syntax, select, functions and tips supported by TAOS SQL "
---

This document explains the syntax, select, functions and some tips that can be used in TAOS SQL. It would be easier to understand with some fundamental knowledge of SQL.

TAOS SQL is the major interface for users to write data into or query from TDengine. For users to easily use, syntax similar to standard SQL is provided. However, please be noted that TAOS SQL is not standard SQL. Besides, because TDengine doesn't provide the functionality of deleting time series data, corresponding statements are not provided in TAOS SQL.

TAOS SQL doesn't support abbreviation for keywords, for example `DESCRIBE` can't be abbreviated as `DESC`.

Syntax Specifications used in this chapter:

- The content inside <\> needs to be input by the user, excluding <\> itself.
- \[ \] means optional input, excluding [] itself.
- | means one of a few options, excluding | itself.
- … means the item prior to it can be repeated multiple times.

To better demonstrate the syntax, usage and rules of TAOS SQL, hereinafter it's assumed that there is a data set of meters. Assuming each meter collects 3 data: current, voltage, phase. The data model is as below:

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

The data set includes the data collected by 4 meters, the corresponding table name is d1001, d1002, d1003, d1004 respectively based on the data model of TDengine.
