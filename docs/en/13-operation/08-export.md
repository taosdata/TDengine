---
title: Data Export
description: This document describes how to export data from TDengine.
---

There are two ways of exporting data from a TDengine cluster:
- Using a SQL statement in TDengine CLI
- Using the `taosdump` tool

## Export Using SQL

If you want to export the data of a table or a STable, please execute the SQL statement below, in the TDengine CLI.

```sql
select * from <tb_name> >> data.csv;
```

The data of table or STable specified by `tb_name` will be exported into a file named `data.csv` in CSV format.

## Export Using taosdump

With `taosdump`, you can choose to export the data of all databases, a database, a table or a STable, you can also choose to export the data within a time range, or even only export the schema definition of a table. For the details of using `taosdump` please refer to the taosdump documentation.
