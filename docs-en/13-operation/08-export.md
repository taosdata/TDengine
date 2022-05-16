---
title: Data Export
---

There are two ways of exporting data from a TDengine cluster, one is SQL statement in TDengine CLI, the other one is `taosdump`.

## Export Using SQL

If you want to export the data of a table or a STable, please execute below SQL statement in TDengine CLI.

```sql
select * from <tb_name> >> data.csv;
```

The data of table or STable specified by `tb_name` will be exported into a file named `data.csv` in CSV format.

## Export Using taosdump

With `taosdump`, you can choose to export the data of all databases, a database, a table or a STable, you can also choose export the data within a time range, or even only export the schema definition of a table. For the details of using `taosdump` please refer to [Tool for exporting and importing data: taosdump](/reference/taosdump).
