---
title: TDengine SQL
description: This document describes the syntax and functions supported by TDengine SQL.
---

This section explains the syntax of SQL to perform operations on databases, tables and STables, insert data, select data and use functions. We also provide some tips that can be used in TDengine SQL. If you have previous experience with SQL this section will be fairly easy to understand. If you do not have previous experience with SQL, you'll come to appreciate the simplicity and power of SQL. TDengine SQL has been enhanced in version 3.0, and the query engine has been rearchitected. For information about how TDengine SQL has changed, see [Changes in TDengine 3.0](../taos-sql/changes).

TDengine SQL is the major interface for users to write data into or query from TDengine. It uses standard SQL syntax and includes extensions and optimizations for time-series data and services. The maximum length of a TDengine SQL statement is 1 MB. Note that keyword abbreviations are not supported. For example, DELETE cannot be entered as DEL.

Syntax Specifications used in this chapter:

- Keywords are given in uppercase, although SQL is not case-sensitive.
- Information that you input is given in lowercase.
- \[ \] means optional input, excluding [] itself.
- | means one of a few options, excluding | itself.
- ... means the item prior to it can be repeated multiple times.

To better demonstrate the syntax, usage and rules of TDengine SQL, hereinafter it's assumed that there is a data set of data from electric meters. Each meter collects 3 data measurements: current, voltage, phase. The data model is shown below:

```
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

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
