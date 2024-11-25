---
title: SQL Manual
description: 'Syntax rules supported by TDengine SQL, main query functions, supported SQL query functions, and common tips.'
slug: /tdengine-reference/sql-manual
---

This document explains the syntax rules supported by TDengine SQL, main query functionalities, supported SQL query functions, and common tips. Readers are expected to have a basic understanding of SQL language. TDengine version 3.0 has made significant improvements and optimizations compared to version 2.x, especially with a complete overhaul of the query engine, leading to many changes in SQL syntax. For detailed changes, please refer to the [Syntax Changes in TDengine 3.0](syntax-changes-in-tdengine-3/) section.

TDengine SQL is the primary tool for users to write and query data in TDengine. It provides standard SQL syntax and has optimized and added many syntaxes and features tailored for time-series data and business characteristics. The maximum length for TDengine SQL statements is 1M. TDengine SQL does not support abbreviations for keywords; for example, DELETE cannot be abbreviated to DEL.

The following conventions are used in this chapter for SQL syntax:

- Keywords are represented in uppercase, but SQL itself does not distinguish between the case of keywords and identifiers.
- User-input content is represented in lowercase letters.
- \[ \] indicates that the content is optional but should not include the brackets themselves when inputting.
- | indicates a choice among multiple options, and one can choose one of them, but the pipe character should not be included in the input.
- … indicates that the preceding item can be repeated multiple times.

To better illustrate the rules and characteristics of SQL syntax, this document assumes the existence of a dataset. Using smart meters (meters) as an example, each smart meter collects three measurements: current, voltage, and phase. The modeling is as follows:

```text
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

The dataset contains data from 4 smart meters, corresponding to 4 subtables according to TDengine's modeling rules, with names d1001, d1002, d1003, and d1004 respectively.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
