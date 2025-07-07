---
title: SQL Manual
slug: /tdengine-reference/sql-manual
---

This document explains the syntax rules, main query functions, supported SQL query functions, and common techniques supported by TDengine SQL. Readers are expected to have a basic understanding of SQL language. TDengine version 3.0 has made significant improvements and optimizations compared to version 2.x, especially with a complete overhaul of the query engine, thus there are many changes in SQL syntax compared to version 2.x. For detailed changes, please see the [3.0 Syntax Changes](./syntax-changes/) section.

TDengine SQL is the primary tool for users to write data and perform queries on TDengine. TDengine SQL provides standard SQL syntax and has optimized and added many syntax features and functions specific to time-series data and business needs. The maximum length of a TDengine SQL statement is 1M. TDengine SQL does not support abbreviations of keywords, for example, DELETE cannot be abbreviated as DEL.

This section follows the conventions below for SQL syntax:

- Keywords are represented in uppercase letters, but SQL itself does not distinguish between the case of keywords and identifiers
- Lowercase letters indicate content that needs to be entered by the user
- \[ \] indicates optional content, but you cannot enter [] itself
- | indicates a choice among multiple options, choose one, but you cannot enter | itself
- ... indicates that the previous item can be repeated multiple times

To better illustrate the rules and characteristics of SQL syntax, this document assumes the existence of a dataset. Taking smart meters as an example, assume each smart meter collects three quantities: current, voltage, and phase. Its modeling is as follows:

```text
taos> DESCRIBE meters;
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | tag        |
 groupid                        | INT                |           4 | tag        |
```

The dataset includes data from 4 smart meters, according to TDengine's modeling rules, corresponding to 4 subtables, named d1001, d1002, d1003, d1004.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
