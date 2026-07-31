---
sidebar_label: TDengine SQL
title: TDengine SQL
description: TDengine SQL syntax, data types, write, query, functions, and common limits
---

import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

This document describes the syntax rules, data types, data definition, data writing, data querying, functions, and common limits supported by TDengine SQL. Readers are expected to have a basic understanding of SQL. If you are migrating from 2.x to 3.x, see [3.0 Syntax Changes](./11-appendix/04-changes.md) for deprecated syntax and replacements.

TDengine SQL is the primary tool for writing and querying data in TDengine. It is based on standard SQL and extends many syntax features and functions for time-series data and business needs. The default maximum length of a TDengine SQL statement is 4 MB and can be configured with the client parameter `maxSQLLength`, with a range of 1 MB to 64 MB. TDengine SQL does not support abbreviations of keywords; for example, `DELETE` cannot be abbreviated as `DEL`.

This section follows the conventions below for SQL syntax:

- Uppercase letters represent keywords, but SQL itself does not distinguish case for keywords or identifiers
- Lowercase letters indicate content that needs to be entered by the user
- Square brackets `[ ]` indicate optional content, but you cannot enter `[]` itself
- `|` indicates a choice among multiple options; choose one, but you cannot enter `|` itself
- `...` indicates that the previous item can be repeated multiple times

To better illustrate the rules and characteristics of SQL syntax, this document assumes a dataset. Taking smart meters as an example, assume each smart meter collects three quantities: current, voltage, and phase. Its modeling is as follows:

```sql
taos> DESCRIBE meters;
  Field    | Type      | Length | Note |
=========================================
  ts       | TIMESTAMP |      8 |      |
  current  | FLOAT     |      4 |      |
  voltage  | INT       |      4 |      |
  phase    | FLOAT     |      4 |      |
  location | BINARY    |     64 | TAG  |
  groupid  | INT       |      4 | TAG  |
```

The dataset includes data from 4 smart meters. According to TDengine modeling rules, they correspond to 4 child tables named `d1001`, `d1002`, `d1003`, and `d1004`. Examples in this chapter use `groupid` as the grouping tag name.

<DocCardList items={useCurrentSidebarCategory().items}/>
