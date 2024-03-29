---
title: Escape Characters
description: This document describes the usage of escape characters in TDengine.
---

## Escape Characters

| Escape Character | **Actual Meaning**       |
| :--------------: | ------------------------ |
|       `\'`       | Single quote `'`           |
|       `\"`       | Double quote `"`           |
|       `\n`        | Line Break               |
|       `\r`        | Carriage Return          |
|       `\t`        | tab                      |
|       `\\`       | Back Slash `\ `             |
|       `\%`       | `%` see below for details  |
|       `\_`       | `_` see below for details |

## Restrictions

1. If there are escape characters in identifiers (database name, table name, column name)
   - Identifier without ``: Error will be returned because identifier must be constituted of digits, ASCII characters or underscore and can't be started with digits
   - Identifier quoted with ``: Original content is kept, no escaping
2. If there are escape characters in values
   - The escape characters will be escaped as the above table. If the escape character doesn't match any supported one, the escape character `\ ` will be ignored(`\x` remaining).
   - `%` and `_` are used as wildcards in `like`. `\%` and `\_` should be used to represent literal `%` and `_` in `like`. If `\%` and `\_` are used out of `like` context, the evaluation result is `\%` and `\_`, instead of `%` and `_`.
