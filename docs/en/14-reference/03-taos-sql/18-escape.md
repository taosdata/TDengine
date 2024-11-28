---
title: Escape Characters
description: Detailed rules for using escape characters in TDengine
slug: /tdengine-reference/sql-manual/escape-characters
---

## Escape Character Table

| Character Sequence | **Represents Character** |
| :----------------: | ------------------------ |
|        `\'`       | Single quote `'`         |
|        `\"`       | Double quote `"`         |
|        `\n`       | Newline                  |
|        `\r`       | Carriage return          |
|        `\t`       | Tab                      |
|        `\\`       | Backslash `\`           |
|        `\%`       | `%` (rules below)       |
|        `\_`       | `_` (rules below)       |

## Rules for Using Escape Characters

1. When escape characters are in identifiers (database name, table name, column name, alias)
   1. Regular identifiers: Directly returns an error for invalid identifiers, as identifiers must consist of numbers, letters, and underscores, and cannot start with a number.
   2. Backtick `` identifiers: Remain unchanged, no escaping applied.
2. When escape characters are in data
   1. Encountering the defined escape characters will trigger escaping (see below for `%` and `_`); if no matching escape character is found, the escape character `\` will be ignored (and `\x` remains unchanged).
   2. For `%` and `\_`, since these characters are wildcards in `LIKE`, use `\\%` and `\\_` to represent the literal `%` and `\_` in pattern matching with `LIKE`. If `\%` or `\_` is used outside the context of `LIKE` pattern matching, they will be interpreted as the strings `\%` and `\_`, not as `%` and `\_`.
