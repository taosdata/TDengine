---
title: Escape Characters
slug: /tdengine-reference/sql-manual/escape-characters
---

## Escape Character Table

| Character Sequence | **Represents Character** |
| :----------------: | ------------------------ |
|       `\'`         | Single quote `'`         |
|       `\"`         | Double quote `"`         |
|       `\n`         | Newline character        |
|       `\r`         | Carriage return          |
|       `\t`         | Tab character            |
|       `\\`         | Backslash `\`            |
|       `\%`         | `%` see below for rules  |
|       `\_`         | `_` see below for rules  |

## Rules for Using Escape Characters

1. Identifiers contain escape characters (database names, table names, column names, aliases)
   1. Regular identifiers: Directly prompt an error for the identifier, because identifiers are required to be numbers, letters, and underscores, and cannot start with a number.
   2. Backtick `` identifiers: Keep as is, do not escape
2. Data contains escape characters
   1. Encountering the defined escape characters will trigger escaping (`%` and `_` see below), if there is no matching escape character, the escape symbol `\` will be ignored (`\x` remains as is).
   2. For `%` and `_`, since these two characters are wildcards in `like`, use `\%` and `\_` in pattern matching `like` to represent the characters `%` and `_` themselves. If `\%` or `\_` are used outside the `like` pattern matching context, their results are the strings `\%` and `\_`, not `%` and `_`.
