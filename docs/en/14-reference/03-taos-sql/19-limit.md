---
sidebar_label: Names & Limits
title: Naming Conventions and Limitations
description: Legal character set and naming restrictions
slug: /tdengine-reference/sql-manual/names
---

## Naming Rules

1. Legal characters: English letters, numbers, and underscores.
2. Names may start with English letters or underscores, but cannot start with numbers.
3. Case insensitive.
4. Cannot be a [reserved keyword](../reserved-keywords/).
5. Rules for escaped table (column) names:
   To support more forms of table (column) names, TDengine introduces a new escape character ``"``. After using the escape character:
   - Case uniformity is no longer applied to the content within escape characters, allowing the preservation of case sensitivity in user-specified table names. For example, \`aBc\` and \`abc\` are considered different table (column) names, while abc and aBc are treated as the same table (column) name.
   - Table (column) names can include characters other than letters, numbers, and underscores, such as \`abc@TD\`, but escaped names still cannot contain `.`; otherwise, the error `The table name cannot contain '.'` will be prompted.
   - Names can start with numbers, such as \`1970\`.
   - Names can be the same as [reserved keywords](../reserved-keywords/), such as \`select\`.

## Password Legal Character Set

`[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`

This excludes ``‘“`\`` (single quotes, double quotes, apostrophes, backslashes, spaces).

## General Restrictions

- Maximum length of database name: 64 bytes
- Maximum length of table name: 192 bytes, excluding database prefix and separator
- Maximum length of each data row: 48KB (64KB starting from version 3.0.5.0) (Note: Each BINARY/NCHAR type column in a data row will also occupy an additional 2 bytes of storage space.)
- Maximum length of column name: 64 bytes
- Maximum allowed columns: 4096, with a minimum of 2 columns, the first column must be a timestamp.
- Maximum length of tag name: 64 bytes
- Maximum allowed tags: 128, with at least 1 tag, the total length of tag values in a table cannot exceed 16KB.
- Maximum length of SQL statements: 1,048,576 characters
- For SELECT statement results, a maximum of 4096 columns are allowed (function calls in the statement may also occupy some column space); if exceeded, fewer return data columns must be explicitly specified to avoid execution errors.
- The number of databases, supertables, and tables is not restricted by the system, only limited by system resources.
- The number of database replicas can only be set to 1 or 3.
- Maximum length of username: 23 bytes
- Maximum length of user password: 31 bytes
- Total number of data rows depends on available resources.
- Maximum number of virtual nodes per single database: 1024
