---
sidebar_label: Names & Limits
title: Naming Conventions and Limitations
slug: /tdengine-reference/sql-manual/names
---

## Naming Rules

1. Legal characters: English letters, numbers, and underscores.
1. Allowed to start with English letters or underscores, not allowed to start with numbers.
1. Case insensitive.
1. Cannot be [reserved keywords](./92-keywords.md).
1. Escaped table (column) name rules:
   To support more forms of table (column) names, TDengine introduces a new escape character "`". After using the escape character:
   - The content within the escape characters is not unified in case, meaning the case specified by the user is retained, for example: \`aBc\` and \`abc\` are different table (column) names, but abc and aBc are the same table (column) name.
   - It is possible to create table (column) names containing characters other than letters, numbers, and underscores, for example: \`abc@TD\`, but the escaped name still cannot contain `.`, otherwise it will prompt `The table name cannot contain '.'`.
   - It is possible to create table (column) names starting with numbers, for example \`1970\`.
   - It is possible to create table (column) names using [reserved keywords](./92-keywords.md), for example \`select\`.

## Legal Character Set for Passwords

`[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`

Removed ``' " ` `` (single and double quotes, apostrophe, backslash, space)

## General Restrictions

- Maximum length of database name is 64 bytes
- Maximum length of table name is 192 bytes, excluding the database name prefix and separator
- Maximum length of each row of data is 48KB (from version 3.0.5.0 it is 64KB) (Note: each BINARY/NCHAR type column within a data row will additionally occupy 2 bytes of storage space)
- Maximum length of column name is 64 bytes
- Maximum of 4096 columns allowed, with a minimum of 2 columns, the first column must be a timestamp.
- Maximum length of tag name is 64 bytes
- Maximum of 128 tags allowed, with at least 1 tag required, total length of tag values in a table not exceeding 16KB
- Default maximum length of SQL statement is 1048576 characters, can be configured via the `maxSQLLength` parameter, with a maximum value of 64MB.
- SELECT statement results can return up to 4096 columns (function calls in the statement may also occupy some column space), exceeding this limit requires explicitly specifying fewer return data columns to avoid execution errors
- Number of databases, supertables, and tables are not limited by the system, only by system resources
- Number of replicas for a database can only be set to 1 or 3
- Maximum length of username is 23 bytes
- Maximum length of user password is 255 bytes
- Total number of data rows depends on available resources
- Maximum number of virtual nodes for a single database is 1024
