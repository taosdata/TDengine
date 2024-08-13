---
title: Name and Size Limits
sidebar_label: Name and Size Limits
description: This document describes the name and size limits in TDengine.
---

## Naming Rules

1. Names can include letters, digits, and underscores (_).
2. Names can begin with letters or underscores (_) but not with digits.
3. Names are not case-sensitive.
4. Rules for names with escape characters are as follows:
   You can escape a name by enclosing it in backticks (`). In this way, you can reuse keyword names for table names. However, the first three naming rules no longer apply.
   Table and column names that are enclosed in escape characters are still subject to length limits. When the length of such a name is calculated, the escape characters are not included. Names specified using escape character are case-sensitive.

   For example, \`aBc\` and \`abc\` are different table or column names, but "abc" and "aBc" are same names because internally they are all "abc".
   Only ASCII visible characters can be used with escape character.

## Password Rules

`[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`

The following characters cannot occur in a password: single quotation marks ('), double quotation marks ("), backticks (`), backslashes (\\), and spaces.

## General Limits

- Maximum length of database name is 64 bytes
- Maximum length of table name is 192 bytes, excluding the database name prefix and the separator.
- Maximum length of each data row is 48K(64K since version 3.0.5.0) bytes. Note that the upper limit includes the extra 2 bytes consumed by each column of BINARY/NCHAR type.
- The maximum length of a column name is 64 bytes.
- Maximum number of columns is 4096. There must be at least 2 columns, and the first column must be timestamp.
- The maximum length of a tag name is 64 bytes
- Maximum number of tags is 128. There must be at least 1 tag. The total length of tag values cannot exceed 16 KB.
- Maximum length of single SQL statement is 1 MB (1048576 bytes). 
- At most 4096 columns can be returned by `SELECT`. Functions in the query statement constitute columns. An error is returned if the limit is exceeded.
- Maximum numbers of databases, STables, tables are dependent only on the system resources.
- The number of replicas can only be 1 or 3.
- The maximum length of a username is 23 bytes.
- The maximum length of a password is 31 bytes.
- The maximum number of rows depends on system resources.
- The maximum number of vnodes in a database is 1024.

## Restrictions of Table/Column Names

### Name Restrictions of Table/Column

The name of a table or column can only be composed of ASCII characters, digits and underscore and it cannot start with a digit. The maximum length is 192 bytes. Names are case insensitive. The name mentioned in this rule doesn't include the database name prefix and the separator.

### Name Restrictions After Escaping

To support more flexible table or column names, new escape character "\`" is introduced in TDengine to avoid the conflict between table name and keywords and break the above restrictions for table names. The escape character is not counted in the length of table name.
With escaping, the string inside escape characters are case sensitive, i.e. will not be converted to lower case internally. The table names specified using escape character are case sensitive.

For example:
\`aBc\` and \`abc\` are different table or column names, but "abc" and "aBc" are same names because internally they are all "abc".

:::note
The characters inside escape characters must be printable characters.

:::
