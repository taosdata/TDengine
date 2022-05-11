---
sidebar_label: Limits
title: Limits and Restrictions
---

## Naming Rules

1. Only English characters, digits and underscore are allowed
2. Can't be started with digits
3. Case Insensitive without escape character "\`"
4. Identifier with escape character "\`"
   To support more flexible table or column names, a new escape character "\`" is introduced. For more details please refer to [escape](/taos-sql/escape).

## Password Rule

The legal character set is `[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`.

## General Limits

- Maximum length of database name is 32 bytes
- Maximum length of table name is 192 bytes, excluding the database name prefix and the separator
- Maximum length of each data row is 48K bytes from version 2.1.7.0 , before which the limit is 16K bytes. Please be noted that the upper limit includes the extra 2 bytes consumed by each column of BINARY/NCHAR type.
- Maximum of column name is 64.
- Maximum number of columns is 4096. There must be at least 2 columns, and the first column must be timestamp.
- Maximum length of tag name is 64.
- Maximum number of tags is 128. There must be at least 1 tag. The total length of tag values should not exceed 16K bytes.
- Maximum length of singe SQL statement is 1048576, i.e. 1 MB bytes. It can be configured in the parameter `maxSQLLength` in the client side, the applicable range is [65480, 1048576].
- At most 4096 columns (or 1024 prior to 2.1.7.0) can be returned by `SELECT`, functions in the query statement may constitute columns. Error will be returned if the limit is exceeded.
- Maximum numbers of databases, stables, tables are only depending on the system resources.
- Maximum of database name is 32 bytes, can't include "." and special characters.
- Maximum replica number of database is 3
- Maximum length of user name is 23 bytes
- Maximum length of password is 15 bytes
- Maximum number of rows depends on the storage space only.
- Maximum number of tables depends on the number of nodes only.
- Maximum number of databases depends on the number of nodes only.
- Maximum number of vnodes for single database is 64.

## Restrictions of `GROUP BY`

`GROUP BY` can be performed on tags and `TBNAME`. It can be performed on data columns too, with one restriction that only one column and the number of unique values on that column is lower than 100,000. Please be noted that `GROUP BY` can't be performed on float or double type.

## Restrictions of `IS NOT NULL`

`IS NOT NULL` can be used on any data type of columns. The non-empty string evaluation expression, i.e. `<\>""` can only be used on non-numeric data types.

## Restrictions of `ORDER BY`

- Only one `order by` is allowed for normal table and sub table.
- At most two `order by` are allowed for stable, and the second one must be `ts`.
- `order by tag` must be used with `group by tag` on same tag, this rule is also applicable to `tbname`.
- `order by column` must be used with `group by column` or `top/bottom` on same column. This rule is applicable to table and stable.
- `order by ts` is applicable to table and stable.
- If `order by ts` is used with `group by`, the result set is sorted using `ts` in each group.

## Restrictions of Table/Column Names

### Name Restrictions of Table/Column

The name of a table or column can only be composed of ASCII characters, digits and underscore, while digit can't be used as the beginning. The maximum length is 192 bytes. Names are case insensitive. The name mentioned in this rule doesn't include the database name prefix and the separator.

### Name Restrictions After Escaping

To support more flexible table or column names, new escape character "`" is introduced in TDengine to avoid the conflict between table name and keywords and break the above restrictions for table name. The escape character is not counted in the length of table name.

With escaping, the string inside escape characters are case sensitive, i.e. will not be converted to lower case internally.

For example:
\`aBc\` and \`abc\` are different table or column names, but "abc" and "aBc" are same names because internally they are all "abc".

:::note
The characters inside escape characters must be printable characters.

:::

### Applicable Versions

Escape character "\`" is available from version 2.3.0.1.
