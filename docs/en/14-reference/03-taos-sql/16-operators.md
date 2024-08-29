---
title: Operators
sidebar_label: Operators
description: This document describes the SQL operators available in TDengine.
---

## Arithmetic Operators

| #   | **Operator** | **Supported Data Types** | **Description**                   |
| --- | :--------: | -------------- | -------------------------- |
| 1   |    +, -    | Numeric       | Expresses sign. Unary operators. |
| 2   |    +, -    | Numeric       | Expresses addition and subtraction. Binary operators. |
| 3   |   \*, /    | Numeric       | Expresses multiplication and division. Binary operators. |
| 4   |     %      | Numeric       | Expresses modulo. Binary operator.   |

## Bitwise Operators

| #   | **Operator** | **Supported Data Types** | **Description**                   |
| --- | :--------: | -------------- | ------------------ |
| 1   |     &      | Numeric       | Bitwise AND. Binary operator. |
| 2   |     \|     | Numeric       | Bitwise OR. Binary operator. |

## JSON Operators

The `->` operator returns the value for a key in JSON column. Specify the column indicator on the left of the operator and the key name on the right of the operator. For example, `col->name` returns the value of the name key.

## Set Operators

Set operators combine the results of two queries. Queries that include set operators are known as compound queries. The expressions corresponding to each query in the select list in a compound query must match in number. The results returned take the data type of the first query, and the data type returned by subsequent queries must be convertible into the data type of the first query. The conditions of the `CAST` function apply to this conversion.

TDengine supports the `UNION` and `UNION ALL` operations. UNION ALL collects all query results and returns them as a composite result without deduplication. UNION collects all query results and returns them as a deduplicated composite result. In a single SQL statement, at most 100 set operators can be supported.

## Comparison Operators

| #   | **Operator** | **Supported Data Types** | **Description**                   |
| --- | :---------------: | -------------------------------------------------------------------- | -------------------- |
| 1   |         =         | All types except BLOB, MEDIUMBLOB, and JSON                             | Equal to |
| 2   |      &lt;&gt;, !=      | All types except BLOB, MEDIUMBLOB, and JSON; the primary key (timestamp) is also not supported | Not equal to               |
| 3   |      &gt;, &lt;       | All types except BLOB, MEDIUMBLOB, and JSON                             | Greater than and less than           |
| 4   |     &gt;=, &lt;=      | All types except BLOB, MEDIUMBLOB, and JSON                             | Greater than or equal to and less than or equal to   |
| 5   |   IS [NOT] NULL   | All types                                                             | Indicates whether the value is null           |
| 6   | [NOT] BETWEEN AND | All types except BLOB, MEDIUMBLOB, JSON and GEOMETRY                  | Closed interval comparison           |
| 7   |        IN         | All types except BLOB, MEDIUMBLOB, and JSON; the primary key (timestamp) is also not supported | Equal to any value in the list |
| 8   |       LIKE        | BINARY, NCHAR, and VARCHAR                                             | Wildcard match           |
| 9   |   MATCH, NMATCH   | BINARY, NCHAR, and VARCHAR                                             | Regular expression match       |
| 10  |     CONTAINS      | JSON                                                                 | Indicates whether the key exists  |

LIKE is used together with wildcards to match strings. Its usage is described as follows:

- '%' matches 0 or any number of characters, '\_' matches any single ASCII character.
- `\_` is used to match the \_ in the string.
- The maximum length of wildcard string is 100 bytes. A very long wildcard string may slowdown the execution performance of `LIKE` operator.

MATCH and NMATCH are used together with regular expressions to match strings. Their usage is described as follows:

- Use POSIX regular expression syntax. For more information, see Regular Expressions.
- The `MATCH` operator returns true when the regular expression is matched. The `NMATCH` operator returns true when the regular expression is not matched.
- Regular expression can be used against only table names, i.e. `tbname`, and tags/columns of binary/nchar types.
- The maximum length of regular expression string is 128 bytes. Configuration parameter `maxRegexStringLen` can be used to set the maximum allowed regular expression. It's a configuration parameter on the client side, and will take effect after restarting the client.

## Logical Operators

| #   | **Operator** | **Supported Data Types** | **Description**                   |
| --- | :--------: | -------------- | --------------------------------------------------------------------------- |
| 1   |    AND     | BOOL           | Logical AND; if both conditions are true, TRUE is returned; If either condition is false, FALSE is returned.
| 2   |    OR     | BOOL           | Logical OR; if either condition is true, TRUE is returned; If both conditions are false, FALSE is returned.

TDengine performs short-path optimization when calculating logical conditions. If the first condition for AND is false, FALSE is returned without calculating the second condition. If the first condition for OR is true, TRUE is returned without calculating the second condition
