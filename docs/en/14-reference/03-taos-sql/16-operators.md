---
title: Operators
description: All operators supported by TDengine
slug: /tdengine-reference/sql-manual/operators
---

## Arithmetic Operators

| #   | **Operator** | **Supported Types** | **Description**                   |
| --- | :----------: | ------------------- | --------------------------------- |
| 1   |    +, -      | Numeric Types       | Represents positive and negative numbers, unary operators |
| 2   |    +, -      | Numeric Types       | Represents addition and subtraction, binary operators |
| 3   |   \*, /      | Numeric Types       | Represents multiplication and division, binary operators |
| 4   |     %        | Numeric Types       | Represents the modulus operation, binary operator |

## Bitwise Operators

| #   | **Operator** | **Supported Types** | **Description**            |
| --- | :----------: | ------------------- | -------------------------- |
| 1   |     &        | Numeric Types       | Bitwise AND, binary operator |
| 2   |     \|       | Numeric Types       | Bitwise OR, binary operator  |

## JSON Operators

The `->` operator can be used to access values by key in columns of JSON type. The left side of `->` is the column identifier, and the right side is the string constant of the key, such as `col->'name'`, which returns the value of the key `'name'`.

## Set Operators

Set operators combine the results of two queries into a single result. Queries that include set operators are called compound queries. In a compound query, the corresponding expressions in the select list of each query must match in number, and the result type is determined by the first query; subsequent queries must have result types that can be converted to the first query's result type, with conversion rules similar to the `CAST` function.

TDengine supports the `UNION ALL` and `UNION` operators. `UNION ALL` merges the result sets returned by the queries without removing duplicates. `UNION` merges the result sets and removes duplicates. A single SQL statement can support up to 100 set operators.

## Comparison Operators

| #   |    **Operator**     | **Supported Types**                                                       | **Description**         |
| --- | :------------------: | -------------------------------------------------------------------------- | ------------------------ |
| 1   |         =            | All types except BLOB, MEDIUMBLOB, and JSON                               | Equal                    |
| 2   |      \<>, !=         | All types except BLOB, MEDIUMBLOB, and JSON, and cannot be the timestamp primary key column of a table | Not equal                |
| 3   |      >, \<           | All types except BLOB, MEDIUMBLOB, and JSON                               | Greater than, less than  |
| 4   |     >=, \<=          | All types except BLOB, MEDIUMBLOB, and JSON                               | Greater than or equal to, less than or equal to |
| 5   |   IS [NOT] NULL      | All types                                                                  | Is null                  |
| 6   | [NOT] BETWEEN AND    | All types except BOOL, BLOB, MEDIUMBLOB, JSON, and GEOMETRY              | Closed interval comparison |
| 7   |        IN            | All types except BLOB, MEDIUMBLOB, and JSON, and cannot be the timestamp primary key column of a table | Equal to any value in the list |
| 8   |      NOT IN          | All types except BLOB, MEDIUMBLOB, and JSON, and cannot be the timestamp primary key column of a table | Not equal to any value in the list |
| 9   |       LIKE           | BINARY, NCHAR, and VARCHAR                                                | Matches the specified pattern string with wildcards |
| 10  |      NOT LIKE        | BINARY, NCHAR, and VARCHAR                                                | Does not match the specified pattern string |
| 11  |   MATCH, NMATCH      | BINARY, NCHAR, and VARCHAR                                                | Regular expression match   |
| 12  |     CONTAINS         | JSON                                                                      | Whether a key exists in JSON |

The `LIKE` condition uses wildcard strings for matching checks, with the following rules:

- '%' (percent) matches 0 to any number of characters; '_' (underscore) matches any single ASCII character.
- To match the '_' (underscore) character that is naturally present in the string, you can write it as '\_' in the wildcard string, escaping it with a backslash.
- The wildcard string cannot exceed 100 bytes in length. It is not recommended to use excessively long wildcard strings, as they may significantly affect the performance of the `LIKE` operation.

The `MATCH` and `NMATCH` conditions use regular expressions for matching, with the following rules:

- Supports regular expressions that conform to POSIX standards. Specifics can be found in Regular Expressions.
- `MATCH` returns TRUE when it matches the regular expression; `NMATCH` returns TRUE when it does not match the regular expression.
- Regular expressions can only filter based on subtable names (i.e., `tbname`) and string-type tag values, and do not support filtering on ordinary columns.
- The length of the regular matching string cannot exceed 128 bytes. The maximum allowed length for the regular matching string can be set and adjusted using the `maxRegexStringLen` parameter, which is a client configuration parameter that requires restarting the client to take effect.

## Logical Operators

| #   | **Operator** | **Supported Types** | **Description**                                                                  |
| --- | :----------: | ------------------- | -------------------------------------------------------------------------------- |
| 1   |    AND       | BOOL                | Logical AND; returns TRUE if both conditions are TRUE; returns FALSE if either is FALSE |
| 2   |     OR       | BOOL                | Logical OR; returns TRUE if either condition is TRUE; returns FALSE if both are FALSE |

TDengine performs short-circuit optimization when evaluating logical conditions: for `AND`, if the first condition is FALSE, it does not evaluate the second condition and returns FALSE immediately; for `OR`, if the first condition is TRUE, it does not evaluate the second condition and returns TRUE immediately.
