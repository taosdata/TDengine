---
title: Operators
slug: /tdengine-reference/sql-manual/operators
---
## Arithmetic Operators

| #   | **Operator** | **Supported Types** | **Description**                   |
| --- | :----------: | ------------------- | --------------------------------- |
| 1   |    +, -      | Numeric types       | Represents positive and negative numbers, unary operators |
| 2   |    +, -      | Numeric types       | Represents addition and subtraction, binary operators |
| 3   |   \*, /      | Numeric types       | Represents multiplication and division, binary operators |
| 4   |     %        | Numeric types       | Represents modulo operation, binary operators   |

## Bitwise Operators

| #   | **Operator** | **Supported Types** | **Description**           |
| --- | :----------: | ------------------- | ------------------------- |
| 1   |     &        | Numeric types       | Bitwise AND, binary operator |
| 2   |     \|       | Numeric types       | Bitwise OR, binary operator |

## JSON Operators

The `->` operator can retrieve values by key from JSON type columns. The left side of `->` is the column identifier, and the right side is the key as a string constant, such as `col->'name'`, which returns the value of the key `'name'`.

## Set Operators

Set operators combine the results of two queries into one result. Queries containing set operators are called compound queries. In compound queries, the number of expressions in the select list of each query must match, and the result type must conform to that of the first query, with subsequent query result types being convertible to the first query's result type, following the same rules as the CAST function.

TDengine supports `UNION ALL` and `UNION` operators. UNION ALL combines the results of the queries and returns them without eliminating duplicates. UNION combines and returns the results of the queries after eliminating duplicates. In the same SQL statement, a maximum of 100 set operators are supported.

## Comparison Operators

| #   |    **Operator**     | **Supported Types**                                                       | **Description**             |
| --- | :-----------------: | ------------------------------------------------------------------------ | -------------------------- |
| 1   |         =           | All types except BLOB, MEDIUMBLOB, and JSON                              | Equal                      |
| 2   |      \<>, !=        | All types except BLOB, MEDIUMBLOB, and JSON, and not for table's timestamp primary key column | Not equal                  |
| 3   |      >, \<         | All types except BLOB, MEDIUMBLOB, and JSON                              | Greater than, less than    |
| 4   |     >=, \<=        | All types except BLOB, MEDIUMBLOB, and JSON                              | Greater than or equal, less than or equal |
| 5   |   IS [NOT] NULL     | All types                                                                 | Whether it is a null value |
| 6   | [NOT] BETWEEN AND   | All types except BOOL, BLOB, MEDIUMBLOB, JSON, and GEOMETRY              | Closed interval comparison |
| 7   |        IN           | All types except BLOB, MEDIUMBLOB, and JSON, and not for table's timestamp primary key column | Equal to any value in the list |
| 8   |      NOT IN         | All types except BLOB, MEDIUMBLOB, and JSON, and not for table's timestamp primary key column | Not equal to any value in the list |
| 9   |       LIKE          | BINARY, NCHAR, and VARCHAR                                               | Matches the specified pattern string with wildcard |
| 10  |      NOT LIKE       | BINARY, NCHAR, and VARCHAR                                               | Does not match the specified pattern string with wildcard |
| 11  |   MATCH, NMATCH     | BINARY, NCHAR, and VARCHAR                                               | Regular expression match   |
| 12  |   REGEXP, NOT REGEXP     | BINARY, NCHAR, and VARCHAR                                               | Regular expression match   |
| 13  |     CONTAINS        | JSON                                                                     | Whether a key exists in JSON |

LIKE conditions use wildcard strings for matching checks, with the following rules:

- '%' (percent sign) matches 0 to any number of characters; '_' (underscore) matches any single ASCII character.
- If you want to match an underscore character that is originally in the string, you can write it as \_ in the wildcard string, i.e., add a backslash to escape it.
- The wildcard string cannot exceed 100 bytes in length. It is not recommended to use too long wildcard strings, as it may severely affect the performance of the LIKE operation.

MATCH/REGEXP and NMATCH/NOT REGEXP conditions use regular expressions for matching, with the following rules:

- Supports regular expressions that comply with the POSIX standard, see Regular Expressions for specific standards.
- When MATCH matches a regular expression, it returns TRUE. When NMATCH does not match a regular expression, it returns TRUE.
- Only supports filtering on subtable names (i.e., tbname) and string type tag values, does not support filtering on ordinary columns.
- The length of the regular expression string cannot exceed 128 bytes. You can set and adjust the maximum allowed regular expression string length through the parameter maxRegexStringLen, which is a client configuration parameter and requires restarting the client to take effect.

## Logical Operators

| #   | **Operator** | **Supported Types** | **Description**                                                                 |
| --- | :----------: | ------------------- | ------------------------------------------------------------------------------- |
| 1   |     AND      | BOOL                | Logical AND, returns TRUE if both conditions are TRUE. Returns FALSE if any is FALSE |
| 2   |      OR      | BOOL                | Logical OR, returns TRUE if any condition is TRUE. Returns FALSE if both are FALSE |

TDengine optimizes logical condition evaluation with short-circuiting, i.e., for AND, if the first condition is FALSE, it does not evaluate the second condition and directly returns FALSE; for OR, if the first condition is TRUE, it does not evaluate the second condition and directly returns TRUE.
