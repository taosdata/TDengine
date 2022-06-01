---
sidebar_label: Operators
title: Operators
---

## Arithmetic Operators

| #   | **Operator** | **Data Types** | **Description**                                           |
| --- | :----------: | -------------- | --------------------------------------------------------- |
| 1   |     +, -     | Numeric Types  | Representing positive or negative numbers, unary operator |
| 2   |     +, -     | Numeric Types  | Addition and substraction, binary operator                |
| 3   |    \*, /     | Numeric Types  | Multiplication and division, binary oeprator              |
| 4   |      %       | Numeric Types  | Taking the remainder, binary operator                     |

## Bitwise Operators

| #   | **Operator** | **Data Types** | **Description**               |
| --- | :----------: | -------------- | ----------------------------- |
| 1   |      &       | Numeric Types  | Bitewise AND, binary operator |
| 2   |      \|      | Numeric Types  | Bitewise OR, binary operator  |

## JSON Operator

`->` operator can be used to get the value of a key in a column of JSON type, the left oeprand is the column name, the right operand is a string constant. For example, `col->'name'` returns the value of key `'name'`.

## Set Operator

Set operators are used to combine the results of two queries into single result. A query including set operators is called a combined query. The number of rows in each result in a combined query must be same, and the type is determined by the first query's result, the type of the following queriess result must be able to be converted to the type of the first query's result, the conversion rule is same as `CAST` function.

TDengine provides 2 set operators: `UNION ALL` and `UNION`. `UNION ALL` combines the results without removing duplicate data. `UNION` combines the results and remove duplicate data rows. In single SQL statement, at most 100 set operators can be used.

## Comparsion Operator

| #   |   **Operator**    | **Data Types**                                                      | **Description**                                 |
| --- | :---------------: | ------------------------------------------------------------------- | ----------------------------------------------- |
| 1   |         =         | Except for BLOB, MEDIUMBLOB and JSON                                | Equal                                           |
| 2   |      <\>, !=      | Except for BLOB, MEDIUMBLOB, JSON and primary key of timestamp type | Not equal                                       |
| 3   |      \>, <       | Except for BLOB, MEDIUMBLOB and JSON                                | Greater than, less than                         |
| 4   |     \>=, <=      | Except for BLOB, MEDIUMBLOB and JSON                                | Greater than or equal to, less than or equal to |
| 5   |   IS [NOT] NULL   | Any types                                                           | Is NULL or NOT                                  |
| 6   | [NOT] BETWEEN AND | Except for BLOB, MEDIUMBLOB and JSON                                | In a value range or not                         |
| 7   |        IN         | Except for BLOB, MEDIUMBLOB, JSON and primary key of timestamp type | In a list of values or not                      |
| 8   |       LIKE        | BINARY, NCHAR and VARCHAR                                           | Wildcard matching                               |
| 9   |   MATCH, NMATCH   | BINARY, NCHAR and VARCHAR                                           | Regular expression matching                     |
| 10  |     CONTAINS      | JSON                                                                | If A key exists in JSON                         |

`LIKE` operator uses wildcard to match a string, the rules are：

- '%' matches 0 to any number of characters; '\_' matches any single ASCII character.
- \_ can be used to match a `_` in the string, i.e. using escape character backslash `\`
- Wildcard string is 100 bytes at most. Longer a wildcard string is, worse the performance of LIKE operator is.

`MATCH` and `NMATCH` operators use regular expressions to match a string, the rules are:

- Regular expressions of POSIX standard are supported.
- Only `tbname`, i.e. table name of sub tables, and tag columns of string types can be matched with regular expression, data columns are not supported.
- Regular expression string is 128 bytes at most, and can be adjusted by setting parameter `maxRegexStringLen`, which is a client side configuration and needs to restart the client to take effect.

## Logical Operators

| #   | **Operator** | **Data Types** | **Description**                                                                          |
| --- | :----------: | -------------- | ---------------------------------------------------------------------------------------- |
| 1   |     AND      | BOOL           | Logical AND, return TRUE if both conditions are TRUE; return FALSE if any one is FALSE. |
| 2   |      OR      | BOOL           | Logical OR, return TRUE if any condition is TRUE; return FALSE if both are FALSE        |

TDengine uses shortcircut optimization when performing logical operations. For AND operator, if the first condition is evaluated to FALSE, then the second one is not evaluated. For OR operator, if the first condition is evaluated to TRUE, then the second one is not evaluated.
